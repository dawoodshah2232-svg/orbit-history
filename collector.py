#!/usr/bin/env python3
"""OrbitTrader spot M1 history relay.

Collects real Swissquote bid quotes for XAUUSD/XAGUSD and major FX pairs,
aggregates them into M1 candles, keeps a rolling six-hour window, and
publishes JSON for the OrbitTrader client.

v3 reliability changes:
- publish every successful run so the client does not wait several minutes;
- reduce request concurrency to avoid Swissquote connection resets/refusals;
- retry transient failures with short exponential backoff;
- reject quotes older than 45 seconds;
- keep source timestamps and bid-basis candles; never substitute PAXG or
  synthetic prices for XAUUSD.
"""
import json
import os
import time
import urllib.error
import urllib.request
import fcntl
from concurrent.futures import ThreadPoolExecutor

BASE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(BASE, "state.json")
LOG = os.path.join(BASE, "collector.log")
LOCK = os.path.join(BASE, "collector.lock")

SYMBOLS = {
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
}

HISTORY_MAX = 360
STALE_MS = 45_000
PUSH_EVERY = 1
SUBPOLLS = 8
SUBPOLL_GAP = 6
MAX_WORKERS = 3
FETCH_TIMEOUT = 8
FETCH_RETRIES = 3


def log(msg):
    line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + msg
    try:
        with open(LOG, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def atomic_write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=True)
    os.replace(tmp, path)


def load_state():
    try:
        with open(STATE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"symbols": {}, "runs": 0}


def _parse_quote(arr):
    if not arr:
        return None
    row = arr[0] or {}
    profs = row.get("spreadProfilePrices") or []
    prof = next((p for p in profs if p.get("spreadProfile") == "standard"),
                profs[0] if profs else None)
    if not prof:
        return None
    bid, ask, ts = prof.get("bid"), prof.get("ask"), row.get("ts")
    if bid is None or ask is None or ts is None:
        return None
    bid = float(bid)
    ask = float(ask)
    ts = int(ts)
    if not (bid > 0 and ask > 0 and ask >= bid and ts > 0):
        return None
    return bid, ts


def fetch_quote(instrument):
    url = ("https://forex-data-feed.swissquote.com/public-quotes/"
           "bboquotes/instrument/" + instrument)
    last_err = None
    for attempt in range(FETCH_RETRIES):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "OrbitTrader-history-relay/3.0",
                    "Accept": "application/json",
                    "Connection": "close",
                },
            )
            with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
                q = _parse_quote(json.load(r))
            if q:
                return q
            last_err = RuntimeError("empty/invalid quote")
        except (urllib.error.URLError, urllib.error.HTTPError,
                TimeoutError, ConnectionError, OSError, ValueError) as e:
            last_err = e
        if attempt < FETCH_RETRIES - 1:
            time.sleep(0.35 * (2 ** attempt))
    if last_err:
        raise last_err
    return None


def _one(item):
    sym, instr = item
    try:
        return sym, fetch_quote(instr), None
    except Exception as e:
        return sym, None, str(e)


def _append_tick(syms, sym, bidpx, ts):
    minute = (ts // 60000) * 60000
    s = syms.setdefault(sym, {"cur": None, "hist": []})
    cur = s.get("cur")

    # Ignore an out-of-order quote instead of corrupting an already newer bar.
    if cur and minute < cur.get("t", 0):
        return

    if cur and cur.get("t") == minute:
        cur["h"] = max(float(cur["h"]), bidpx)
        cur["l"] = min(float(cur["l"]), bidpx)
        cur["c"] = bidpx
        cur["n"] = int(cur.get("n", 1)) + 1
        return

    if cur:
        s["hist"].append(cur)
        s["hist"] = s["hist"][-HISTORY_MAX:]

    s["cur"] = {
        "t": minute,
        "o": bidpx,
        "h": bidpx,
        "l": bidpx,
        "c": bidpx,
        "n": 1,
    }


def main():
    try:
        lf = open(LOCK, "w")
        fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        return

    st = load_state()
    st["runs"] = st.get("runs", 0) + 1
    if "basis_since" not in st:
        st["basis_since"] = int(time.time() * 1000)
    syms = st.setdefault("symbols", {})

    items = list(SYMBOLS.items())
    for p in range(SUBPOLLS):
        now_ms = int(time.time() * 1000)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            results = list(ex.map(_one, items))

        for sym, q, err in results:
            if err:
                log(f"{sym} fetch failed after retries: {err}")
                continue
            if q is None:
                continue
            bidpx, ts = q
            age = now_ms - ts
            if age < -5_000:
                log(f"{sym} future quote ({-age}ms), skipped")
                continue
            if age > STALE_MS:
                log(f"{sym} stale quote ({age // 1000}s old), skipped")
                continue
            _append_tick(syms, sym, bidpx, ts)

        if p < SUBPOLLS - 1:
            time.sleep(SUBPOLL_GAP)

    for sym in SYMBOLS:
        s = syms.get(sym)
        if not s:
            continue
        out = s.get("hist", []) + ([s["cur"]] if s.get("cur") else [])
        out = sorted(out, key=lambda x: int(x.get("t", 0)))[-HISTORY_MAX:]
        atomic_write(
            os.path.join(BASE, f"{sym}_m1.json"),
            {
                "basis": "bid",
                "source": "swissquote",
                "basis_since": st["basis_since"],
                "generated_at": int(time.time() * 1000),
                "candles": out,
            },
        )

    atomic_write(STATE, st)

    if st["runs"] % PUSH_EVERY == 0:
        rc = os.system(
            f"cd {BASE} && git add -A && "
            f"git -c user.email=relay@orbithub -c user.name=orbithistory-relay "
            f"commit -qm 'relay v3 stable ticks' 2>/dev/null; "
            f"git push -q origin gh-pages 2>&1 | tail -1"
        )
        if rc != 0:
            log(f"push exited {rc}")


if __name__ == "__main__":
    main()
