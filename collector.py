#!/usr/bin/env python3
"""OrbitTrader spot M1 history relay.
Polls Swissquote public quotes for hot FX/metal symbols, aggregates real
ticks into M1 candles, keeps a rolling 6h window, and publishes per-symbol
JSON to the gh-pages branch (served via GitHub Pages). No simulated data:
if a quote is stale/missing the tick is skipped, never invented.

v2: several sub-polls per run (symbols fetched concurrently) so each M1
candle is built from multiple real ticks and carries a genuine high/low.
A single 1-tick-per-minute sample produced flat o==h==l==c candles with no
wicks; that was a sampling artifact, not the market.
"""
import json, os, time, urllib.request, fcntl
from concurrent.futures import ThreadPoolExecutor

BASE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(BASE, "state.json")
LOG = os.path.join(BASE, "collector.log")
LOCK = os.path.join(BASE, "collector.lock")

# app symbol -> swissquote instrument
SYMBOLS = {
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
}
HISTORY_MAX = 360          # 6h of M1
STALE_MS = 120_000         # ignore quotes older than this
PUSH_EVERY = 3             # push to origin every N runs
SUBPOLLS = 6               # quote passes per run -> ticks per M1 candle
SUBPOLL_GAP = 7            # seconds between passes (run lasts ~45s)

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

def fetch_quote(instrument):
    url = ("https://forex-data-feed.swissquote.com/public-quotes/"
           "bboquotes/instrument/" + instrument)
    req = urllib.request.Request(url, headers={"User-Agent": "OrbitTrader-history-relay/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        arr = json.load(r)
    if not arr:
        return None
    # prefer the "standard" spread profile, else first available
    profs = arr[0].get("spreadProfilePrices") or []
    prof = next((p for p in profs if p.get("spreadProfile") == "standard"), profs[0] if profs else None)
    if not prof:
        return None
    bid, ask = prof.get("bid"), prof.get("ask")
    ts = arr[0].get("ts")
    if bid is None or ask is None or ts is None:
        return None
    return (bid + ask) / 2.0, int(ts)

def _one(item):
    sym, instr = item
    try:
        return sym, fetch_quote(instr), None
    except Exception as e:  # network hiccup -> skip, never invent
        return sym, None, str(e)

def main():
    # single-instance guard: skip quietly if the previous run is still going
    try:
        lf = open(LOCK, "w")
        fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        return
    st = load_state()
    st["runs"] = st.get("runs", 0) + 1
    syms = st.setdefault("symbols", {})

    for p in range(SUBPOLLS):
        now_ms = int(time.time() * 1000)
        with ThreadPoolExecutor(max_workers=len(SYMBOLS)) as ex:
            results = list(ex.map(_one, SYMBOLS.items()))
        for sym, q, err in results:
            if err:
                log(f"{sym} fetch failed: {err}")
                continue
            if q is None:
                continue
            mid, ts = q
            if now_ms - ts > STALE_MS:
                log(f"{sym} stale quote ({(now_ms - ts)//1000}s old), skipped")
                continue
            minute = (ts // 60000) * 60000
            s = syms.setdefault(sym, {"cur": None, "hist": []})
            cur = s["cur"]
            if cur and cur["t"] == minute:
                if mid > cur["h"]:
                    cur["h"] = mid
                if mid < cur["l"]:
                    cur["l"] = mid
                cur["c"] = mid
                cur["n"] = cur.get("n", 1) + 1
            else:
                if cur:
                    s["hist"].append(cur)
                    s["hist"] = s["hist"][-HISTORY_MAX:]
                s["cur"] = {"t": minute, "o": mid, "h": mid, "l": mid, "c": mid, "n": 1}
        if p < SUBPOLLS - 1:
            time.sleep(SUBPOLL_GAP)

    # publish files: history + forming candle
    for sym in SYMBOLS:
        s = syms.get(sym)
        if not s:
            continue
        out = s["hist"] + ([s["cur"]] if s["cur"] else [])
        atomic_write(os.path.join(BASE, f"{sym}_m1.json"), out)
    atomic_write(STATE, st)

    if st["runs"] % PUSH_EVERY == 0:
        rc = os.system(
            f"cd {BASE} && git add -A && "
            f"git -c user.email=relay@orbithub -c user.name=orbithistory-relay "
            f"commit -qm 'ticks' 2>/dev/null; git push -q origin gh-pages 2>&1 | tail -1"
        )
        if rc != 0:
            log(f"push exited {rc}")

if __name__ == "__main__":
    main()
