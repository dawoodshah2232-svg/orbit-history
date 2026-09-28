#!/usr/bin/env python3
"""OrbitTrader spot M1 history relay.
Polls Swissquote public quotes for hot FX/metal symbols, aggregates real
ticks into M1 candles, keeps a rolling 6h window, and publishes per-symbol
JSON to the gh-pages branch (served via GitHub Pages). No simulated data:
if a quote is stale/missing the tick is skipped, never invented.
"""
import json, os, time, urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(BASE, "state.json")
LOG = os.path.join(BASE, "collector.log")

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

def main():
    st = load_state()
    st["runs"] = st.get("runs", 0) + 1
    now_ms = int(time.time() * 1000)
    syms = st.setdefault("symbols", {})

    for sym, instr in SYMBOLS.items():
        try:
            q = fetch_quote(instr)
        except Exception as e:  # network hiccup -> skip, never invent
            log(f"{sym} fetch failed: {e}")
            q = None
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
            cur["h"] = max(cur["h"], mid)
            cur["l"] = min(cur["l"], mid)
            cur["c"] = mid
        else:
            if cur:
                s["hist"].append(cur)
                s["hist"] = s["hist"][-HISTORY_MAX:]
            s["cur"] = {"t": minute, "o": mid, "h": mid, "l": mid, "c": mid}
        # publish file: history + forming candle
        out = s["hist"] + ([s["cur"]] if s["cur"] else [])
        atomic_write(os.path.join(BASE, f"{sym}_m1.json"), out)
        time.sleep(1.5)  # gentle pacing between symbols

    atomic_write(STATE, st)

    if st["runs"] % PUSH_EVERY == 0:
        rc = os.system(
            f"cd {BASE} && git add -A -q && "
            f"git -c user.email=relay@orbithub -c user.name=orbithistory-relay "
            f"commit -qm 'ticks' 2>/dev/null; git push -q origin gh-pages 2>&1 | tail -1"
        )
        if rc != 0:
            log(f"push exited {rc}")

if __name__ == "__main__":
    main()
