#!/usr/bin/env python3
"""
Semetrics Skew — Phase-1-Pipeline (Deribit, BTC/ETH)
=====================================================
Täglicher Snapshot der Options-Surface:
  Deribit Public API -> Black-76-IV -> Delta-Smile -> 25Δ/10Δ RR, BF, ATM
  -> Interpolation auf konstante Laufzeiten (7/30/60/90/180 Tage)

Output (./data):
  skew_latest_{CCY}.json        aktueller Snapshot
  skew_history_{CCY}.csv        append-only Tageshistorie (eine Zeile pro Tenor)

Identische Berechnungslogik wie das Frontend (Semetrics_Skew_v1_0_Deribit.html)
und wie der spätere CME/Databento-Fetcher (Phase 2) — nur die Quelle wechselt.

Nutzung:  python3 fetch_skew.py [--currencies BTC ETH] [--out data]
Abhängigkeit: requests  (pip install requests)
"""

import argparse
import csv
import json
import math
import os
import sys
import time
from datetime import datetime, timezone

import requests

API = "https://www.deribit.com/api/v2/public/"
TENORS_D = [7, 30, 60, 90, 180]
PILLARS = [0.10, 0.25, 0.50, 0.75, 0.90]   # Call-Delta
MIN_T_D = 1.5
YEAR_S = 365 * 86400
MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
     "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1)}


# ── Black-76 ────────────────────────────────────────────────────────────────
def n_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def black76(F, K, T, vol, is_call):
    if T <= 0 or vol <= 0:
        return max(0.0, (F - K) if is_call else (K - F))
    sT = vol * math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sT * sT) / sT
    d2 = d1 - sT
    if is_call:
        return F * n_cdf(d1) - K * n_cdf(d2)
    return K * n_cdf(-d2) - F * n_cdf(-d1)


def implied_vol(price_usd, F, K, T, is_call):
    intrinsic = max(0.0, (F - K) if is_call else (K - F))
    if not price_usd > 0 or price_usd <= intrinsic + 1e-9 or price_usd >= F:
        return None
    lo, hi = 0.01, 5.0
    for _ in range(64):
        m = (lo + hi) / 2
        p = black76(F, K, T, m, is_call)
        if abs(p - price_usd) < 1e-7 * F:
            return m
        if p > price_usd:
            hi = m
        else:
            lo = m
    v = (lo + hi) / 2
    return v if 0.011 < v < 4.99 else None


def call_delta(F, K, T, vol):
    return n_cdf((math.log(F / K) + 0.5 * vol * vol * T) / (vol * math.sqrt(T)))


# ── Smile-Interpolation (linear im Call-Delta-Raum, keine Extrapolation) ────
def interp_smile(points, d_target):
    if not points or d_target < points[0][0] or d_target > points[-1][0]:
        return None
    for i in range(1, len(points)):
        (da, va), (db, vb) = points[i - 1], points[i]
        if da <= d_target <= db:
            w = 0.0 if db == da else (d_target - da) / (db - da)
            return va + w * (vb - va)
    return None


# ── Instrument-Parser: BTC-26JUN26-100000-C ─────────────────────────────────
def parse_name(name):
    try:
        ccy, expiry, strike, cp = name.split("-")
        day = int(expiry[:-5])
        mon = MONTHS[expiry[-5:-2]]
        year = 2000 + int(expiry[-2:])
        ts = datetime(year, mon, day, 8, 0, tzinfo=timezone.utc).timestamp()
        return ccy, ts, float(strike), cp == "C"
    except (ValueError, KeyError):
        return None


# ── API ─────────────────────────────────────────────────────────────────────
def api(method, **params):
    for attempt in range(3):
        try:
            r = requests.get(API + method, params=params, timeout=30)
            r.raise_for_status()
            j = r.json()
            if "error" in j:
                raise RuntimeError(j["error"].get("message", "API error"))
            return j["result"]
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2 * (attempt + 1))


# ── Kern: Chain -> Expiry-Metriken ──────────────────────────────────────────
def build_expiries(rows, index_price, now_s):
    by_exp = {}
    for r in rows:
        parsed = parse_name(r.get("instrument_name", ""))
        mark = r.get("mark_price")
        if not parsed or not mark or mark <= 0:
            continue
        _, ts, K, is_call = parsed
        T = (ts - now_s) / YEAR_S
        if T * 365 < MIN_T_D:
            continue
        F = r.get("underlying_price") or index_price
        if not F or F <= 0:
            continue
        # OTM-Konvention: Calls für K>=F, Puts für K<F
        if not ((is_call and K >= F) or (not is_call and K < F)):
            continue
        iv = implied_vol(mark * index_price, F, K, T, is_call)
        if iv is None:
            continue
        d = call_delta(F, K, T, iv)
        if not 0.015 < d < 0.985:
            continue
        e = by_exp.setdefault(ts, {"F": [], "pts": [], "oi": 0.0})
        e["F"].append(F)
        e["pts"].append((d, iv))
        e["oi"] += r.get("open_interest") or 0.0

    out = []
    for ts, e in by_exp.items():
        pts_raw = sorted(e["pts"])
        pts = []
        for d, iv in pts_raw:                       # Delta-Duplikate mitteln
            if pts and abs(pts[-1][0] - d) < 0.004:
                pts[-1] = (pts[-1][0], (pts[-1][1] + iv) / 2)
            else:
                pts.append((d, iv))
        if len(pts) < 5:
            continue
        atm = interp_smile(pts, 0.50)
        if atm is None:
            continue
        c25, p25 = interp_smile(pts, 0.25), interp_smile(pts, 0.75)
        c10, p10 = interp_smile(pts, 0.10), interp_smile(pts, 0.90)
        out.append({
            "ts": ts,
            "days": (ts - now_s) / 86400,
            "F": sum(e["F"]) / len(e["F"]),
            "atm": atm,
            "rr25": (c25 - p25) if (c25 is not None and p25 is not None) else None,
            "rr10": (c10 - p10) if (c10 is not None and p10 is not None) else None,
            "bf25": ((c25 + p25) / 2 - atm) if (c25 is not None and p25 is not None) else None,
            "oi": e["oi"],
            "n_opts": len(pts),
        })
    out.sort(key=lambda x: x["ts"])
    return out


# ── Konstante Laufzeiten (ATM in Total-Varianz, RR/BF linear in T) ──────────
def build_const_maturities(exps):
    res = {}
    for td in TENORS_D:
        lo = max((e for e in exps if e["days"] <= td), key=lambda e: e["days"], default=None)
        hi = min((e for e in exps if e["days"] >= td), key=lambda e: e["days"], default=None)
        if lo is None or hi is None:
            res[td] = None
            continue
        if lo is hi:
            res[td] = {k: lo[k] for k in ("atm", "rr25", "rr10", "bf25")}
            continue
        w = (td - lo["days"]) / (hi["days"] - lo["days"])

        def lerp(a, b):
            return None if (a is None or b is None) else a + w * (b - a)

        var_lo = lo["atm"] ** 2 * lo["days"]
        var_hi = hi["atm"] ** 2 * hi["days"]
        res[td] = {
            "atm": math.sqrt((var_lo + w * (var_hi - var_lo)) / td),
            "rr25": lerp(lo["rr25"], hi["rr25"]),
            "rr10": lerp(lo["rr10"], hi["rr10"]),
            "bf25": lerp(lo["bf25"], hi["bf25"]),
        }
    return res


# ── Main ────────────────────────────────────────────────────────────────────
def run(currency, out_dir):
    now = datetime.now(timezone.utc)
    now_s = now.timestamp()
    date_str = now.strftime("%Y-%m-%d")

    index_price = api("get_index_price", index_name=f"{currency.lower()}_usd")["index_price"]
    book = api("get_book_summary_by_currency", currency=currency, kind="option")
    try:
        dvol_raw = api("get_volatility_index_data", currency=currency,
                       start_timestamp=int((now_s - 3 * 86400) * 1000),
                       end_timestamp=int(now_s * 1000), resolution="1D")
        dvol = dvol_raw["data"][-1][4] if dvol_raw.get("data") else None
    except Exception:
        dvol = None

    exps = build_expiries(book, index_price, now_s)
    if not exps:
        raise RuntimeError(f"{currency}: keine auswertbaren Expiries")
    cm = build_const_maturities(exps)

    os.makedirs(out_dir, exist_ok=True)
    fmt = lambda v: "" if v is None else f"{v:.6f}"

    # 1) Latest-Snapshot (vollständig, fürs Frontend optional nutzbar)
    latest = {
        "generated_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "currency": currency,
        "spot": index_price,
        "dvol": dvol,
        "const_maturities": {str(td): cm[td] for td in TENORS_D},
        "expiries": [{k: (round(v, 6) if isinstance(v, float) else v)
                      for k, v in e.items()} for e in exps],
    }
    with open(os.path.join(out_dir, f"skew_latest_{currency}.json"), "w") as f:
        json.dump(latest, f, indent=1)

    # 2) Historie: eine Zeile pro Tenor und Tag, idempotent (Tag nicht doppelt)
    hist_path = os.path.join(out_dir, f"skew_history_{currency}.csv")
    header = ["date", "tenor_d", "atm", "rr25", "rr10", "bf25", "spot", "dvol"]
    existing_dates = set()
    if os.path.exists(hist_path):
        with open(hist_path, newline="") as f:
            existing_dates = {row["date"] for row in csv.DictReader(f)}
    write_header = not os.path.exists(hist_path)
    if date_str in existing_dates:
        print(f"{currency}: {date_str} bereits in Historie — übersprungen.")
        return
    with open(hist_path, "a", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        for td in TENORS_D:
            c = cm[td]
            if c is None:
                continue
            w.writerow([date_str, td, fmt(c["atm"]), fmt(c["rr25"]),
                        fmt(c["rr10"]), fmt(c["bf25"]),
                        f"{index_price:.2f}", "" if dvol is None else f"{dvol:.2f}"])
    c30 = cm.get(30) or {}
    print(f"{currency}: ok — {len(exps)} Expiries · Spot {index_price:,.0f} · "
          f"ATM30 {100 * (c30.get('atm') or 0):.1f}% · RR25/30D "
          f"{100 * (c30.get('rr25') or 0):+.2f} Vol-Pkt.")


def main():
    ap = argparse.ArgumentParser(description="Semetrics Skew — Deribit-Pipeline")
    ap.add_argument("--currencies", nargs="+", default=["BTC", "ETH"])
    ap.add_argument("--out", default="data")
    args = ap.parse_args()
    failures = 0
    for c in args.currencies:
        try:
            run(c.upper(), args.out)
        except Exception as exc:
            failures += 1
            print(f"{c}: FEHLER — {exc}", file=sys.stderr)
    sys.exit(1 if failures == len(args.currencies) else 0)


if __name__ == "__main__":
    main()
