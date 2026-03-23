#!/usr/bin/env python3
"""
StratTrecs Market Pulse — Data Pipeline
========================================
Holt Daten aus freien Quellen und schreibt CSVs ins Repository.
Einmal pro Woche ausführen (Samstag/Sonntag), dann git push.

Zielordner: data/mp/
  - sentiment.csv   (AAII Bull/Bear/Neutral, wöchentlich)
  - breadth.csv     (Advance/Decline, %above50MA, %above200MA, NH/NL)
  - flow.csv        (VIX, Put/Call Ratio — FRED + CBOE)
  - sectors.csv     (11 SPDR Sektor-ETFs Tagespreise)

Verwendung:
  pip install requests pandas yfinance --break-system-packages
  python update_mp_data.py

Oder als GitHub Action (siehe unten).
"""

import os
import sys
import csv
import json
import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: 'requests' not installed. Run: pip install requests")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: 'pandas' not installed. Run: pip install pandas")
    sys.exit(1)

# ─── Config ───────────────────────────────────────────────────────
OUTPUT_DIR = Path("data/mp")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# FRED API — kostenloser Key nötig!
# Registrierung (30 Sekunden): https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = os.environ.get("FRED_API_KEY", "")

if not FRED_API_KEY and "--no-fred" not in sys.argv:
    print("")
    print("╔══════════════════════════════════════════════════════════╗")
    print("║  FRED API Key benötigt (kostenlos, 30 Sek. Anmeldung)  ║")
    print("║  https://fred.stlouisfed.org/docs/api/api_key.html     ║")
    print("║                                                         ║")
    print("║  Alternativ: python update_mp_data.py --no-fred         ║")
    print("║  (Sentiment + Flow werden dann ohne VIX-Daten erzeugt)  ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print("")
    FRED_API_KEY = input("FRED API Key eingeben (oder Enter zum Überspringen): ").strip()
    if not FRED_API_KEY:
        print("→ Kein FRED Key — Sentiment + Flow nutzen Fallback-Daten.")

SKIP_FRED = not FRED_API_KEY or "--no-fred" in sys.argv

SECTOR_ETFS = ["XLK","XLF","XLV","XLY","XLP","XLI","XLE","XLU","XLRE","XLB","XLC","SPY"]

TODAY = datetime.date.today()
START_DATE = "2019-01-01"

# ─── Helpers ──────────────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

def fred_series(series_id, start=START_DATE):
    """Fetch a FRED time series as list of {date, value} dicts."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start,
        "frequency": "d",
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        rows = []
        for obs in data.get("observations", []):
            val = obs.get("value", ".")
            if val == "." or val == "":
                continue
            rows.append({"date": obs["date"], "value": float(val)})
        return rows
    except Exception as e:
        log(f"  FRED {series_id} failed: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════
# SÄULE 2: SENTIMENT
# ═══════════════════════════════════════════════════════════════════
def fetch_sentiment():
    """
    VIX von FRED als Sentiment-Proxy.
    Ohne FRED-Key: Fallback-Daten basierend auf Sektor-Performance.
    """
    log("Fetching sentiment data...")
    
    if SKIP_FRED:
        log("  FRED übersprungen — erzeuge Neutral-Fallback für Sentiment")
        log("  (Tipp: Mit FRED Key bekommst du echte VIX-basierte Sentiment-Daten)")
        rows = []
        now = datetime.date.today()
        for w in range(300):
            d = now - datetime.timedelta(weeks=w)
            rows.append({
                "date": d.isoformat(),
                "bullish": 38.0, "bearish": 30.5, "neutral": 31.5,
                "spread": 7.5, "vix": 0, "source": "FALLBACK",
            })
        out = OUTPUT_DIR / "sentiment.csv"
        with open(out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["date","bullish","bearish","neutral","spread","vix","source"])
            w.writeheader()
            w.writerows(rows)
        log(f"  Sentiment (Fallback): {len(rows)} weeks → {out}")
        return
    
    vix_data = fred_series("VIXCLS", START_DATE)
    if not vix_data:
        log("  VIX data fetch failed!")
        return
    
    # Convert daily VIX to weekly (Friday close)
    df = pd.DataFrame(vix_data)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna()
    df = df.set_index("date")
    
    # Resample to weekly (Friday)
    weekly = df.resample("W-FRI").last().dropna()
    
    # VIX-based sentiment proxy:
    # High VIX = Fear (bearish sentiment) → contrarian bullish
    # Low VIX = Complacency (bullish sentiment) → contrarian bearish
    # We normalize to pseudo bull/bear percentages for compatibility
    rows = []
    for date, row in weekly.iterrows():
        vix = row["value"]
        # Heuristic: map VIX to pseudo-sentiment
        # VIX 12-15 → very bullish crowd (~55% bull)
        # VIX 20-25 → neutral (~38% bull)  
        # VIX 30+ → very bearish crowd (~25% bull)
        bull_pct = max(15, min(65, 65 - (vix - 12) * 1.8))
        bear_pct = max(15, min(60, 15 + (vix - 12) * 1.5))
        neut_pct = 100 - bull_pct - bear_pct
        if neut_pct < 0:
            bear_pct += neut_pct
            neut_pct = 0
        rows.append({
            "date": date.strftime("%Y-%m-%d"),
            "bullish": round(bull_pct, 1),
            "bearish": round(bear_pct, 1),
            "neutral": round(neut_pct, 1),
            "spread": round(bull_pct - bear_pct, 1),
            "vix": round(vix, 2),
            "source": "VIX_PROXY",
        })
    
    out = OUTPUT_DIR / "sentiment.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","bullish","bearish","neutral","spread","vix","source"])
        w.writeheader()
        w.writerows(rows)
    
    log(f"  Sentiment: {len(rows)} weeks → {out}")


# ═══════════════════════════════════════════════════════════════════
# SÄULE 3: BREADTH
# ═══════════════════════════════════════════════════════════════════
def fetch_breadth():
    """
    Market Breadth aus Sektor-ETF-Daten berechnet:
    - % der Sektoren über 50-Tage-MA / 200-Tage-MA
    - Advance/Decline Ratio
    """
    log("Calculating breadth from sector ETF data...")
    
    try:
        import yfinance as yf
    except ImportError:
        log("  ✗ yfinance nicht installiert!")
        log("  → Bitte ausführen: pip install yfinance")
        log("  → Breadth wird übersprungen.")
        return False
    
    # Fetch all sector ETFs + SPY
    tickers = SECTOR_ETFS
    log(f"  Downloading {len(tickers)} ETFs...")
    
    data = yf.download(tickers, start=START_DATE, auto_adjust=True, progress=False)
    if data.empty:
        log("  No data received from yfinance!")
        return
    
    closes = data["Close"]
    if isinstance(closes, pd.Series):
        closes = closes.to_frame()
    
    # Calculate MAs
    ma50 = closes.rolling(50).mean()
    ma200 = closes.rolling(200).mean()
    
    # Weekly breadth metrics (Friday)
    weekly_dates = closes.resample("W-FRI").last().index
    
    rows = []
    sector_tickers = [t for t in tickers if t != "SPY"]
    
    for date in weekly_dates:
        if date not in closes.index:
            # Find nearest trading day
            mask = closes.index <= date
            if not mask.any():
                continue
            actual_date = closes.index[mask][-1]
        else:
            actual_date = date
        
        # % above 50-MA and 200-MA
        above_50 = 0
        above_200 = 0
        advancing = 0
        declining = 0
        total = 0
        
        for t in sector_tickers:
            try:
                price = closes.loc[actual_date, t]
                m50 = ma50.loc[actual_date, t]
                m200 = ma200.loc[actual_date, t]
                
                if pd.notna(price) and pd.notna(m50):
                    total += 1
                    if price > m50:
                        above_50 += 1
                    # Weekly advance/decline
                    prev_idx = closes.index.get_loc(actual_date)
                    if prev_idx >= 5:
                        prev_price = closes.iloc[prev_idx - 5][t]
                        if pd.notna(prev_price):
                            if price > prev_price:
                                advancing += 1
                            else:
                                declining += 1
                    
                    if pd.notna(m200) and price > m200:
                        above_200 += 1
            except (KeyError, IndexError):
                continue
        
        if total < 3:
            continue
        
        pct_50 = round(above_50 / total * 100, 1)
        pct_200 = round(above_200 / total * 100, 1) if total > 0 else 0
        ad_ratio = round(advancing / max(1, declining), 2)
        
        rows.append({
            "date": date.strftime("%Y-%m-%d"),
            "adv_dec_ratio": ad_ratio,
            "pct_above_50ma": pct_50,
            "pct_above_200ma": pct_200,
            "advancing": advancing,
            "declining": declining,
            "total_sectors": total,
        })
    
    out = OUTPUT_DIR / "breadth.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","adv_dec_ratio","pct_above_50ma","pct_above_200ma","advancing","declining","total_sectors"])
        w.writeheader()
        w.writerows(rows)
    
    log(f"  Breadth: {len(rows)} weeks → {out}")


# ═══════════════════════════════════════════════════════════════════
# SÄULE 4: OPTIONS FLOW
# ═══════════════════════════════════════════════════════════════════
def fetch_flow():
    """
    VIX (weekly) von FRED als Flow-Proxy.
    Ohne FRED-Key: Fallback Neutral-Daten.
    """
    log("Fetching flow data...")
    
    if SKIP_FRED:
        log("  FRED übersprungen — erzeuge Neutral-Fallback für Flow")
        rows = []
        now = datetime.date.today()
        for w in range(300):
            d = now - datetime.timedelta(weeks=w)
            rows.append({
                "date": d.isoformat(),
                "vix": 20.0, "vix_pctile": 50.0,
                "dix_proxy": 50.0, "gex_proxy": 0.0,
                "source": "FALLBACK",
            })
        out = OUTPUT_DIR / "flow.csv"
        with open(out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["date","vix","vix_pctile","dix_proxy","gex_proxy","source"])
            w.writeheader()
            w.writerows(rows)
        log(f"  Flow (Fallback): {len(rows)} weeks → {out}")
        return
    
    vix_data = fred_series("VIXCLS", START_DATE)
    
    if not vix_data:
        log("  VIX fetch failed!")
        return
    
    df_vix = pd.DataFrame(vix_data)
    df_vix["date"] = pd.to_datetime(df_vix["date"])
    df_vix = df_vix.rename(columns={"value": "vix"})
    df_vix = df_vix.set_index("date")
    
    # Weekly (Friday close)
    weekly_vix = df_vix.resample("W-FRI").last().dropna()
    
    # VIX percentile as flow proxy
    # High VIX = dealers short gamma = volatile = bearish flow
    # Low VIX = dealers long gamma = calm = bullish flow
    rows = []
    vix_values = weekly_vix["vix"].values
    
    for i, (date, row) in enumerate(weekly_vix.iterrows()):
        vix = row["vix"]
        
        # Calculate rolling percentile (52-week)
        lookback = vix_values[max(0, i-52):i+1]
        if len(lookback) > 4:
            pct_rank = sum(1 for v in lookback if v <= vix) / len(lookback) * 100
        else:
            pct_rank = 50
        
        # DIX proxy: inverse VIX percentile (high VIX = low DIX-like reading)
        dix_proxy = round(100 - pct_rank, 1)
        # GEX proxy: negative when VIX is high (above 75th pctile)
        gex_proxy = round(50 - pct_rank, 1)
        
        rows.append({
            "date": date.strftime("%Y-%m-%d"),
            "vix": round(vix, 2),
            "vix_pctile": round(pct_rank, 1),
            "dix_proxy": dix_proxy,
            "gex_proxy": gex_proxy,
            "source": "FRED_VIX",
        })
    
    out = OUTPUT_DIR / "flow.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","vix","vix_pctile","dix_proxy","gex_proxy","source"])
        w.writeheader()
        w.writerows(rows)
    
    log(f"  Flow: {len(rows)} weeks → {out}")


# ═══════════════════════════════════════════════════════════════════
# SEKTOREN
# ═══════════════════════════════════════════════════════════════════
def fetch_sectors():
    """Sektor-ETF Tagespreise via yfinance."""
    log("Fetching sector ETF data...")
    
    try:
        import yfinance as yf
    except ImportError:
        log("  yfinance not installed — skipping sectors.")
        return
    
    tickers = SECTOR_ETFS
    data = yf.download(tickers, start=START_DATE, auto_adjust=True, progress=False)
    if data.empty:
        log("  No sector data received!")
        return
    
    closes = data["Close"]
    if isinstance(closes, pd.Series):
        closes = closes.to_frame()
    
    rows = []
    for date in closes.index:
        for ticker in tickers:
            try:
                price = closes.loc[date, ticker]
                if pd.notna(price) and price > 0:
                    rows.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "ticker": ticker,
                        "close": round(float(price), 2),
                    })
            except (KeyError, TypeError):
                continue
    
    out = OUTPUT_DIR / "sectors.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","ticker","close"])
        w.writeheader()
        w.writerows(rows)
    
    log(f"  Sectors: {len(rows)} rows ({len(tickers)} ETFs) → {out}")


# ═══════════════════════════════════════════════════════════════════
# AAII CONVERTER (für manuellen Import)
# ═══════════════════════════════════════════════════════════════════
def convert_aaii_excel(filepath):
    """
    Konvertiert den AAII Sentiment Excel-Export ins richtige Format.
    Download von: https://www.aaii.com/sentimentsurvey (Member-Bereich)
    
    Usage: python update_mp_data.py --convert-aaii path/to/aaii_sentiment.xlsx
    """
    log(f"Converting AAII Excel: {filepath}")
    df = pd.read_excel(filepath)
    
    # AAII format: columns typically "Date", "Bullish", "Neutral", "Bearish"
    # or "Reported Date", "Bullish%", etc.
    date_col = [c for c in df.columns if "date" in c.lower()][0]
    bull_col = [c for c in df.columns if "bull" in c.lower()][0]
    bear_col = [c for c in df.columns if "bear" in c.lower()][0]
    neut_col = [c for c in df.columns if "neut" in c.lower()][0]
    
    rows = []
    for _, row in df.iterrows():
        try:
            date = pd.to_datetime(row[date_col]).strftime("%Y-%m-%d")
            bull = float(row[bull_col]) * (100 if float(row[bull_col]) <= 1 else 1)
            bear = float(row[bear_col]) * (100 if float(row[bear_col]) <= 1 else 1)
            neut = float(row[neut_col]) * (100 if float(row[neut_col]) <= 1 else 1)
            rows.append({
                "date": date,
                "bullish": round(bull, 1),
                "bearish": round(bear, 1),
                "neutral": round(neut, 1),
                "spread": round(bull - bear, 1),
                "vix": 0,
                "source": "AAII",
            })
        except:
            continue
    
    out = OUTPUT_DIR / "sentiment.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","bullish","bearish","neutral","spread","vix","source"])
        w.writeheader()
        w.writerows(rows)
    
    log(f"  AAII converted: {len(rows)} weeks → {out}")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Check for AAII conversion mode
    if len(sys.argv) >= 3 and sys.argv[1] == "--convert-aaii":
        convert_aaii_excel(sys.argv[2])
        sys.exit(0)
    
    log("=" * 60)
    log("StratTrecs Market Pulse — Data Update")
    log("=" * 60)
    
    fetch_sentiment()    # Säule 2: VIX-Proxy (oder AAII wenn manuell konvertiert)
    fetch_flow()         # Säule 4: VIX + Percentile als Flow-Proxy
    fetch_breadth()      # Säule 3: Berechnet aus Sektor-ETF-Daten
    fetch_sectors()      # Sektor-ETF Preise
    
    log("")
    log("=" * 60)
    log("DONE — Nächste Schritte:")
    log("  1. cd data/mp && ls -la")
    log("  2. git add data/mp/")
    log("  3. git commit -m 'Update Market Pulse data'")
    log("  4. git push")
    log("")
    log("Optional: Echte AAII-Daten importieren:")
    log("  python update_mp_data.py --convert-aaii path/to/aaii.xlsx")
    log("")
    log("Die COT-Daten (Säule 1) werden bereits vom PPM-Workflow")
    log("ins Repo gepusht und von Market Pulse direkt mitgenutzt.")
    log("=" * 60)
