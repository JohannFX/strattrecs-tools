#!/usr/bin/env python3
"""
fetch_macro_cot.py
───────────────────────────────────────────────────────────────────────────
Zieht die CFTC Disaggregated (Futures Only) und TFF (Futures Only) Reports
über die offizielle CFTC Socrata Open-Data-API, filtert auf die sechs
Semetrics-Macro-Assets (Gold, WTI, Copper, S&P 500 E-mini, US 2Y, US 10Y)
und schreibt je Reporttyp eine CSV-Datei pro Jahr — exakt im selben Muster
wie das bestehende data/cot/{year}.csv für die FX-Legacy-Daten.

Ausgabe:
  data/macro_cot_disaggregated/{year}.csv
  data/macro_cot_tff/{year}.csv

Gedacht für den Einsatz in GitHub Actions (dort ist der Netzwerkzugriff auf
publicreporting.cftc.gov uneingeschränkt möglich). Lokal ohne Internetzugriff
nicht lauffähig.

Datenquelle (offizielle CFTC Socrata-Datasets, öffentlich):
  Disaggregated – Futures Only : https://publicreporting.cftc.gov/resource/72hh-3qpy.json
  TFF – Futures Only           : https://publicreporting.cftc.gov/resource/gpe5-46if.json
"""
import csv
import io
import os
import sys
import time
import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone

SOCRATA_BASE = {
    "disaggregated": "https://publicreporting.cftc.gov/resource/72hh-3qpy.json",
    "tff": "https://publicreporting.cftc.gov/resource/gpe5-46if.json",
}

# Muss 1:1 zu MACRO_ASSETS im HTML-Modul passen (Section 2 der Spezifikation).
# WICHTIG: Dies sind PRÄFIXE, kein exakter Stringvergleich — CFTC hängt teils
# zusätzlichen Text an (Kontraktgröße etc.) oder benennt Kontrakte um. Matching
# erfolgt über starts_with(), nicht über Gleichheit. Verifiziert gegen echte
# CFTC-Live-Daten am 2026-07-12:
#   - "2-YEAR/10-YEAR U.S. TREASURY NOTES" wurde zu "UST 2Y/10Y NOTE" umbenannt
#   - "E-MINI S&P 500" wird inzwischen mit angehängtem Kontraktgrößen-Text geführt
# Falls die CFTC erneut umbenennt, hier UND im HTML-Modul
# (window.SemetricsMacroCot.MACRO_ASSETS[...].aliases) synchron nachziehen.
MACRO_ASSETS = {
    "GOLD": {
        "reportType": "disaggregated",
        "aliases": ["GOLD - COMMODITY EXCHANGE"],
    },
    "WTI": {
        "reportType": "disaggregated",
        "aliases": [
            "WTI-PHYSICAL - NEW YORK MERCANTILE EXCHANGE",
            "CRUDE OIL, LIGHT SWEET-WTI - NEW YORK MERCANTILE EXCHANGE",
        ],
    },
    "COPPER": {
        "reportType": "disaggregated",
        "aliases": ["COPPER- #1 - COMMODITY EXCHANGE", "COPPER-GRADE #1"],
    },
    "SP500": {
        "reportType": "tff",
        "aliases": [
            "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE",
        ],
    },
    "US2Y": {
        "reportType": "tff",
        "aliases": ["UST 2Y NOTE", "2-YEAR U.S. TREASURY NOTES", "2-YEAR U.S. TREASURY NOTE"],
    },
    "US10Y": {
        "reportType": "tff",
        "aliases": ["UST 10Y NOTE", "10-YEAR U.S. TREASURY NOTES", "10-YEAR U.S. TREASURY NOTE"],
    },
}

FIRST_YEAR = 2015          # ausreichend für 156W-Lookback + Puffer, an Bedarf anpassbar
OUT_DIRS = {
    "disaggregated": "data/macro_cot_disaggregated",
    "tff": "data/macro_cot_tff",
}
PAGE_SIZE = 50000  # Socrata-Maximum pro Request
USER_AGENT = "Semetrics-MacroCOT-Mirror/1.0 (+https://github.com/JohannFX/strattrecs-tools)"


def socrata_get(url, params, retries=3, backoff=2.0):
    qs = urllib.parse.urlencode(params)
    full_url = f"{url}?{qs}"
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(full_url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=60) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001 — bewusst breit, da Retry-Loop
            last_err = e
            time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Socrata-Request endgültig fehlgeschlagen: {full_url}\n{last_err}")


def fetch_report(report_type):
    """Holt ALLE Wochen für ALLE Alias-Präfixe dieses Reporttyps (paginiert)."""
    base_url = SOCRATA_BASE[report_type]
    prefixes = sorted({a for asset in MACRO_ASSETS.values() if asset["reportType"] == report_type for a in asset["aliases"]})
    # starts_with() statt Gleichheit: robust gegen CFTC-Umbenennungen und
    # angehängten Zusatztext (z.B. Kontraktgrößen-Beschreibung).
    where_clause = " OR ".join([f"starts_with(market_and_exchange_names, '{p.replace(chr(39), '')}')" for p in prefixes])
    all_rows, offset = [], 0
    while True:
        params = {
            "$where": where_clause,
            "$order": "report_date_as_yyyy_mm_dd ASC",
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        batch = socrata_get(base_url, params)
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return all_rows


def write_year_files(rows, report_type):
    if not rows:
        print(f"[warn] Keine Zeilen für Reporttyp {report_type} erhalten — nichts geschrieben.")
        return
    fieldnames = sorted({k for row in rows for k in row.keys()})
    by_year = {}
    for row in rows:
        date_str = row.get("report_date_as_yyyy_mm_dd", "")[:4]
        if not date_str.isdigit():
            continue
        year = int(date_str)
        if year < FIRST_YEAR:
            continue
        by_year.setdefault(year, []).append(row)

    out_dir = OUT_DIRS[report_type]
    os.makedirs(out_dir, exist_ok=True)
    for year, year_rows in sorted(by_year.items()):
        path = os.path.join(out_dir, f"{year}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in year_rows:
                writer.writerow(row)
        print(f"[ok] {path}: {len(year_rows)} Zeilen")


def main():
    print(f"Macro-COT-Mirror-Lauf: {datetime.now(timezone.utc).isoformat()}")
    for report_type in ("disaggregated", "tff"):
        print(f"--- {report_type} ---")
        rows = fetch_report(report_type)
        write_year_files(rows, report_type)
    print("Fertig.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"[FATAL] {e}", file=sys.stderr)
        sys.exit(1)
