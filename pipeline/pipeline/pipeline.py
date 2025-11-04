from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from fetch_etender import fetch_period, build_master_csv

# Папки внутри репозитория
BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "pipeline" / "raw"
PROCESSED = BASE / "pipeline" / "processed"

START_YEAR = int(os.getenv("START_YEAR", "2022"))
END_YEAR = int(os.getenv("END_YEAR", str(date.today().year)))

def main():
    print(f"[info] fetching period {START_YEAR}..{END_YEAR}")
    items = fetch_period(START_YEAR, END_YEAR, RAW)
    out_csv = PROCESSED / "tenders_master.csv"
    build_master_csv(items, out_csv)
    print(f"[ok] saved: {out_csv}  rows={len(items)}")

if __name__ == "__main__":
    main()
