from __future__ import annotations

import os
import json
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests

# ====== НАСТРОЙКИ API (можно переопределить через Secrets/Variables) ======
BASE_API = (os.getenv("ETENDER_BASE_API") or "https://etender.gov.az/api/v2/tenders").rstrip("/")
DATE_FROM_PARAM = os.getenv("ETENDER_FROM_PARAM", "from")
DATE_TO_PARAM   = os.getenv("ETENDER_TO_PARAM", "to")
PAGE_PARAM      = os.getenv("ETENDER_PAGE_PARAM", "page")
PAGE_START      = int(os.getenv("ETENDER_PAGE_START", "1"))
PAGE_LIMIT      = int(os.getenv("ETENDER_PAGE_LIMIT", "200"))  # макс. страниц/мес (антифриз)

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "aihub-bot/1.0 (+github actions)",
    "Connection": "keep-alive",
}

def make_url(page: int, date_from: str, date_to: str) -> str:
    return f"{BASE_API}?{PAGE_PARAM}={page}&{DATE_FROM_PARAM}={date_from}&{DATE_TO_PARAM}={date_to}"

def month_range(year: int, month: int) -> Tuple[date, date]:
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end

def fetch_month(cur_year: int, cur_month: int, save_dir: Path,
                start: date, end: date, timeout: int = 30) -> List[Dict]:
    save_dir.mkdir(parents=True, exist_ok=True)
    page = PAGE_START
    all_items: List[Dict] = []

    while page < PAGE_START + PAGE_LIMIT:
        url = make_url(page, start.isoformat(), end.isoformat())
        if not url.startswith("http"):
            print(f"[warn] invalid url built: {url}")
            break

        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
        except requests.RequestException as e:
            print(f"[warn] network error for {url}: {e}")
            break

        if resp.status_code != 200:
            print(f"[info] stop on status {resp.status_code} for {url}")
            break

        try:
            data = resp.json()
        except Exception:
            print(f"[warn] not a JSON for {url}, stop.")
            break

        items = data.get("results") or data.get("items") or []
        if not items:
            print(f"[info] empty page {page} for {cur_year}-{cur_month:02d}")
            break

        # сохраняем страницу для аудита
        (save_dir / f"{cur_year:04d}-{cur_month:02d}-p{page}.json").write_text(
            json.dumps(items, ensure_ascii=False)
        )

        all_items.extend(items)
        page += 1
        time.sleep(0.2)  # бережный режим

    return all_items

def fetch_period(start_year: int, end_year: int, raw_dir: Path) -> List[Dict]:
    all_rows: List[Dict] = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            start, end = month_range(y, m)
            rows = fetch_month(y, m, raw_dir, start, end)
            if rows:
                print(f"[info] {y}-{m:02d}: fetched {len(rows)} rows")
                all_rows.extend(rows)
    return all_rows

def build_master_csv(items: List[Dict], out_csv: Path) -> None:
    import pandas as pd

    if not items:
        df = pd.DataFrame(columns=["id", "title", "date", "amount", "buyer"])
    else:
        def norm(row: Dict) -> Dict:
            buyer = row.get("buyer") or row.get("procuringEntity")
            if isinstance(buyer, dict):
                buyer = buyer.get("name")
            return {
                "id": row.get("id") or row.get("tender_id"),
                "title": row.get("title") or row.get("name"),
                "date": row.get("date") or row.get("published_at"),
                "amount": row.get("amount") or row.get("value") or row.get("price"),
                "buyer": buyer,
            }
        df = pd.DataFrame([norm(x) for x in items])

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
