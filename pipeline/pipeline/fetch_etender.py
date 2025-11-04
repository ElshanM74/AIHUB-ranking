from __future__ import annotations

import os
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import requests

# Базовый endpoint (можно переопределить переменной окружения)
BASE_API = (os.getenv("ETENDER_BASE_API") or "https://etender.gov.az/api/v2/tenders").rstrip("/")

HEADERS = {
    "Accept": "application/json, */*;q=0.8",
    "User-Agent": "aihub-bot/1.0 (+github actions)",
    "Connection": "keep-alive",
}

def make_url(page: int, date_from: str, date_to: str) -> str:
    # При необходимости переименуй параметры 'from'/'to' под реальный API
    return f"{BASE_API}?page={page}&from={date_from}&to={date_to}"

def month_range(year: int, month: int) -> Tuple[date, date]:
    start = date(year, month, 1)
    end = (date(year + (month == 12), (month % 12) + 1, 1) - timedelta(days=1))
    return start, end

def fetch_month(cur_year: int, cur_month: int, save_dir: Path,
                start: date, end: date, timeout: int = 30) -> List[Dict]:
    save_dir.mkdir(parents=True, exist_ok=True)
    page = 1
    all_items: List[Dict] = []

    while True:
        url = make_url(page, start.isoformat(), end.isoformat())
        if not url.startswith("http"):
            raise ValueError(f"Invalid API url: {url}")

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

        # Сохраняем сырой снимок страницы (на будущее/отладку)
        (save_dir / f"{cur_year:04d}-{cur_month:02d}-p{page}.json").write_text(
            json.dumps(items, ensure_ascii=False)
        )

        all_items.extend(items)
        page += 1

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
