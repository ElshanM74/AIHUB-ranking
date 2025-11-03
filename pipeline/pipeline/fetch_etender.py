# pipeline/pipeline/fetch_etender.py
from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import requests


# ==============================
# НАСТРОЙКИ (можно переопределять через Secrets/Env)
# ==============================
# Базовый endpoint. Если у etender другой — положи правильный в секрет/вар:
#   BASE_API=https://...  (Settings → Secrets → Actions)
BASE_API = (
    # приоритет: ENV → дефолт
    (os.getenv("ETENDER_BASE_API") or "https://etender.gov.az/api/v2/tenders").rstrip("/")
)

# Имена параметров дат. Если у API не 'from'/'to', подставь свои:
DATE_FROM_PARAM = os.getenv("ETENDER_FROM_PARAM", "from")
DATE_TO_PARAM   = os.getenv("ETENDER_TO_PARAM", "to")
PAGE_PARAM      = os.getenv("ETENDER_PAGE_PARAM", "page")

HEADERS = {
    "Accept": "application/json, */*;q=0.8",
    "User-Agent": "aihub-bot/1.0 (+github actions)",
    "Connection": "keep-alive",
}

DEFAULT_TIMEOUT = 30
RETRY_429_SLEEP = 5  # сек между попытками при 429


def month_range(year: int, month: int) -> Tuple[date, date]:
    """Границы месяца [start, end]."""
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def make_url(page: int, date_from: str, date_to: str) -> str:
    """Собираем URL с учётом имён параметров."""
    return f"{BASE_API}?{PAGE_PARAM}={page}&{DATE_FROM_PARAM}={date_from}&{DATE_TO_PARAM}={date_to}"


def fetch_month(cur_year: int, cur_month: int, raw_root: Path,
                start: date, end: date, timeout: int = DEFAULT_TIMEOUT) -> List[Dict]:
    """
    Качает все страницы за месяц [start; end], кладёт JSON в raw_root/YYYY/MM,
    возвращает список записей.
    """
    month_dir = raw_root / f"{cur_year:04d}" / f"{cur_month:02d}"
    month_dir.mkdir(parents=True, exist_ok=True)

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

        # обработка частых кейсов
        if resp.status_code == 429:
            print(f"[warn] 429 Too Many Requests for {url} → sleep {RETRY_429_SLEEP}s & retry")
            time.sleep(RETRY_429_SLEEP)
            continue
        if resp.status_code >= 400:
            print(f"[info] stop on status {resp.status_code} for {url}; text[:200]={resp.text[:200]!r}")
            break

        # пробуем распарсить JSON
        try:
            data = resp.json()
        except Exception:
            print(f"[warn] not a JSON for {url}; text[:200]={resp.text[:200]!r} → stop.")
            break

        # На разных API ключ может быть 'results' или 'items' (или список в корне)
        if isinstance(data, list):
            items = data
        else:
            items = data.get("results") or data.get("items") or []

        # пустая страница — конец пагинации
        if not items:
            print(f"[info] empty page {page} for {cur_year}-{cur_month:02d}")
            break

        # сохраняем страницу (для истории/отладки)
        out = month_dir / f"{cur_year:04d}-{cur_month:02d}-p{page}.json"
        out.write_text(json.dumps(items, ensure_ascii=False))
        print(f"[debug] saved {out.name} with {len(items)} rows")

        all_items.extend(items)
        page += 1

    return all_items


def fetch_period(start_year: int, end_year: int, raw_dir: Path) -> List[Dict]:
    """
    С января start_year по декабрь end_year включительно.
    Возвращает все записи за период.
    """
    all_rows: List[Dict] = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            start, end = month_range(y, m)
            rows = fetch_month(y, m, raw_dir, start, end)
            print(f"[info] {y}-{m:02d}: fetched {len(rows)} rows")
            all_rows.extend(rows)
    return all_rows


def build_master_csv(items: List[Dict], out_csv: Path) -> None:
    """
    Простейшая сборка в CSV. Если нужно — переименуй ключи под фактический JSON.
    """
    import pandas as pd

    def norm(row: Dict) -> Dict:
        buyer = row.get("buyer") or row.get("procuringEntity")
        if isinstance(buyer, dict):
            buyer = buyer.get("name")

        return {
            "id": row.get("id") or row.get("tender_id") or row.get("tenderId"),
            "title": row.get("title") or row.get("name") or row.get("subject"),
            "date": row.get("date") or row.get("published_at") or row.get("publishDate"),
            "amount": row.get("amount") or row.get("value") or row.get("price"),
            "buyer": buyer,
        }

    if not items:
        df = pd.DataFrame(columns=["id", "title", "date", "amount", "buyer"])
    else:
        df = pd.DataFrame([norm(x) for x in items])

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"[ok] master CSV saved: {out_csv}")
