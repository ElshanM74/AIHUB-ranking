# pipeline/pipeline/fetch_etender.py
from __future__ import annotations

import os
import json
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import requests

# ------------------ НАСТРОЙКИ (переопределяются через Secrets/Variables) ------------------
# Базовый endpoint. Если у etender другой путь/домен — задайте в переменной ETENDER_BASE_API.
BASE_API = (os.getenv("ETENDER_BASE_API") or "https://etender.gov.az/api/v2/tenders").rstrip("/")

# Имена параметров для дат/страниц — если на реальном API другие, задайте в ENV:
DATE_FROM_PARAM = os.getenv("ETENDER_FROM_PARAM", "from")
DATE_TO_PARAM   = os.getenv("ETENDER_TO_PARAM", "to")
PAGE_PARAM      = os.getenv("ETENDER_PAGE_PARAM", "page")

# Антифриз
PAGE_START      = int(os.getenv("ETENDER_PAGE_START", "1"))   # с какой страницы начинать
PAGE_LIMIT      = int(os.getenv("ETENDER_PAGE_LIMIT", "200")) # максимум страниц за один месяц (предохранитель)
RETRY_COUNT     = int(os.getenv("ETENDER_RETRY_COUNT", "3"))  # попыток на запрос
RETRY_SLEEP_S   = int(os.getenv("ETENDER_RETRY_SLEEP", "5"))  # пауза между повторами (сек)
TIMEOUT_S       = int(os.getenv("ETENDER_TIMEOUT_S", "30"))   # таймаут HTTP (сек)

HEADERS = {
    "Accept": "application/json, */*;q=0.8",
    "User-Agent": "aihub-bot/1.0 (+github actions)",
    "Connection": "keep-alive",
}

# ------------------ ВСПОМОГАТЕЛЬНОЕ ------------------
def month_range(year: int, month: int) -> Tuple[date, date]:
    """Границы месяца [start, end]."""
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end

def make_url(page: int, dfrom: str, dto: str) -> str:
    """Собираем URL с учетом имён параметров."""
    return f"{BASE_API}?{PAGE_PARAM}={page}&{DATE_FROM_PARAM}={dfrom}&{DATE_TO_PARAM}={dto}"

def _safe_json(resp: requests.Response) -> Optional[dict | list]:
    try:
        return resp.json()
    except Exception:
        print(f"[warn] not JSON: status={resp.status_code} sample={resp.text[:200]!r}")
        return None

def _normalize_item(row: Dict) -> Dict:
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

# ------------------ ЗАГРУЗКА ------------------
def fetch_month(y: int, m: int, raw_root: Path, start: date, end: date) -> List[Dict]:
    """
    Тянет все страницы за месяц [start; end] с повторами и мягкими ошибками.
    Сырой JSON сохраняется в raw_root/YYYY/MM. Возвращает список записей.
    """
    month_dir = raw_root / f"{y:04d}" / f"{m:02d}"
    month_dir.mkdir(parents=True, exist_ok=True)

    all_items: List[Dict] = []
    page = PAGE_START

    while page - PAGE_START < PAGE_LIMIT:
        url = make_url(page, start.isoformat(), end.isoformat())

        # Повторяем запрос до RETRY_COUNT раз
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
            except requests.RequestException as e:
                print(f"[warn] network error (try {attempt}/{RETRY_COUNT}) {url}: {e}")
                time.sleep(RETRY_SLEEP_S)
                continue

            if resp.status_code == 429:
                print(f"[warn] 429 Too Many Requests → sleep {RETRY_SLEEP_S}s & retry")
                time.sleep(RETRY_SLEEP_S)
                continue

            if resp.status_code >= 500:
                print(f"[warn] server {resp.status_code} (try {attempt}/{RETRY_COUNT}) → retry")
                time.sleep(RETRY_SLEEP_S)
                continue

            # 2xx/4xx — прекращаем повторы и обрабатываем ответ
            break
        else:
            print(f"[info] give up after {RETRY_COUNT} tries: {url}")
            return all_items

        if resp.status_code >= 400:
            print(f"[info] stop on status {resp.status_code} for {url} sample={resp.text[:200]!r}")
            break

        data = _safe_json(resp)
        if data is None:
            break

        # Возможные форматы: список, либо объект с 'results'/'items'
        if isinstance(data, list):
            page_items = data
        else:
            page_items = data.get("results") or data.get("items") or []

        if not page_items:
            print(f"[info] empty page={page} {y}-{m:02d} url={url}")
            break

        # Сохраняем сырую страницу для отладки и истории
        (month_dir / f"{y:04d}-{m:02d}-p{page}.json").write_text(json.dumps(page_items, ensure_ascii=False))
        print(f"[debug] saved {y:04d}-{m:02d}-p{page}.json rows={len(page_items)}")

        all_items.extend(page_items)
        page += 1

    return all_items

def fetch_period(start_year: int, end_year: int, raw_dir: Path) -> List[Dict]:
    """
    Идём от января start_year до декабря end_year (включительно),
    собираем все записи, возвращаем единый список.
    """
    all_rows: List[Dict] = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            start, end = month_range(y, m)
            month_rows = fetch_month(y, m, raw_dir, start, end)
            print(f"[info] {y}-{m:02d}: fetched {len(month_rows)} rows")
            all_rows.extend(month_rows)
    return all_rows

# ------------------ СБОРКА CSV ------------------
def build_master_csv(items: List[Dict], out_csv: Path) -> None:
    """
    Простейшая сборка в CSV. Если схему JSON знаем точнее — меняем mapping в _normalize_item().
    """
    import pandas as pd

    if not items:
        df = pd.DataFrame(columns=["id", "title", "date", "amount", "buyer"])
    else:
        df = pd.DataFrame([_normalize_item(x) for x in items])

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"[ok] master CSV saved: {out_csv}")
