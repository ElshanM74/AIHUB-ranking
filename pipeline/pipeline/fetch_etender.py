from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

import requests

# ==============================
# НАСТРОЙКИ HTTP API (интернет)
# ==============================
# Это примерный endpoint. Если он другой — просто поменяй строку ниже.
BASE_API = "https://etender.gov.az/api/v2/tenders"

# Заголовки (при необходимости можно дополнить)
HEADERS = {
    "Accept": "application/json, */*;q=0.8",
    "User-Agent": "aihub-bot/1.0 (+github actions)",
    "Connection": "keep-alive",
}

def make_url(page: int, date_from: str, date_to: str) -> str:
    """
    Собираем HTTP-URL для запроса. Если у etender.gov.az другой формат
    параметров, поменяй имена 'from'/'to' на нужные.
    """
    return f"{BASE_API}?page={page}&from={date_from}&to={date_to}"


# ==============================
# КАЧАЕМ ДАННЫЕ ПО МЕСЯЦАМ
# ==============================

def fetch_month(cur_year: int, cur_month: int, save_dir: Path,
                start: date, end: date, timeout: int = 30) -> List[Dict]:
    """
    Скачивает страницы за месяц [start; end], сохраняет JSON-страницы в save_dir,
    возвращает список записей (items).
    """
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
            # Не падаем пайплайном; просто выходим из цикла
            print(f"[warn] network error for {url}: {e}")
            break

        if resp.status_code != 200:
            # Либо пусто, либо неверные параметры — считаем, что страниц больше нет
            print(f"[info] stop on status {resp.status_code} for {url}")
            break

        try:
            data = resp.json()
        except Exception:
            print(f"[warn] not a JSON for {url}, stop.")
            break

        # На разных API ключ может называться 'results' или 'items'
        items = data.get("results") or data.get("items") or []
        if not items:
            print(f"[info] empty page {page} for {cur_year}-{cur_month:02d}")
            break

        # сохраняем страницу (для истории/отладки)
        out = save_dir / f"{cur_year:04d}-{cur_month:02d}-p{page}.json"
        out.write_text(json.dumps(items, ensure_ascii=False))

        all_items.extend(items)
        page += 1

    return all_items


def month_range(year: int, month: int) -> (date, date):
    """Возвращает границы месяца [start, end]."""
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def fetch_period(start_year: int, end_year: int, raw_dir: Path) -> List[Dict]:
    """
    Проходит с января start_year до декабря end_year включительно.
    Возвращает список всех записей за период.
    """
    all_rows: List[Dict] = []
    for y in range(start_year, end_year + 1):
        for m in range(1, 13):
            start, end = month_range(y, m)
            # Если месяц выходит за end_year — закончим после декабря end_year
            if y == end_year and m == 12:
                pass
            rows = fetch_month(y, m, raw_dir, start, end)
            if rows:
                print(f"[info] {y}-{m:02d}: fetched {len(rows)} rows")
                all_rows.extend(rows)
    return all_rows


def build_master_csv(items: List[Dict], out_csv: Path) -> None:
    """
    Простейшая сборка в CSV. Для реальной модели колонок подстрой ключи.
    Если список пуст — создадим пустой CSV с заголовками.
    """
    import pandas as pd

    if not items:
        # Минимальные заголовки, чтобы файл существовал
        df = pd.DataFrame(columns=["id", "title", "date", "amount", "buyer"])
    else:
        # Подстрой ключи под реальную структуру API
        def norm(row: Dict) -> Dict:
            return {
                "id": row.get("id") or row.get("tender_id"),
                "title": row.get("title") or row.get("name"),
                "date": row.get("date") or row.get("published_at"),
                "amount": row.get("amount") or row.get("value") or row.get("price"),
                "buyer": (row.get("buyer") or row.get("procuringEntity") or {}).get("name")
                        if isinstance(row.get("buyer") or row.get("procuringEntity"), dict)
                        else row.get("buyer") or row.get("procuringEntity"),
            }

        df = pd.DataFrame([norm(x) for x in items])

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
