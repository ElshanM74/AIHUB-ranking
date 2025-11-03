import os
import time
import csv
import re
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE = Path(__file__).resolve().parents[2]  # корень репозитория
RAW_DIR = BASE / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "AI-Hub Azerbaijan (contact: info@ai-hub.az) - academic/research use"
}

def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _parse_list_page(html: str):
    """Возвращает список тендеров с одной страницы."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("table tbody tr")
    items = []
    for r in rows:
        cols = r.find_all("td")
        if len(cols) < 4:
            continue
        tender_no = _clean_text(cols[0].get_text())
        org = _clean_text(cols[1].get_text())
        desc = _clean_text(cols[2].get_text())
        deadline = _clean_text(cols[3].get_text())
        # если есть ссылка на детали (для суммы/категории) — собираем
        link = cols[0].find("a")
        href = link["href"] if link and link.has_attr("href") else None
        items.append({
            "tender_no": tender_no,
            "organization": org,
            "Description": desc,
            "deadline": deadline,
            "detail_url": href
        })
    return items

def _parse_detail_page(html: str):
    """Пытаемся вытащить сумму/валюту/коды из карточки тендера (если есть)."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    # простые эвристики (подкорректируем после первого прогона)
    amount = None
    currency = None
    m = re.search(r"(?i)(\d[\d\s.,]{3,})\s*(AZN|USD|EUR|₼|\$|€)", text)
    if m:
        amount = m.group(1)
        currency = m.group(2)
    cpv = None
    m2 = re.search(r"(?i)CPV[:\s]+([0-9]{8}-\d)", text)
    if m2:
        cpv = m2.group(1)
    return {"amount_raw": amount, "currency_raw": currency, "cpv": cpv}

def fetch_month(year: int, month: int, base_url: str, max_pages: int = 50, sleep_sec: float = 1.0):
    """
    Грузим страницы листинга за месяц (без «официального API» — HTML-парсинг).
    Если у etender есть параметры даты/страниц — подставим их в base_url заранее.
    """
    all_items = []
    for page in range(1, max_pages + 1):
        # пример листинга; при необходимости подстрой: ?page={page}
        url = f"{base_url}?page={page}"
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code != 200:
            break
        items = _parse_list_page(r.text)
        if not items:
            break

        # фильтр на месяц по дедлайну, если в таблице есть даты в человекочитаемом формате
        month_items = []
        for it in items:
            date_str = it.get("deadline")
            # попытки распарсить дату (подкорректируем формат после первой проверки)
            parsed = None
            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    parsed = datetime.strptime(date_str, fmt).date()
                    break
                except Exception:
                    pass
            it["deadline_parsed"] = parsed
            month_items.append(it)

        # опционально фильтруем по месяцу, если парсинг даты удался
        filtered = []
        for it in month_items:
            d = it.get("deadline_parsed")
            if d and d.year == year and d.month == month:
                filtered.append(it)
            elif d is None:
                # если дату не распознали — оставляем (чтобы не потерять)
                filtered.append(it)

        all_items.extend(filtered)
        time.sleep(sleep_sec)

    # обогащение (деталка) — осторожно, лимитируем запросы
    out_rows = []
    for it in all_items[:300]:  # ограничим, чтобы не злоупотреблять (настрой)
        row = dict(it)
        if it.get("detail_url"):
            try:
                detail_url = it["detail_url"]
                if detail_url.startswith("/"):
                    detail_url = "https://etender.gov.az" + detail_url
                r2 = requests.get(detail_url, headers=HEADERS, timeout=30)
                if r2.status_code == 200:
                    detail = _parse_detail_page(r2.text)
                    row.update(detail)
                time.sleep(0.7)
            except Exception:
                pass
        out_rows.append(row)

    df = pd.DataFrame(out_rows)
    raw_path = RAW_DIR / f"tenders_{year}_{month:02d}.csv"
    df.to_csv(raw_path, index=False)
    return raw_path

def build_master_csv():
    """Склеиваем все raw CSV в единый procurements.csv (с дедупликацией по tender_no)."""
    files = sorted(RAW_DIR.glob("tenders_*.csv"))
    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f))
        except Exception:
            pass
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    if "tender_no" in df.columns:
        df = df.drop_duplicates(subset=["tender_no"])
    df.rename(columns={"organization": "ministry"}, inplace=True)
    # минимальный набор полей для твоего классификатора
    if "Description" not in df.columns:
        df["Description"] = df.get("desc", "")
    out_path = BASE / "procurements.csv"
    df.to_csv(out_path, index=False)
    return out_path

def fetch_period(start_date: str, end_date: str, base_url: str):
    """Качаем помесячно от start_date до end_date включительно."""
    start = datetime(int(start_date), 1, 1).date()
    end = datetime(int(end_date), 12, 31).date()
    cur = start.replace(day=1)
    while cur <= end:
        fetch_month(cur.year, cur.month, base_url=base_url)
        # следующий месяц
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return build_master_csv()
