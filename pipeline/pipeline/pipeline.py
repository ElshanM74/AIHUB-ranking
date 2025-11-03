import os
from pathlib import Path
from datetime import date

import pandas as pd
from openai import OpenAI

from fetch_etender import fetch_period, build_master_csv

# Базовые папки внутри репозитория
BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "pipeline" / "raw"
PROCESSED = BASE / "pipeline" / "processed"

# Параметры периода из env (по умолчанию: 2022 … текущий год)
START_YEAR = int(os.getenv("START_YEAR", "2022"))
END_YEAR = int(os.getenv("END_YEAR", str(date.today().year)))

# OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def classify_text(text: str) -> str:
    prompt = (
        "Classify this procurement item into one of these categories: "
        "[SOFT, HARD, INT, CLOUD, TRAIN, SEC, OFFICE, OTHER]. "
        "RETURN ONLY THE LABEL.\nText: " + str(text)
    )
    resp = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return resp.choices[0].message.content.strip().upper()

def main():
    # 1) Тянем данные за период и собираем сводный CSV
    master_csv = PROCESSED / "tenders_master.csv"
    items = fetch_period(START_YEAR, END_YEAR, RAW)
    build_master_csv(items, master_csv)
    print(f"[ok] master CSV saved: {master_csv}")

    # 2) Классифицируем (если есть строки)
    df = pd.read_csv(master_csv)
    if len(df) == 0:
        print("[info] master is empty — nothing to classify (OK).")
        out_csv = PROCESSED / "classified.csv"
        df.to_csv(out_csv, index=False)
        print(f"[ok] empty classified saved: {out_csv}")
        return

    # Если у df нет столбца 'title', скорректируй ниже имя колонки.
    text_col = "title" if "title" in df.columns else df.columns[0]
    df["Category"] = df[text_col].fillna("").astype(str).apply(classify_text)

    out_csv = PROCESSED / "classified.csv"
    df.to_csv(out_csv, index=False)
    print(f"[ok] classified CSV saved: {out_csv}")

if __name__ == "__main__":
    main()
