import os
from pathlib import Path
from datetime import date
import pandas as pd
from openai import OpenAI

from fetch_etender import fetch_period, build_master_csv


# === БАЗОВЫЕ ПАПКИ ===
BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "pipeline" / "raw"
PROCESSED = BASE / "pipeline" / "processed"

# создаём папку processed, если её нет
PROCESSED.mkdir(parents=True, exist_ok=True)


# === НАСТРОЙКИ ===
START_YEAR = int(os.getenv("START_YEAR", "2022"))
END_YEAR = int(os.getenv("END_YEAR", str(date.today().year)))

# OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# === КЛАССИФИКАЦИЯ ===
def classify_text(text: str) -> str:
    """Классифицирует текст тендера по категориям"""
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


# === ОСНОВНАЯ ФУНКЦИЯ ===
def main():
    print("🚀 Starting AI-Hub ranking pipeline...")

    # 1️⃣ Загрузка тендерных данных
    master_csv = PROCESSED / "tenders_master.csv"
    items = fetch_period(START_YEAR, END_YEAR, RAW)
    build_master_csv(items, master_csv)
    print(f"[✅] Master CSV saved: {master_csv}")

    # 2️⃣ Классификация данных
    df = pd.read_csv(master_csv)
    if len(df) == 0:
        print("[ℹ️] No data found — skipping classification.")
        out_csv = PROCESSED / "classified.csv"
        df.to_csv(out_csv, index=False)
        print(f"[✅] Empty classified file saved: {out_csv}")
        return

    text_col = "title" if "title" in df.columns else df.columns[0]
    df["Category"] = df[text_col].fillna("").astype(str).apply(classify_text)

    out_csv = PROCESSED / "classified.csv"
    df.to_csv(out_csv, index=False)
    print(f"[✅] Classified file saved: {out_csv}")


# === ЗАПУСК ===
if __name__ == "__main__":
    main()
