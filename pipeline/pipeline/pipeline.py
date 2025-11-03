import os
from pathlib import Path
from datetime import date
import pandas as pd
from openai import OpenAI

# Импорт модуля для загрузки тендеров
from fetch_etender import fetch_period, build_master_csv

# Настройка OpenAI клиента
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Базовая папка проекта
BASE = Path(__file__).resolve().parent
RAW = BASE / "raw"
RAW.mkdir(exist_ok=True)

# ---- 1) Скачиваем тендеры за последние 3 года ----
this_year = date.today().year
start_year = this_year - 3       # можно поменять на -4, если хочешь 4 года
end_year = this_year

fetch_period(start_year, end_year, RAW)
build_master_csv(RAW, "procurements.csv")

# ---- 2) Классификация тендеров ----
def classify_text(text: str) -> str:
    prompt = (
        f"Classify this procurement item into one of these categories: "
        f"[SOFT, HARD, INT, CLOUD, TRAIN, SEC, OFFICE, OTHER]. "
        f"RETURN ONLY the label.\nText: {text}"
    )
    r = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return r.choices[0].message.content.strip().upper()

# ---- 3) Загружаем CSV и применяем классификацию ----
df = pd.read_csv("procurements.csv")
df["Category"] = df["Description"].apply(classify_text)

# ---- 4) Сохраняем результат ----
out_path = "pipeline/classified.csv"
df.to_csv(out_path, index=False)
print(f"✅ Classification complete. Results saved to {out_path}")
