# pipeline/pipeline.py
from pathlib import Path
import os
import pandas as pd
from openai import OpenAI

# Папки для результатов
BASE = Path(__file__).resolve().parents[1]
PROC = BASE / "data" / "processed"
REPT = BASE / "data" / "reports"
PROC.mkdir(parents=True, exist_ok=True)
REPT.mkdir(parents=True, exist_ok=True)

# Клиент OpenAI (берёт ключ из GitHub Secret)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Категории, считаем «цифровыми» для рейтинга
DIGITAL = {"SOFTWARE", "IT", "SECURITY", "TRAINING", "CLOUD"}

def classify_text(text: str) -> str:
    prompt = (
        "Classify this procurement item into one of these categories: "
        "[HARDWARE, SOFTWARE, IT, SECURITY, TRAINING, CLOUD, OFFICE, OTHER]. "
        "Return ONLY the label.\n\nText: " + str(text)
    )
    r = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return r.choices[0].message.content.strip().upper()

def step_classify(input_csv: str = "procurements.csv"):
    df = pd.read_csv(BASE / input_csv)
    if "Description" not in df.columns:
        raise RuntimeError("Нужна колонка 'Description' в procurements.csv")
    if "ministry" not in df.columns:
        df["ministry"] = "UNKNOWN"
    df["Category"] = df["Description"].astype(str).apply(classify_text)
    df.to_csv(PROC / "classified.csv", index=False)
    return df

def step_aggregate():
    df = pd.read_csv(PROC / "classified.csv")
    by_m = df.groupby("ministry")
    total = by_m.size().rename("Total")
    digital = by_m.apply(lambda x: x["Category"].isin(DIGITAL).sum()).rename("Digital")
    office = by_m.apply(lambda x: (x["Category"] == "OFFICE").sum()).rename("Office")
    out = pd.concat([total, digital, office], axis=1).reset_index()
    out["DigitalShare"] = (out["Digital"] / out["Total"]).round(3)
    out["PaperPenalty"] = (out["Office"] / out["Total"]).round(3)
    out["Score"] = (out["DigitalShare"] * 100 - out["PaperPenalty"] * 20).round(1)
    out.sort_values("Score", ascending=False).to_csv(REPT / "ranking.csv", index=False)
    return out

def main():
    step_classify()           # создаст data/processed/classified.csv
    res = step_aggregate()    # создаст data/reports/ranking.csv
    print("Top-10:\n", res.head(10))

if __name__ == "__main__":
    main()
