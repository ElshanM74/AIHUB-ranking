from pathlib import Path
import pandas as pd
from openai import OpenAI
import os

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw"
PROC = BASE / "data" / "processed"
REPT = BASE / "data" / "reports"
for p in [RAW, PROC, REPT]:
    p.mkdir(parents=True, exist_ok=True)

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
DIGITAL = {"SOFT", "INT", "CLOUD", "SEC", "TRAIN"}

def classify_text(text: str) -> str:
    prompt = ("Classify this procurement item into one of "
              "[SOFT, HARD, INT, CLOUD, TRAIN, SEC, OFFICE, OTHER]. "
              "Return ONLY the label.\nText: " + str(text))
    r = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return r.choices[0].message.content.strip().upper()

def step_classify(input_csv="all_raw.csv"):
    df = pd.read_csv(RAW / input_csv)
    if "description" not in df.columns:
        raise RuntimeError("Нужна колонка 'description'")
    if "ministry" not in df.columns:
        df["ministry"] = "UNKNOWN"
    df["Category"] = df["description"].astype(str).apply(classify_text)
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
    step_classify()
    res = step_aggregate()
    print("Top-10:\n", res.head(10))

if __name__ == "__main__":
    main()
