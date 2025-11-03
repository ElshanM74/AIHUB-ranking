import os
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
from openai import OpenAI

from fetch_etender import fetch_period, build_master_csv

BASE = Path(__file__).resolve().parents[2]  # корень репо
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

DIGITAL = {"SOFTWARE", "IT", "SECURITY", "TRAIN", "CLOUD", "AI", "DATA"}

def classify_text(text: str) -> str:
    prompt = (
        "Classify this procurement item into one of: "
        "[SOFT, HARD, IT, SECURITY, TRAIN, CLOUD, AI, DATA, OFFICE, OTHER]. "
        "Return ONLY the label.\nText: " + str(text)
    )
    r = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return r.choices[0].message.content.strip().upper()

def daily_fetch():
    end = date.today()
    start = end - timedelta(days=7)
    base_url = "https://etender.gov.az/main/competition/index"
    fetch_period(start.isoformat(), end.isoformat(), base_url)

def backfill_fetch(start_date: str, end_date: str):
    base_url = "https://etender.gov.az/main/competition/index"
    fetch_period(start_date, end_date, base_url)

def classify_and_rank():
    proc = BASE / "procurements.csv"
    if not proc.exists():
        proc = build_master_csv()
    df = pd.read_csv(proc)

    if "Description" not in df.columns:
        df["Description"] = ""

    df["Category"] = df["Description"].astype(str).apply(classify_text)

    out_class = BASE / "pipeline" / "pipeline" / "classified.csv"
    out_class.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_class, index=False)

    if "ministry" not in df.columns:
        df["ministry"] = "UNKNOWN"

    by_m = df.groupby("ministry")
    total = by_m.size().rename("Total")
    digital = by_m.apply(lambda x: x["Category"].isin(DIGITAL).sum()).rename("Digital")
    office = by_m.apply(lambda x: (x["Category"] == "OFFICE").sum()).rename("Office")

    rank = pd.concat([total, digital, office], axis=1).reset_index()
    rank["DigitalShare"] = (rank["Digital"] / rank["Total"]).round(3)
    rank["PaperPenalty"] = (rank["Office"] / rank["Total"]).round(3)
    rank["Score"] = (rank["DigitalShare"] * 100 - rank["PaperPenalty"] * 20).round(1)

    out_rank = BASE / "pipeline" / "pipeline" / "ranking.csv"
    rank.sort_values("Score", ascending=False).to_csv(out_rank, index=False)

def main():
    mode = os.getenv("FETCH_MODE", "daily")
    if mode == "daily":
        daily_fetch()
    elif mode == "backfill":
        start = os.getenv("START_DATE", (date.today().replace(year=date.today().year - 4)).isoformat())
        end = os.getenv("END_DATE", date.today().isoformat())
        backfill_fetch(start, end)
    else:
        build_master_csv()

    classify_and_rank()
    print("✅ Done: classified.csv + ranking.csv")

if __name__ == "__main__":
    main()
