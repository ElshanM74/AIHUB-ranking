import os
import pandas as pd
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def classify_text(text: str) -> str:
    prompt = f"Classify this procurement item into one of these categories: [SOFT, HARD, INT, CLOUD, TRAIN, SEC, OFFICE, OTHER]. Return ONLY the label. Text: {text}"
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip().upper()

# Load the data
df = pd.read_csv("procurements.csv")

# Apply classification
df["Category"] = df["Description"].apply(classify_text)

# Save results to root folder for GitHub visibility
output_path = "../classified_results.csv"
df.to_csv(output_path, index=False)
print(f"✅ Classification complete. Results saved to {output_path}")
