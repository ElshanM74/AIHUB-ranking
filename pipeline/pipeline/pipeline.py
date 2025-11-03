import os
import pandas as pd
from openai import OpenAI

# Подключаем API-ключ OpenAI из GitHub Secrets
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# --- Функция классификации текста ---
def classify_text(text: str) -> str:
    prompt = f"""
    Classify this procurement item into one of these categories:
    [SOFT, HARD, INT, CLOUD, TRAIN, AI, DATA, OTHER].
    Text: {text}
    """
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip().upper()

# --- Загружаем исходные данные ---
input_path = "../../procurements.csv"
df = pd.read_csv(input_path)

# --- Применяем классификацию ---
df["Category"] = df["Description"].apply(classify_text)

# --- Сохраняем результат ---
output_path = "classified.csv"
df.to_csv(output_path, index=False)

print(f"✅ Classification complete. Results saved to {output_path}")
