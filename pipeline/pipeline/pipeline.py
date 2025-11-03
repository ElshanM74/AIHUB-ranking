name: Run AI-Hub Ranking

on:
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.10"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run classification
        env:
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
        run: |
          python - <<'EOF'
          import os
          import pandas as pd
          from openai import OpenAI

          client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

          df = pd.read_csv("procurements.csv")

          def classify(text):
              prompt = f"Classify this procurement item into one of these categories: [HARDWARE, SOFTWARE, IT, SECURITY, TRAINING]"
              response = client.chat.completions.create(
                  model="gpt-3.5-turbo",
                  messages=[{"role": "user", "content": text + ' ' + prompt}],
                  temperature=0
              )
              return response.choices[0].message.content.strip()

          df["Category"] = df["Description"].apply(classify)
          df.to_csv("classified.csv", index=False)
          print("✅ Classification complete and saved to classified.csv")
          EOF

      - name: Upload result to repository
        uses: stefanzweifel/git-auto-commit-action@v5
        with:
          commit_message: "Update classified results"
          branch: main
          file_pattern: "*.csv"
