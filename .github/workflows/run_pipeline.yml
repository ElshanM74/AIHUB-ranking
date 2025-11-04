name: Run AI-Hub Ranking

on:
  workflow_dispatch:
  schedule:
    - cron: '0 3 * * *' # каждый день в 03:00 UTC = 07:00 по Баку

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Debug folder structure
        run: |
          echo "📁 Current directory:"
          pwd
          echo "📄 Files in this directory:"
          ls -R

      - name: Run pipeline
        run: |
          echo "🚀 Starting pipeline..."
          python3 pipeline/pipeline/pipeline.py
          echo "✅ Pipeline completed."

      - name: Save results (artifact only)
        uses: actions/upload-artifact@v4
        with:
          name: ai-hub-results
          path: pipeline/pipeline/processed/*.csv
