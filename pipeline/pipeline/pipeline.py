import os
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
from openai import OpenAI

from fetch_etender import fetch_period, build_master_csv

BASE = Path(__file__).resolve().parents[
