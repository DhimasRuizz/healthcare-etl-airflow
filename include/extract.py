import requests
import pandas as pd
import os
from airflow.hooks.base import BaseHook

def extract_data():
    conn = BaseHook.get_connection("csv_source")
    csv_path = conn.extra_dejson.get("path", "/data/healthcare_dataset.csv")

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV source not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df.to_csv("/tmp/raw_healthcare_data.csv", index=False)
    print("Extract . . .")
    print("Data extracted successfully to /tmp!")