import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_data():
    df = pd.read_csv("/tmp/clean_healthcare_data.csv")

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    