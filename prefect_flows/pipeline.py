from prefect import flow, task
import pandas as pd
import duckdb
from persist_data import save_to_csv

from fetch_btc_data_polars import fetch_btc_data
from validation_tasks import validate_with_polars, run_dbt_tests, run_dbt_transform


@task
def load_to_duckdb(df: pd.DataFrame, table_name: str = "raw_data"):
    print(f"[Load] Writing to DuckDB table: {table_name}")
    conn = duckdb.connect("foundry.duckdb")
    conn.register("temp_df", df)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
    conn.close()
    print(f"[Load] Table `{table_name}` created in foundry.duckdb")


@flow(name="DataFoundry Pipeline with Pandera + dbt")
def data_crypto_foundry_pipeline():
    df = fetch_btc_data()
    df = validate_with_polars(df)
    save_to_csv(df)
    load_to_duckdb(df)
    run_dbt_transform()  # 🧱 primero construye el modelo
    run_dbt_tests()  # 🧪 luego corre los tests


if __name__ == "__main__":
    data_crypto_foundry_pipeline()
