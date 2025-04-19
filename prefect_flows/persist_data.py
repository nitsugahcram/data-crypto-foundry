from prefect import task
import pandas as pd
import polars as pl


@task
def _save_to_csv(df: pd.DataFrame, output_path: str = "data/input.csv"):
    df.to_csv(output_path, index=False)
    print(f"[Save] Data saved to {output_path}")


@task
def save_to_csv(df: pl.DataFrame, output_path: str = "data/input.csv"):
    df.write_csv(output_path, has_header=True)
    print(f"[Save] Data saved to {output_path}")
