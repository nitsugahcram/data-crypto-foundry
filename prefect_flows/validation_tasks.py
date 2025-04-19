import pandera as pa
from pandera import Column, DataFrameSchema, Check
import pandas as pd
from prefect import task
import polars as pl
import subprocess

# Define schema
schema = DataFrameSchema(
    {
        "date": Column(pa.DateTime, unique=True),
        "price": Column(float, checks=Check.gt(10000)),
        "market_cap": Column(float, checks=Check.gt(0)),
        "volume": Column(float, nullable=False),
    }
)


@task
def validate_with_pandera(df: pd.DataFrame) -> pd.DataFrame:
    try:
        validated_df = schema.validate(df)
        print("✅ Pandera validation passed.")
        return validated_df
    except pa.errors.SchemaError as e:
        print("❌ Pandera validation failed.")
        print(str(e))
        raise


@task
def validate_with_polars(df: pl.DataFrame) -> pl.DataFrame:
    assert df.select(pl.col("price").is_not_null().all()).to_series()[0], (
        "❌ 'price' has nulls"
    )
    assert df.select((pl.col("price") > 10000).all()).to_series()[0], (
        "❌ 'price' must be > 10,000"
    )
    assert df.select(pl.col("market_cap").is_not_null().all()).to_series()[0], (
        "❌ 'market_cap' has nulls"
    )
    assert df.select(pl.col("volume").is_not_null().all()).to_series()[0], (
        "❌ 'volume' has nulls"
    )
    assert df.select(pl.col("date").is_unique()).to_series()[0], (
        "❌ 'date' must be unique"
    )

    print("✅ All Polars assertions passed.")
    return df


@task
def run_dbt_transform():
    print("⚙️ Running dbt run...")
    result = subprocess.run(
        [
            "dbt",
            "run",
            "--profiles-dir",
            "./dbt_project",
            "--project-dir",
            "./dbt_project",
        ],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception("❌ dbt run failed")
    print("✅ dbt run completed.")


@task
def run_dbt_tests():
    print("🧪 Running dbt tests...")
    result = subprocess.run(
        [
            "dbt",
            "test",
            "--profiles-dir",
            "./dbt_project",
            "--project-dir",
            "./dbt_project",
        ],
        capture_output=True,
        text=True,
        # cwd="dbt_project",  # <- ejecuta desde la carpeta donde está dbt_project.yml
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception("❌ dbt tests failed")
    print("✅ dbt tests passed.")
