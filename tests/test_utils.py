import polars as pl
import pytest
from prefect_flows.validation_tasks import validate_with_polars

def test_validate_pass():
    df = pl.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "price": [15000.0, 16000.0],
        "market_cap": [1_000_000, 2_000_000],
        "volume": [5000, 6000]
    }).with_columns(pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d"))

    out = validate_with_polars.fn(df)
    assert out.shape == (2, 4)

def test_validate_fail_on_price():
    df = pl.DataFrame({
        "date": ["2024-01-01", "2024-01-02"],
        "price": [500.0, 100.0],  # too low
        "market_cap": [1_000_000, 2_000_000],
        "volume": [5000, 6000]
    }).with_columns(pl.col("date").str.strptime(pl.Datetime, "%Y-%m-%d"))

    with pytest.raises(AssertionError):
        validate_with_polars.fn(df)