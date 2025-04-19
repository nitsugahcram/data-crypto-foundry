import requests
import polars as pl
from prefect import task
from datetime import datetime

@task
def fetch_btc_data(days: int = 365) -> pl.DataFrame:
    print("[Fetch] Getting data from CoinGecko...")
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "usd",
        "days": str(days),
        "interval": "daily"
    }
    r = requests.get(url, params=params)
    data = r.json()

    prices = data["prices"]
    market_caps = data["market_caps"]
    volumes = data["total_volumes"]

    df = pl.DataFrame({
        "timestamp": [p[0] for p in prices],
        "price": [p[1] for p in prices],
        "market_cap": [mc[1] for mc in market_caps],
        "volume": [v[1] for v in volumes]
    })

    df = df.with_columns([
        (pl.col("timestamp") / 1000).cast(pl.Datetime).alias("date")
    ]).select(["date", "price", "market_cap", "volume"])

    print(f"[Fetch] Loaded {df.shape[0]} rows.")
    return df