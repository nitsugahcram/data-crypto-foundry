# 🏗️ DataFoundry – Robust Data Pipeline for Crypto Analytics

**DataFoundry** is a modern data pipeline designed to ingest, validate, transform and test historical Bitcoin market data. It combines high-performance tools like Polars, DuckDB, dbt, and Prefect to ensure data quality, reproducibility and pipeline observability.

---

## 🚀 Project Goal

The goal of this pipeline is to prepare reliable, tested and ready-to-analyze Bitcoin data for downstream reporting or analytics platforms. It demonstrates modern data engineering practices such as:

- Schema and value validation (Polars assertions)
- Model-driven transformations (dbt)
- Declarative testing (dbt tests)
- Flow orchestration (Prefect)
- Reproducible environment setup

---

## ⚙️ Stack

- [Polars](https://pola-rs.github.io/polars/) – Data ingestion and validation
- [DuckDB](https://duckdb.org/) – Lightweight OLAP database for local development
- [dbt-duckdb](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb) – Data transformation + testing layer
- [Prefect](https://docs.prefect.io/) – Workflow orchestration
- [VSCode DevContainer] – Reproducible environment

---

## 📈 Flow Diagram (ASCII)

```
[CoinGecko API]
      │
      ▼
[fetch_btc_data (Polars)]
      │
      ▼
[validate_with_polars] -> [failed]
      │
      ▼
[save_to_csv → data/input.csv]
      │
      ▼
[load_to_duckdb → foundry.duckdb/raw_data]
      │
      ▼
[dbt run → btc_daily_stats]
      │
      ▼
[dbt test] -> [failed]
```

## 📁 Project Structure

```
data-foundry/
├── data/                        # CSVs and local outputs
├── dbt_project/                # dbt config and models
│   ├── dbt_project.yml
│   ├── models/
│   │   └── btc_daily_stats.sql
│   │   └── btc_daily_stats.yml
├── prefect_flows/             # Prefect + Polars pipeline code
│   └── pipeline.py
│   └── validation_tasks.py
├── foundry.duckdb             # Local DuckDB file
├── requirements.txt
├── README.md
```

---

## ▶️ Running the Pipeline

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the pipeline

```bash
python prefect_flows/pipeline.py
```

This will:

- Fetch daily Bitcoin market data from CoinGecko
- Validate the DataFrame using Polars assertions
- Save raw CSV locally (`data/input.csv`)
- Load the data into a DuckDB table (`raw_data`)
- Run dbt transformations → `btc_daily_stats`
- Run dbt tests to assert schema and value integrity

---

## 🧪 Validation Logic

### ✅ Polars (in-Python)

- `price` > 10,000
- `market_cap` > 0
- `volume` not null
- `date` unique

### ✅ dbt Tests

Defined in `btc_daily_stats.yml`:

- not_null + range test for `price_usd`
- not_null for `market_cap_billion`, `volume_million`
- unique for `date`

---

## 👁️ Flow UI (Optional)

If using Prefect locally:

```bash
prefect server start
```

Then visit: `http://localhost:4200`

---

## 📌 Future Enhancements

- Add `pytest`-based unit tests for each task
- Add Great Expectations (optional)
- Generate HTML docs with `dbt docs generate`
- Add CI pipeline (GitHub Actions)
- Integrate visualization layer (Metabase, Streamlit, etc.)

---

Built by **Agustin** – aiming to demonstrate senior-level engineering practices through clean design, modular structure and data quality focus. ✨
