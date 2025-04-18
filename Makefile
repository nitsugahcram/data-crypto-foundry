.PHONY: init run test clean

init:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
	cd dbt_project && dbt deps

run:
	. .venv/bin/activate && python prefect_flows/pipeline.py

integration_test:
	cd dbt_project && dbt run --profiles-dir . && dbt test --profiles-dir .

unit_test:
	pytest tests

clean:
	rm -rf __pycache__ .venv foundry.duckdb data/input.csv dbt_packages