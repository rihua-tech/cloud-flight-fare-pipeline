SHELL := /bin/bash

.PHONY: setup up down load dbt-build analyze lint test demo

setup:
	python -m pip install -r requirements.txt

up:
	docker compose up -d postgres

down:
	docker compose down

load:
	python scripts/load_sample_to_postgres.py

dbt-build:
	cp dbt/flight_fares/profiles.yml.example ~/.dbt/profiles.yml
	cd dbt/flight_fares && dbt deps && dbt build

analyze:
	python scripts/run_analysis_queries.py

lint:
	ruff check .

test:
	pytest -q

demo: up setup load dbt-build analyze
	@echo "Demo complete. Optional: python ml/train_buy_wait.py"
