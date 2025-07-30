install:
	poetry install

lint:
	poetry run black src tests
	poetry run ruff check src tests
	poetry run mypy src

lint-fix:
	poetry run black src tests
	poetry run ruff check --fix src tests
	poetry run ruff format src tests

pre-commit-install:
	poetry run pre-commit install

pre-commit-run:
	poetry run pre-commit run --all-files

test:
	pytest tests

run:
	poetry run python run_local_pipeline.py

# Docker commands
docker-build:
	docker-compose build

docker-up:
	docker-compose up -d

airflow-start:
	chmod +x scripts/start_airflow.sh
	./scripts/start_airflow.sh

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f airflow

docker-shell:
	docker-compose exec airflow bash

docker-clean:
	docker-compose down -v --remove-orphans
	docker system prune -f

# Airflow management commands
airflow-cleanup:
	docker-compose exec airflow python /opt/airflow/scripts/cleanup_stuck_tasks.py

airflow-restart:
	docker-compose restart airflow

airflow-logs:
	docker-compose logs -f airflow

airflow-shell:
	docker-compose exec airflow bash

# Quick pipeline run
pipeline-run:
	docker-compose exec airflow airflow dags trigger ad_data_pipeline

pipeline-status:
	docker-compose exec airflow airflow dags list-runs -d ad_data_pipeline

# Development commands
dev-setup: install
	mkdir -p data/bronze data/silver data/gold logs

dev-run: dev-setup
	poetry run python run_local_pipeline.py --mode mock

# Production commands
prod-build: docker-build
prod-up: docker-up
prod-down: docker-down 