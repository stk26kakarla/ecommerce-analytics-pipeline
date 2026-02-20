.PHONY: setup terraform-init terraform-apply generate-data generate-samples bronze-ingest silver-clean gold-aggregate run-pipeline test clean \
        dbt-deps dbt-snapshot dbt-run dbt-test dbt-docs-serve dbt-all

setup:
	docker compose up -d --build
	@echo "Waiting for services to be healthy..."
	@docker compose exec localstack bash -c "until curl -sf http://localhost:4566/_localstack/health; do sleep 2; done" > /dev/null 2>&1
	@echo "All services are up."

terraform-init:
	cd terraform && terraform init

terraform-apply:
	cd terraform && terraform apply -auto-approve

generate-data:
	docker compose exec spark python -m src.ingestion.generate_ecommerce_data

generate-samples:
	docker compose exec spark python -m src.ingestion.generate_ecommerce_data --samples-only

bronze-ingest:
	docker compose exec spark spark-submit src/spark_jobs/bronze_ingestion.py

silver-clean:
	docker compose exec spark spark-submit src/spark_jobs/silver_cleaning.py

gold-aggregate:
	docker compose exec spark spark-submit src/spark_jobs/gold_aggregation.py

## ── dbt (local, no Docker needed) ────────────────────────────────────────────
## Run all dbt targets from the repo root so data/raw/samples paths resolve.
## Prerequisites: pip install dbt-duckdb  (or: pip install -r requirements.txt)

# Install dbt packages (dbt_utils etc.)
dbt-deps:
	dbt deps --project-dir dbt

# Build / refresh the SCD2 snapshot table (must run before dbt-run)
dbt-snapshot:
	dbt snapshot --project-dir dbt --profiles-dir dbt

# Compile and run all models
dbt-run:
	dbt run --project-dir dbt --profiles-dir dbt

# Execute schema + custom tests
dbt-test:
	dbt test --project-dir dbt --profiles-dir dbt

# Generate docs and open the browser UI (Ctrl-C to stop)
dbt-docs-serve:
	dbt docs generate --project-dir dbt --profiles-dir dbt
	dbt docs serve --project-dir dbt --profiles-dir dbt

# Full dbt pipeline: install packages → snapshot → run models → test
dbt-all: dbt-deps dbt-snapshot dbt-run dbt-test

run-pipeline:
	docker compose exec spark python src/pipeline.py

test:
	docker compose exec spark pytest tests/ -v

clean:
	docker compose down -v
	cd terraform && rm -rf .terraform .terraform.lock.hcl terraform.tfstate terraform.tfstate.backup
	@echo "Cleaned up all containers, volumes, and Terraform state."
