.PHONY: setup terraform-init terraform-apply generate-data generate-samples run-pipeline test clean

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

run-pipeline:
	docker compose exec spark python src/pipeline.py

test:
	docker compose exec spark pytest tests/ -v

clean:
	docker compose down -v
	cd terraform && rm -rf .terraform .terraform.lock.hcl terraform.tfstate terraform.tfstate.backup
	@echo "Cleaned up all containers, volumes, and Terraform state."
