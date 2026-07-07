.PHONY: setup up down logs clean

setup:
	@echo "Creating necessary local folders..."
	mkdir -p dags jobs
	@echo "Setup complete. Please update your .env file."

build:
	docker compose build

up: build
	docker compose up -d
	@echo "Airflow UI starting at http://localhost:8080"

down:
	docker compose down

logs:
	docker compose logs -f webserver scheduler

clean:
	docker compose down -v
	docker system prune -f
