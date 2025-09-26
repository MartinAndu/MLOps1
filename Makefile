build:
	@docker compose build --no-cache airflow-webserver airflow-scheduler airflow-init

init:
	@docker compose up airflow-init

start:
	@docker compose up -d

restart:
	@docker compose restart airflow-webserver airflow-scheduler

stop:
	@docker compose down
