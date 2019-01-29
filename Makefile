.PHONY: run build

build:
	docker build --rm -t airflow-pipeline .

run: build
	docker run -d -p 8080:8080 airflow-pipeline
	@echo airflow running on http://localhost:8080

kill:
	@echo "Killing docker-airflow containers"
	docker kill $(shell docker ps -q --filter ancestor=airflow-pipeline)

tty:
	docker exec -i -t $(shell docker ps -q --filter ancestor=airflow-pipeline) /bin/bash