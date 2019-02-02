IMAGE ?= yevdeveloper/airflow-pipeline

.PHONY: run build

build:
	docker build --rm -t $(IMAGE) .

run: build
	docker run -d -p 8080:8080 $(IMAGE)
	@echo airflow running on http://localhost:8080

kill:
	@echo "Killing airflow-pipeline containers"
	docker kill $(shell docker ps -q --filter ancestor=$(IMAGE) )

tty:
	docker exec -i -t $(shell docker ps -q --filter ancestor=$(IMAGE) ) /bin/bash

CONT-LS = $(shell docker ps -aq)