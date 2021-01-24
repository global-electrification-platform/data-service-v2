PYTHON_BIN=./vpy/bin
PYTHON=$(PYTHON_BIN)/python3
GIT_HASH=$(shell git rev-parse --short HEAD)

run:
	cd api && ../$(PYTHON_BIN)/uvicorn main:app --reload --log-level debug

dev-serve: run

test:
	$(PYTHON_BIN)/pytest -s tests/test.py

vpy:
	python3 -mvenv vpy

update: vpy
	$(PYTHON_BIN)/pip install --upgrade -r requirements.txt -r dev-requirements.txt

cs-live:
	docker run --rm -d --name cs --ulimit nofile=262144:262144 -p 8123:8123 -p 9000:9000 -p 9004:9004 -v `pwd`/ch/data:/var/lib/clickhouse:delegated yandex/clickhouse-server

cs-client:
	docker exec -ti cs clickhouse-client

