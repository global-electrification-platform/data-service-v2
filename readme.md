# Api server for GEP

Ported from the node/postgres to python/clickhouse

Run server: ``docker run --restart=always -v /Users/marioromera/Documents/derilinx/water/volumes:/var/lib/clickhouse -d --name cs --ulimit nofile=262144:262144 -p 8123:8123 -p 9000:9000 yandex/clickhouse-server``

Run API: ``cd api && uvicorn main:app --reload``

Browse http://127.0.0.1:8000/docs


# todo

- grab countries/models tables from postgres and import to clickhouse. 
- integration testing to compare results.
