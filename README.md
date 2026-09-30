# Lakehouse Docker Stack

Stack local de dung lakehouse voi MinIO, Spark, Apache Iceberg, Nessie,
Trino, Airflow va Metabase.

## Version matrix

| Component | Version/Image |
| --- | --- |
| MinIO | `quay.io/minio/minio:latest` |
| Spark | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` |
| Iceberg | `1.11.0` |
| Nessie | `ghcr.io/projectnessie/nessie:0.108.4` |
| Trino | `trinodb/trino:483` |
| Airflow | `apache/airflow:3.3.1-python3.12` |
| Metabase | `metabase/metabase:v0.63.17` |
| Postgres | `postgres:16-alpine` |

Spark 4.2.0 da co release, nhung Iceberg 1.11.0 hien cung cap runtime jar
chinh thuc cho Spark 4.1/4.0/3.5. Vi vay stack pin Spark 4.1.3 de tranh
lech dependency.

Metabase dung official Starburst connection de ket noi Trino. Starburst driver
trong Metabase cung ho tro Trino, nen BI layer se query Iceberg thong qua Trino
catalog `iceberg`.

## Start

```powershell
docker compose up -d --build --remove-orphans
```

Lan dau build Airflow se cai Java, PySpark va Airflow providers nen co the mat
vai phut. Metabase khong can build rieng vi dung image official.

## UI va endpoint

| Service | URL | Login |
| --- | --- | --- |
| MinIO API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Nessie API | http://localhost:19120 | no auth |
| Spark Master UI | http://localhost:8081 | n/a |
| Spark Worker UI | http://localhost:8083 | n/a |
| Trino | http://localhost:8082 | user `trino` |
| Airflow | http://localhost:8080 | `airflow` / `airflow` |
| Metabase | http://localhost:3000 | setup lan dau tren UI |
| Postgres | `localhost:5433` | per-service users |

## Metabase

Metabase dung Postgres lam application database:

```text
postgres://metabase:metabase@postgres:5432/metabase
```

Lan dau vao http://localhost:3000, tao admin user trong UI. Sau do them database
ket noi Trino nhu sau:

```text
Database type: Starburst
Display name: Trino Iceberg
Host: trino
Port: 8080
Catalog: iceberg
Schema: de trong hoac nhap demo neu chi muon browse schema demo
Username: trino
Password: de trong
SSL: off
```

Neu UI co muc connection string, co the dung:

```text
jdbc:trino://trino:8080/iceberg
```

## Smoke test bang Spark SQL

```powershell
docker compose run --rm spark-sql -e "CREATE NAMESPACE IF NOT EXISTS nessie.demo; CREATE TABLE IF NOT EXISTS nessie.demo.events (id BIGINT, name STRING) USING iceberg; INSERT INTO nessie.demo.events VALUES (1, 'hello-lakehouse'); SELECT * FROM nessie.demo.events;"
```

Kiem tra bang Trino:

```powershell
docker compose exec trino trino --catalog iceberg --schema demo --execute "SELECT * FROM events"
```

## Airflow DAG

DAG mau `lakehouse_smoke_dag` submit PySpark job
`/opt/lakehouse/scripts/spark/iceberg_smoke.py` len Spark standalone cluster va
ghi vao `nessie.demo.airflow_events`.

## Project layout

```text
airflow/          Airflow DAGs, plugins va logs
config/           Runtime config cho Spark va Trino
infra/docker/     Dockerfiles va image requirements
infra/postgres/   Postgres bootstrap scripts
scripts/spark/    PySpark jobs dung cho Airflow hoac chay truc tiep
```

## Stop/reset

```powershell
docker compose down
```

Xoa ca data volumes:

```powershell
docker compose down -v
```

## Tai lieu tham chieu

- Spark downloads: https://spark.apache.org/downloads
- Iceberg releases: https://iceberg.apache.org/releases/
- Nessie Iceberg REST: https://projectnessie.org/guides/iceberg-rest/
- Trino Iceberg/REST catalog: https://trino.io/docs/current/object-storage/metastores.html
- Airflow Docker Compose: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- Metabase Docker: https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker
- Metabase Starburst/Trino connection: https://github.com/metabase/metabase/blob/master/docs/databases/connections/starburst.md
