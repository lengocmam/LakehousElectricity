#!/bin/sh
set -eu

if [ -n "${POSTGRES_HOST:-}" ]; then
  until pg_isready --host "$POSTGRES_HOST" --username "$POSTGRES_USER" >/dev/null 2>&1; do
    sleep 1
  done
fi

psql_cmd() {
  if [ -n "${POSTGRES_HOST:-}" ]; then
    psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOST" --username "$POSTGRES_USER" "$@"
  else
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" "$@"
  fi
}

create_user_database() {
  database="$1"
  user="$2"
  password="$3"

  psql_cmd <<-EOSQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$user') THEN
    EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '$user', '$password');
  END IF;
  EXECUTE format('ALTER ROLE %I LOGIN PASSWORD %L', '$user', '$password');
END
\$\$;
SELECT 'CREATE DATABASE "$database" OWNER "$user"'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$database')\gexec
EOSQL

  psql_cmd --dbname "$database" <<-EOSQL
GRANT ALL ON SCHEMA public TO "$user";
EOSQL
}

create_user_database airflow "${AIRFLOW_DB_USER:-airflow}" "${AIRFLOW_DB_PASSWORD:-airflow}"
create_user_database metabase "${METABASE_DB_USER:-metabase}" "${METABASE_DB_PASSWORD:-metabase}"
create_user_database nessie "${NESSIE_DB_USER:-nessie}" "${NESSIE_DB_PASSWORD:-nessie}"