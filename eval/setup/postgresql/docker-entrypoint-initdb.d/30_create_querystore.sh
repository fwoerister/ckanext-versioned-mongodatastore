#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE ROLE querystore NOSUPERUSER NOCREATEDB NOCREATEROLE LOGIN PASSWORD 'querystore';
    CREATE DATABASE querystore OWNER ckan ENCODING 'utf-8';
    GRANT ALL PRIVILEGES ON DATABASE querystore TO ckan;
EOSQL
