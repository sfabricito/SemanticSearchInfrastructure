# MSSQL quickstart

## Prereqs

- Docker + Docker Compose
- Python 3 with `pyodbc`
- ODBC driver installed (Driver 18 or 17). On Ubuntu/Debian:
  ```bash
  sudo apt-get update
  sudo apt-get install curl gnupg2 software-properties-common
  curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc >/dev/null
  curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
  sudo apt-get update
  sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
  ```
  If you only have Driver 17, set `MSSQL_ODBC_DRIVER=ODBC Driver 17 for SQL Server`.

## Run SQL Server

```bash
cd mssql
docker compose up -d
```

Environment is configured via `.env` (edit `SA_PASSWORD` to your own strong value).

## Load sample data

From the same directory after SQL Server is up:
```bash
python3 app.py
```
It will create database `CrimesDB`, table `dbo.Crimes`, and insert sample rows from `sample.json`.

Notes:
- `docker compose` automatically reads `.env`.
- `app.py` now auto-loads `.env` from this folder so it uses the same credentials/host/port/driver values.
