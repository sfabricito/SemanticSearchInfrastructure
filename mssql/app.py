import os
import uuid
import json
import pyodbc
import datetime as dt
from pathlib import Path


def load_dotenv(env_path: Path) -> None:
    """Minimal .env loader so the script can read the same values docker-compose uses."""
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        # simple ${VAR} expansion if present
        if value.startswith("${") and value.endswith("}"):
            ref = value[2:-1]
            value = os.environ.get(ref, "")
        if key and key not in os.environ:
            os.environ[key] = value

def get_conn(server: str, port: int, user: str, password: str, database: str | None = None) -> pyodbc.Connection:
    """
    Uses ODBC Driver 18. If you only have Driver 17, change the driver name below.
    """
    driver = os.getenv("MSSQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

    db_part = f";DATABASE={database}" if database else ""
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{port};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"
        f"{db_part}"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def ensure_database(conn_master: pyodbc.Connection, db_name: str) -> None:
    # CREATE DATABASE is not allowed inside an explicit transaction; switch to autocommit
    prev_autocommit = conn_master.autocommit
    conn_master.autocommit = True
    try:
        cur = conn_master.cursor()
        cur.execute(
            """
            IF DB_ID(?) IS NULL
            BEGIN
                DECLARE @sql NVARCHAR(MAX) = N'CREATE DATABASE ' + QUOTENAME(?) + N';';
                EXEC sp_executesql @sql;
            END
            """,
            (db_name, db_name),
        )
    finally:
        conn_master.autocommit = prev_autocommit


def ensure_table(conn: pyodbc.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        IF OBJECT_ID('dbo.Crimes', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.Crimes (
                id              INT IDENTITY(1,1) PRIMARY KEY,
                crime_uuid      UNIQUEIDENTIFIER NOT NULL,
                occurred_at     DATETIME2(0)      NOT NULL,
                reported_at     DATETIME2(0)      NULL,
                offense_type    NVARCHAR(100)     NOT NULL,
                description     NVARCHAR(1000)    NULL,
                country         NVARCHAR(80)      NULL,
                state_province  NVARCHAR(80)      NULL,
                city            NVARCHAR(120)     NULL,
                address         NVARCHAR(200)     NULL,
                latitude        DECIMAL(9,6)      NULL,
                longitude       DECIMAL(9,6)      NULL,
                source_system   NVARCHAR(120)     NULL,
                created_at      DATETIME2(0)      NOT NULL CONSTRAINT DF_Crimes_created_at DEFAULT SYSUTCDATETIME(),
                CONSTRAINT UQ_Crimes_crime_uuid UNIQUE (crime_uuid)
            );

            CREATE INDEX IX_Crimes_occurred_at ON dbo.Crimes(occurred_at);
            CREATE INDEX IX_Crimes_offense_type ON dbo.Crimes(offense_type);
        END
        """
    )
    conn.commit()


def parse_dt(value):
    """
    Parses ISO-8601 datetime strings or returns None.
    """
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value
    return dt.datetime.fromisoformat(value)


def insert_sample_rows(
    conn: pyodbc.Connection,
    json_path: str = "sample.json"
) -> None:

    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    with path.open("r", encoding="utf-8") as f:
        rows = json.load(f)

    if not isinstance(rows, list):
        raise ValueError("sample.json must contain a JSON array")

    cur = conn.cursor()
    cur.fast_executemany = True

    sql = """
    INSERT INTO dbo.Crimes (
        crime_uuid, occurred_at, reported_at, offense_type, description,
        country, state_province, city, address, latitude, longitude, source_system
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    params = []
    for r in rows:
        params.append(
            (
                uuid.UUID(r["crime_uuid"]) if r.get("crime_uuid") else uuid.uuid4(),
                parse_dt(r["occurred_at"]),
                parse_dt(r.get("reported_at")),
                r["offense_type"],
                r.get("description"),
                r.get("country"),
                r.get("state_province"),
                r.get("city"),
                r.get("address"),
                r.get("latitude"),
                r.get("longitude"),
                r.get("source_system"),
            )
        )

    cur.executemany(sql, params)
    conn.commit()

    print(f"✅ Inserted {len(params)} rows from {json_path}")


def main() -> None:
    # Load local .env so the app matches docker-compose settings
    load_dotenv(Path(__file__).parent / ".env")

    # If you run from your HOST machine:
    server = os.getenv("MSSQL_SERVER", "127.0.0.1")
    port = int(os.getenv("MSSQL_PORT", "1433"))

    # If you run from ANOTHER CONTAINER in the same docker-compose network:
    # set MSSQL_SERVER=mssql

    user = os.getenv("MSSQL_USER", "sa")

    # Put the password in an env var (recommended).
    password = os.getenv("MSSQL_PASSWORD") or os.getenv("SA_PASSWORD") or "Str0ng!Passw0rd"

    db_name = os.getenv("MSSQL_DB", "CrimesDB")

    # 1) Connect to master to create DB if needed
    conn_master = get_conn(server, port, user, password, database="master")
    ensure_database(conn_master, db_name)
    conn_master.close()

    # 2) Connect to target DB and create table + insert
    conn = get_conn(server, port, user, password, database=db_name)
    ensure_table(conn)
    insert_sample_rows(conn)
    conn.close()

    print(f"✅ Done. Database='{db_name}', Table='dbo.Crimes' created and sample rows inserted.")


if __name__ == "__main__":
    main()
