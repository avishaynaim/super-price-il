from __future__ import annotations

from pathlib import Path

from .connection import DB_PATH, connect

SCHEMA_SQL = Path(__file__).with_name("schema.sql")


def migrate() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    with connect() as conn:
        conn.executescript(sql)
    from .seed import seed_chains
    seed_chains()
    print(f"schema applied at {DB_PATH}")


if __name__ == "__main__":
    migrate()
