from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterator

import psycopg
from dagster import resource


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def _pg_config_from_env() -> PostgresConfig:
    return PostgresConfig(
        host=os.environ["DAGSTER_PG_HOST"],
        port=int(os.environ.get("DAGSTER_PG_PORT", "5432")),
        dbname="rtoe_ue",
        user=os.environ["DAGSTER_PG_USER"],
        password=os.environ["DAGSTER_PG_PASSWORD"],
    )


@resource
def postgres_resource(_context) -> Iterator[psycopg.Connection]:
    """
    psycopg v3 connection resource.
    Yields a connection with explicit commit/rollback handling by callers.
    """
    cfg = _pg_config_from_env()
    conn = psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        connect_timeout=10,
    )
    try:
        yield conn
    finally:
        conn.close()
