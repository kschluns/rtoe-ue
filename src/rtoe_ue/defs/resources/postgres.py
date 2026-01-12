from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterator

import boto3
import psycopg
from dagster import resource


@dataclass(frozen=True)
class PostgresIAMConfig:
    host: str
    port: int
    dbname: str
    user: str


def _pg_iam_config_from_env() -> PostgresIAMConfig:
    return PostgresIAMConfig(
        host=os.environ["DAGSTER_PG_HOST"],
        port=int(os.environ.get("DAGSTER_PG_PORT", "5432")),
        dbname="rtoe_ue",
        user="dagster_app",
    )


def _generate_iam_auth_token(cfg: PostgresIAMConfig) -> str:
    session = boto3.session.Session()
    if not session.region_name:
        raise RuntimeError(
            "AWS region not resolved; set AWS_REGION or AWS_DEFAULT_REGION"
        )
    rds = session.client("rds")
    return rds.generate_db_auth_token(
        DBHostname=cfg.host,
        Port=cfg.port,
        DBUsername=cfg.user,
    )


@resource
def postgres_resource(_context) -> Iterator[psycopg.Connection]:
    """
    Postgres connection using AWS IAM DB authentication.
    Token lifetime ~15 minutes; safe for Dagster ops.
    """
    cfg = _pg_iam_config_from_env()
    token = _generate_iam_auth_token(cfg)
    _context.log.info(
        f"IAM Postgres connect: host={cfg.host} port={cfg.port} db={cfg.dbname} user={cfg.user}"
    )
    _context.log.info(str(token))

    conn = psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.dbname,
        user=cfg.user,
        password=token,
        sslmode="require",
        connect_timeout=10,
    )
    try:
        yield conn
    finally:
        conn.close()
