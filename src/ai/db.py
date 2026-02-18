from __future__ import annotations

from dataclasses import dataclass

import psycopg
import streamlit as st
from psycopg import sql


@dataclass(frozen=True)
class DbConfig:
    dbname: str
    host: str
    user: str
    password: str
    schema_staging: str


def validate_config(config: DbConfig) -> list[str]:
    missing = []
    if not config.dbname:
        missing.append("DBNAME")
    if not config.host:
        missing.append("HOST")
    if not config.user:
        missing.append("DB_USER")
    if not config.password:
        missing.append("PASSWORD")
    if not config.schema_staging:
        missing.append("SCHEMA_STAGING")
    return missing


def _connect_kwargs(config: DbConfig) -> dict[str, str]:
    return {
        "dbname": config.dbname,
        "host": config.host,
        "user": config.user,
        "password": config.password,
    }


@st.cache_resource
def get_connection(config: DbConfig) -> psycopg.Connection:
    return psycopg.connect(
        dbname=config.dbname,
        host=config.host,
        user=config.user,
        password=config.password,
    )


def check_connection(config: DbConfig) -> bool:
    conn = get_connection(config)
    with conn.cursor() as cur:
        cur.execute("select 1;")
        cur.fetchone()
    return True


@st.cache_data(ttl=60)
def fetch_row_count(config: DbConfig, table: str) -> int:
    query = sql.SQL("select count(*) from {}.{}").format(
        sql.Identifier(config.schema_staging),
        sql.Identifier(table),
    )
    conn = get_connection(config)
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        return int(row[0]) if row else 0


@st.cache_data(ttl=60)
def fetch_range(config: DbConfig, table: str, column: str) -> tuple[object, object]:
    query = sql.SQL("select min({}), max({}) from {}.{}").format(
        sql.Identifier(column),
        sql.Identifier(column),
        sql.Identifier(config.schema_staging),
        sql.Identifier(table),
    )
    conn = get_connection(config)
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        if not row:
            return None, None
        return row[0], row[1]
