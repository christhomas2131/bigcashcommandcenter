"""
db/connection.py — Postgres connection pool.

Uses a module-level ThreadedConnectionPool so connections are reused
across Streamlit reruns without leaking. Max 5 connections (safe for
Render's free Postgres tier, which allows 25).
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras
import psycopg2.pool

import config

_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_lock = threading.Lock()


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _lock:
            if _pool is None:  # double-checked locking
                _pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=5,
                    dsn=config.get_database_url(),
                )
    return _pool


def close_pool() -> None:
    """Close all pool connections. Call on app shutdown if needed."""
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


_STALE_ERRORS = (psycopg2.InterfaceError, psycopg2.OperationalError)


def _fresh_conn() -> tuple:
    """Return (pool, conn), resetting the pool if the connection is stale."""
    pool = _get_pool()
    conn = pool.getconn()
    if conn.closed:
        try:
            pool.putconn(conn)
        except Exception:
            pass
        close_pool()
        pool = _get_pool()
        conn = pool.getconn()
    return pool, conn


@contextmanager
def get_conn() -> Generator:
    """
    Yield a pooled Postgres connection.
    Commits on clean exit, rolls back on exception, always returns to pool.
    Auto-retries once on stale/dropped connections (InterfaceError, OperationalError).
    """
    pool, conn = _fresh_conn()
    try:
        yield conn
        conn.commit()
    except _STALE_ERRORS:
        # Server dropped the connection — reset pool and let caller retry.
        close_pool()
        raise
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            pool.putconn(conn)
        except Exception:
            pass


@contextmanager
def cursor(row_dict: bool = True) -> Generator:
    """
    Yield an auto-closing cursor inside a managed connection.
    row_dict=True (default) returns rows as dicts via RealDictCursor.
    """
    factory = psycopg2.extras.RealDictCursor if row_dict else None
    with get_conn() as conn:
        cur = conn.cursor(cursor_factory=factory)
        try:
            yield cur
        finally:
            cur.close()
