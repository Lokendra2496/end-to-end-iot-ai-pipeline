from __future__ import annotations

from pathlib import Path

import psycopg2

from shared.logging import get_logger

log = get_logger(__name__)


def apply_sql_migrations(conn) -> None:
    """
    Apply .sql migrations from the repo's migrations/ directory.

    This is a lightweight alternative to Alembic for this repo. It is safe to run
    on startup because migrations are written idempotently.
    """
    repo_root = Path(__file__).resolve().parents[1]
    migrations_dir = repo_root / "migrations"
    if not migrations_dir.exists():
        log.info("migrations_dir_missing_skipping", extra={"migrations_dir": str(migrations_dir)})
        return

    sql_files = sorted(migrations_dir.glob("*.sql"))
    if not sql_files:
        log.info("no_migrations_found", extra={"migrations_dir": str(migrations_dir)})
        return

    with conn.cursor() as cur:
        for path in sql_files:
            sql = path.read_text(encoding="utf-8")
            cur.execute(sql)
            log.info("migration_applied", extra={"migration": path.name})
    conn.commit()

