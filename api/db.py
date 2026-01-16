from contextlib import contextmanager

from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

from shared.logging import get_logger
from shared.settings import get_settings
from shared.retry import retry
from shared.migrations import apply_sql_migrations

_pool: SimpleConnectionPool | None = None
log = get_logger(__name__)

def init_pool():
    global _pool
    if _pool is None:
        s = get_settings()

        def _create_pool() -> SimpleConnectionPool:
            return SimpleConnectionPool(
                minconn=int(s.PG_POOL_MIN),
                maxconn=int(s.PG_POOL_MAX),
                host=s.PG_HOST,
                port=str(s.PG_PORT),
                dbname=s.PG_DB,
                user=s.PG_USER,
                password=s.PG_PASSWORD,
            )

        _pool = retry(_create_pool, name="postgres_pool_init")
        # Apply schema migrations once when the pool is first created.
        try:
            conn = _pool.getconn()
            try:
                apply_sql_migrations(conn)
            finally:
                _pool.putconn(conn)
        except Exception as e:  # noqa: BLE001
            # If migrations fail, the service should not start silently.
            log.info("migrations_failed", extra={"error": str(e)})
            raise
        log.info("postgres_pool_ready")

def close_pool():
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool=None

@contextmanager
def get_db():
    if _pool is None:
        init_pool()
    assert _pool is not None

    conn = _pool.getconn()
    try:
        conn.cursor_factory = RealDictCursor
        yield conn
    finally:
        _pool.putconn(conn)
