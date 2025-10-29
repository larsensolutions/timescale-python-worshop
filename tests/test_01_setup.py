import pytest
from psycopg import OperationalError
from utils.db import get_connection

MIN_TSDB_VERSION = (2, 11, 0)  # Minimum required TimescaleDB version


@pytest.fixture(scope="module")
def conn():
    try:
        c = get_connection()
    except OperationalError as e:
        pytest.skip(
            f"DB not reachable. Start it with `docker compose up -d`. Details: {e}"
        )
    yield c
    c.close()


def test_can_connect_and_select_1(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1;")
        assert cur.fetchone()[0] == 1


def test_timescaledb_extension_installed(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT extversion FROM pg_extension WHERE extname='timescaledb';")
        row = cur.fetchone()
        assert row is not None, "TimescaleDB extension is not installed"
        # Compare semantic version >= MIN_TSDB_VERSION
        ver_tuple = tuple(int(x) for x in row[0].split(".")[:3])
        assert (
            ver_tuple >= MIN_TSDB_VERSION
        ), f"TimescaleDB {row[0]} < {'.'.join(map(str, MIN_TSDB_VERSION))}"


def test_timescaledb_info_views_exist(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('timescaledb_information.hypertables');")
        assert (
            cur.fetchone()[0] is not None
        ), "timescaledb_information.hypertables view missing"


def test_temp_table_roundtrip(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE _setup_probe(x int);")
        cur.execute("INSERT INTO _setup_probe VALUES (1),(2);")
        cur.execute("SELECT count(*) FROM _setup_probe;")
        assert cur.fetchone()[0] == 2
