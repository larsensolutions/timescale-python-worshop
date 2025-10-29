from utils.db import get_connection


def test_hypertable_exists():
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
              SELECT hypertable_name
              FROM timescaledb_information.hypertables
              WHERE hypertable_name='timeseries_data'
          """
        )
        assert cur.fetchone(), "Hypertable 'sensors' not found"
