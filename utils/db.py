import os
import psycopg
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """
    Returns a psycopg connection object. Use directly, or the decorator functions conn_read/conn_write.
    """
    return psycopg.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "password"),
        dbname=os.getenv("DB_NAME", "postgres"),
        port=int(os.getenv("DB_PORT", 5432)),
    )
