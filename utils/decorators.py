import time

from functools import wraps
from typing import Callable, Any
from utils.db import get_connection

from threading import Lock

from utils.api import post_async, post_sync

# global state for throttling
_next_allowed_at = 0.0
_lock = Lock()


def time_execution(
    sync: bool = False, post: bool = True, rank: bool = True
) -> Callable:
    """
    Decorator to measure and print the execution time of a function.
    """

    def _decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            global _next_allowed_at
            start_time = time.monotonic()

            # Execute the original function
            result = func(*args, **kwargs)

            end_time = time.monotonic()
            duration = end_time - start_time

            # Print the results
            print(f"⏱️  {func.__name__} finished in {duration:.4f} seconds.")
            if not post:
                return result

            payload = {
                "funcName": func.__name__,
                "value": round(duration, 4),
                "uom": "s",
                "rank": rank,
            }
            if sync:
                try:
                    post_sync(payload)
                except Exception:
                    pass
            else:
                with _lock:
                    now = time.monotonic()
                    if now >= _next_allowed_at:
                        _next_allowed_at = now + 0
                        post_async(payload)

            return result

        return wrapper

    return _decorator


def db_read_once(func):
    """
    Decorator for once off single read database connection.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        conn = get_connection()
        cur = conn.cursor()
        try:
            return func(cur, *args, **kwargs)
        finally:
            cur.close()
            conn.close()

    return wrapper


def db_write_once(autocommit: bool = False):
    """
    Decorator for once off single write database connection with commit/rollback.
    """

    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            conn = get_connection()
            try:
                if autocommit:
                    conn.autocommit = True
                cur = conn.cursor()
                try:
                    result = func(cur, *args, **kwargs)
                finally:
                    cur.close()
                if not autocommit:
                    conn.commit()
                return result
            except Exception:
                if not autocommit:
                    conn.rollback()
                raise
            finally:
                conn.close()

        return wrapper

    return deco
