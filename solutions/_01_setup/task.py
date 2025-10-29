# solutions/_01_setup/task.py
from utils.decorators import time_execution, db_read_once


@time_execution(sync=True, rank=False)
@db_read_once
def verify_connection_solution(cursor):
    """Verify the database connection."""
    return cursor.execute("SELECT NOW();")


def run():
    """Run the setup task to verify database connection"""
    verify_connection_solution()


if __name__ == "__main__":
    run()
