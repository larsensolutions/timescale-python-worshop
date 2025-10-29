# tasks/_01_setup/task.py
from utils.db import get_connection
from utils.decorators import time_execution


# The time_execution decorator helps measure how long the function takes to run
@time_execution(post=False, rank=False)
def verify_connection():
    # TODO: Use the connection to verify the database is reachable
    """Verify the database connection."""
    return


def run():
    """Run the setup task to verify database connection"""
    verify_connection()


if __name__ == "__main__":
    run()
