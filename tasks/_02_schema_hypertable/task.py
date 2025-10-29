# tasks/_02_schema_hypertable/task.py

# You can explore the available utilities in the `utils` folder
from utils.decorators import time_execution, db_write_once
from utils.disk import load_sql


# The "time_execution" decorator will print the execution time of this function
# The "db_write_once" decorator is ment to provide the function with a database connection with error handling and rollback for an assumed write operation.
@time_execution(sync=True, rank=False)
@db_write_once()
def init_database(cur):
    sql_query = load_sql("task.sql")
    return cur.execute(sql_query)


# The run function to be called when executing this task using the CLI. Please refer to the main README.md for instructions.
def run():
    init_database()


# Needed if this file is executed directly
if __name__ == "__main__":
    run()
