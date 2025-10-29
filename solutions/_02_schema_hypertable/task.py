# solutions/_02_schema_hypertable/task.py
from utils.decorators import time_execution, db_write_once
from utils.disk import load_sql


@time_execution(sync=True, rank=False)
@db_write_once()
def init_database_solution(cur):
    sql_query = load_sql("task.sql")
    return cur.execute(sql_query)


def run():
    init_database_solution()


if __name__ == "__main__":
    run()
