from db_connection import DuckDBConnection,db
from transform.etl_raw import RawETL
from transform.etl_curated import CuratedETL
from analysis.task_A import TaskA_Analysis
from analysis.task_B import TaskB_Analysis
from analysis.task_C import TaskC_Analysis


if __name__ == "__main__":
    print("\n------------------------- OPEN : DATABASE ------------------------------")
    db.db_open()
    print("\n------------------------- RAW LAYER : PROCESSING ------------------------------")
    raw = RawETL()
    raw.run_raw_process()
    print("\n-------------------------CURATED LAYER : PROCESSING------------------------------")
    curated =CuratedETL()
    curated.run_curated_process()
    print("\n-------------------------TAKS A : ANALYSIS------------------------------")
    task_A =TaskA_Analysis()
    task_A.run_task_A()
    print("\n-------------------------TAKS B : ANALYSIS------------------------------")
    task_b =TaskB_Analysis()
    task_b.run_task_B()
    print("\n-------------------------TAKS C : ANALYSIS------------------------------")
    task_c =TaskC_Analysis()
    task_c.run_task_C()
    print("\n------------------------- CLOSE : DATABASE ------------------------------")
    db.db_close()