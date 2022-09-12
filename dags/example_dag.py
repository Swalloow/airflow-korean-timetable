from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from timetable import BeforeWorkdayTimetable

with DAG(
    dag_id="example_workday_timetable_dag",
    start_date=datetime(2022, 8, 25),
    timetable=BeforeWorkdayTimetable(),
    catchup=True,
) as dag:
    test = BashOperator(
        task_id="test",
        bash_command="date",
    )
