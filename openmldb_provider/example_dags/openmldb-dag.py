import pendulum
from airflow.decorators import dag

from openmldb_provider.operators.openmldb_operator import OpenMLDBOperator


@dag(
    start_date=pendulum.datetime(2022, 1, 1),
    schedule_interval=None,
    # ``default_args`` will get passed on to each task. You can override them on a per-task basis during
    # operator initialization.
    default_args={"retries": 2, "openmldb_conn_id": "conn_sample", },
    tags=["example"],
    default_view="graph",
)
def sample_workflow():
    """
    ### Sample DAG

    Showcases the OpenMLDB provider package's operator and sensor.

    To run this example, create an HTTP connection with:
    - id: conn_sample
    - type: http
    - host: 127.0.0.1:9080/

    endpoint: dbs/airflow_example

    You can use `airflow connections add conn_sample --conn-uri http://127.0.0.1:9080/`
    """

    ping_op = OpenMLDBOperator(task_id="ping", endpoint="dbs/airflow_example", sql="select 1")

    setup_database = OpenMLDBOperator(task_id="create-db", endpoint="dbs/airflow_example",
                                      sql="create database if not exists airflow_example")
    # task_sensor = SampleSensor(task_id="sensor", sample_conn_id="conn_sample", endpoint="")

    ping_op >> setup_database


sample_workflow_dag = sample_workflow()
