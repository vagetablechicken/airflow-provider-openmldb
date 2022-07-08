import pendulum
from airflow.decorators import dag
from airflow.operators.python import PythonOperator, BranchPythonOperator

import xgboost_train_sample
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
    db = "airflow_example"
    table = "airflow_table"
    raw_data = "/tmp/train_sample.csv"
    feature_path = "/tmp/feature_data"
    model_path = "/tmp/model.json"
    deploy_name = "demo"
    predict_server = "127.0.0.1:8881"

    sql_part = f"SELECT is_attributed, app, device, os, channel, hour(click_time) as hour, day(click_time) as day, " \
               f"count(channel) over w1 as qty, count(channel) over w2 as ip_app_count, " \
               f"count(channel) over w3 as ip_app_os_count FROM {table} WINDOW " \
               f"w1 as(partition by ip order by click_time ROWS_RANGE BETWEEN 1h PRECEDING AND CURRENT ROW), " \
               f"w2 as(partition by ip, app order by click_time ROWS_RANGE " \
               f"BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), " \
               f"w3 as(partition by ip, app, os order by click_time ROWS_RANGE " \
               f"BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"

    setup_database = OpenMLDBOperator(task_id="create-db", endpoint=f"dbs/{db}",
                                      sql=f"create database if not exists {db}",
                                      response_check=lambda response: response.json()['code'] == 0)

    setup_table = OpenMLDBOperator(task_id="create-table", endpoint=f"dbs/{db}",
                                   sql=f"create table if not exists {table} (ip int, app int, device int, os int, "
                                       f"channel int, click_time timestamp, is_attributed int)",
                                   response_check=lambda response: response.json()['code'] == 0)

    load_offline_data = OpenMLDBOperator(task_id="load-offline-data", endpoint=f"dbs/{db}",
                                         sql=f"LOAD DATA INFILE 'file://{raw_data}' "
                                             f"INTO TABLE {table} OPTIONS(mode='overwrite');",
                                         response_check=lambda response: response.json()['code'] == 0)

    feature_extraction = OpenMLDBOperator(task_id="feature-extract", endpoint=f"dbs/{db}",
                                          sql=f"{sql_part} INTO OUTFILE 'file://{feature_path}' OPTIONS("
                                              f"mode='overwrite')",
                                          response_check=lambda response: response.json()['code'] == 0)

    # return auc
    train = PythonOperator(task_id="train",
                           python_callable=xgboost_train_sample.train_task,
                           op_args=[f"{feature_path}/*.csv", model_path], )

    def branch_func(**kwargs):
        print("hw test", kwargs)
        ti = kwargs['ti']
        xcom_value = int(ti.xcom_pull(task_ids='train'))
        print("hw test", xcom_value)
        if xcom_value >= 99.0:
            return "deploy-sql"
        else:
            return "fail-report"

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=branch_func,
    )

    # success: deploy sql and model
    deploy_sql = OpenMLDBOperator(task_id="deploy-sql", endpoint=f"dbs/{db}",
                                  sql=f"DEPLOY {deploy_name} {sql_part}",
                                  response_check=lambda response: response.json()['code'] == 0,
                                  log_response=True)

    def update_req():
        import requests
        requests.post('http://' + predict_server + '/update', json={
            'database': db,
            'deployment': deploy_name, 'model_path': model_path
        })

    deploy = PythonOperator(task_id="deploy", python_callable=update_req)

    deploy_sql >> deploy

    # fail: report
    fail_report = PythonOperator(task_id="fail-report", python_callable=lambda: print('fail'))

    setup_database >> setup_table >> load_offline_data >> feature_extraction >> train >> branching >> [deploy_sql,
                                                                                                       fail_report]


sample_workflow_dag = sample_workflow()
