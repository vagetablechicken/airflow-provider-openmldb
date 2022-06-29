"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_openmldb_operator.TestOpenMLDBOperator

"""

import json
import logging
import unittest
from unittest import mock

import requests_mock

# Import Operator
from airflow.exceptions import AirflowException

from openmldb_provider.operators.openmldb_operator import OpenMLDBOperator

log = logging.getLogger(__name__)


def assert_ok(response_text):
    res_json = json.loads(response_text)
    assert res_json == {"code": 0, 'msg': 'ok'}


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_CONN_SAMPLE='http://http%3A%2F%2Fwww.httpbin.org%2F',
                 AIRFLOW_CONN_OPENMLDB_DEFAULT='http://http%3A%2F%2F127.0.0.1%3A9080%2F')
class TestOpenMLDBOperator(unittest.TestCase):
    """
    Test OpenMLDB Operator.
    """

    @requests_mock.mock()
    def test_operator(self, m):
        # Mock endpoint
        m.post('http://www.httpbin.org/', json={'data': 'mocked response'})

        operator = OpenMLDBOperator(
            task_id='run_operator',
            openmldb_conn_id='conn_sample',
            sql='xx'
        )

        # Airflow calls the operator's execute method at runtime with the task run's bespoke context dictionary
        response_payload = operator.execute(context={})
        response_payload_json = json.loads(response_payload)

        log.info(response_payload_json)

        # Assert the API call returns expected mocked payload
        assert response_payload_json['data'] == 'mocked response'

    def test_operator_with_empty_sql(self):
        operator = OpenMLDBOperator(
            task_id='run_operator',
            sql='')
        with self.assertRaises(AirflowException) as exc:
            operator.execute({})
        assert str(exc.exception) == 'no sql'

    def test_operator_with_sql(self):
        assert_ok(OpenMLDBOperator(task_id='setup-database', endpoint='dbs/airflow_test',
                                   sql='create database if not exists airflow_test').execute({}))
        assert_ok(OpenMLDBOperator(task_id='setup-table', endpoint='dbs/airflow_test',
                                   sql='create table if not exists airflow_table(c1 int)').execute({}))
        # TODO(hw): response doesn't have the result data now, so just do an offline query here
        assert_ok(OpenMLDBOperator(task_id='feature-extraction-offline', endpoint='dbs/airflow_test',
                                   sql='select * from t1').execute({}))
        # do an online query
        assert_ok(OpenMLDBOperator(task_id='feature-extraction-online', endpoint='dbs/airflow_test',
                                   mode='online',
                                   sql='select * from t1').execute({}))


if __name__ == '__main__':
    unittest.main()
