"""
Unittest module to test Hooks.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_sample_hook.TestSampleHook

"""
import json
import logging
import unittest
from unittest import mock

import requests_mock

# Import Hook
from openmldb_provider.hooks.openmldb_api_hook import OpenMLDBAPIHook

log = logging.getLogger(__name__)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_CONN_SAMPLE='http://https%3A%2F%2Fwww.httpbin.org%2F')
@mock.patch.dict('os.environ',
                 AIRFLOW_CONN_OPENMLDB_DEFAULT='http://http%3A%2F%2F127.0.0.1%3A9080%2Fdbs%2Fairflow_test')
class TestOpenMLDBAPIHook(unittest.TestCase):
    """
    Test OpenMLDB API Hook.
    """

    @requests_mock.mock()
    def test_post(self, m):
        # Mock endpoint
        m.post('https://www.httpbin.org/', json={'data': 'mocked response'})

        # Instantiate hook
        hook = OpenMLDBAPIHook(
            openmldb_conn_id='conn_sample',
            method='post'
        )

        # Sample Hook's run method executes an API call
        response = hook.run()

        # Retrieve response payload
        payload = response.json()

        # Assert success status code
        assert response.status_code == 200

        # Assert the API call returns expected mocked payload
        assert payload['data'] == 'mocked response'

    @requests_mock.mock()
    def test_get(self, m):
        # Mock endpoint
        m.get('https://www.httpbin.org/', json={'data': 'mocked response'})

        # Instantiate hook
        hook = OpenMLDBAPIHook(
            openmldb_conn_id='conn_sample',
            method='get'
        )

        # Sample Hook's run method executes an API call
        response = hook.run()

        # Retrieve response payload
        payload = response.json()

        # Assert success status code
        assert response.status_code == 200

        # Assert the API call returns expected mocked payload
        assert payload['data'] == 'mocked response'

    def test_query_api_server_without_data(self):
        hook = OpenMLDBAPIHook()
        # no data
        response = hook.run()
        res = json.loads(response.text)
        assert res == {'code': -1, 'msg': 'Json parse failed'}

    def test_query_api_server_with_sql(self):
        hook = OpenMLDBAPIHook()
        response = hook.run(data='{"sql":"select 1", "mode":"offsync"}')
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}

    def test_query_api_server_without_mode(self):
        hook = OpenMLDBAPIHook()
        response = hook.run(data='{"sql":"select 1"}')
        res = json.loads(response.text)
        assert res['code'] == -1
        assert res['msg'].startswith('Json parse failed')

    def test_query_api_server(self):
        hook = OpenMLDBAPIHook()
        # We can send ddl by post too, but not recommended for users.
        # Here just do it for tests, mode won't affect
        response = hook.run(data='{"sql": "create database if not exists airflow_test", "mode": "online"}',
                            headers={"content-type": "application/json"})
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}

        response = hook.run(data='{"sql":"create table if not exists airflow_table(c1 int)", "mode":"online"}',
                            headers={"content-type": "application/json"})
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}

        # an offline sync query
        response = hook.run(data='{"sql":"select * from airflow_table", "mode":"offsync"}',
                            headers={"content-type": "application/json"})
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}

        # an online query(always sync)
        response = hook.run(data='{"sql":"select * from airflow_table", "mode":"online"}',
                            headers={"content-type": "application/json"})
        res = json.loads(response.text)
        assert res == {'code': 0, 'msg': 'ok'}


if __name__ == '__main__':
    unittest.main()
