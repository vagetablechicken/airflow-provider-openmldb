import json
from typing import Any, Dict, Optional, Callable

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from openmldb_provider.hooks.openmldb_api_hook import OpenMLDBAPIHook


class OpenMLDBOperator(BaseOperator):
    """
    Calls an endpoint on an OpenMLDB API Server to execute an action.

    :param openmldb_conn_id: connection to run the operator with
    :type openmldb_conn_id: str
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: str
    :param mode: The execute mode to use, default = "offsync"
    :type mode: str
    :param sql: The sql to pass
    :type sql: str
    :param headers: The HTTP headers to be added to the request
    :type headers: a dictionary of string key/value pairs
    """

    # Specify the arguments that are allowed to parse with jinja templating
    template_fields = [
        'endpoint',
        'sql',
        'headers',
    ]
    template_fields_renderers = {'headers': 'json', 'sql': 'sql'}  # TODO(hw): sql renderer?
    template_ext = ()
    ui_color = '#f4a460'

    def __init__(
            self,
            *,
            endpoint: Optional[str] = None,
            mode: str = 'offsync',
            sql: Any = None,
            headers: Optional[Dict[str, str]] = None,
            response_check: Optional[Callable[..., bool]] = None,
            response_filter: Optional[Callable[..., Any]] = None,
            extra_options: Optional[Dict[str, Any]] = None,
            openmldb_conn_id: str = 'openmldb_default',
            log_response: bool = False,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.openmldb_conn_id = openmldb_conn_id
        self.mode = mode
        self.endpoint = endpoint
        self.headers = headers or {"content-type": "application/json"}  # use json body by default
        self.sql = sql or {}
        self.response_check = response_check
        self.response_filter = response_filter
        self.extra_options = extra_options or {}
        self.log_response = log_response
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:
        from airflow.utils.operator_helpers import determine_kwargs
        if not self.sql:
            raise AirflowException('no sql')
        hook = OpenMLDBAPIHook('POST', openmldb_conn_id=self.openmldb_conn_id)

        self.log.info("Call HTTP method")
        data = {"sql": self.sql, "mode": self.mode}
        response = hook.run(self.endpoint, json.dumps(data), self.headers)
        if self.log_response:
            self.log.info(response.text)
        if self.response_check:
            kwargs = determine_kwargs(self.response_check, [response], context)
            if not self.response_check(response, **kwargs):
                raise AirflowException("Response check returned False.")
        if self.response_filter:
            kwargs = determine_kwargs(self.response_filter, [response], context)
            return self.response_filter(response, **kwargs)
        return response.text
