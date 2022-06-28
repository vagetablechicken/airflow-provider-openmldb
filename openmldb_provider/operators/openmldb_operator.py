from typing import Any, Callable, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from openmldb_provider.hooks.openmldb_api_hook import OpenMLDBAPIHook


class OpenMLDBOperator(BaseOperator):
    """
    Calls an endpoint on an OpenMLDB API Server to execute an action.

    :param sample_conn_id: connection to run the operator with
    :type sample_conn_id: str
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
            extra_options: Optional[Dict[str, Any]] = None,
            sample_conn_id: str = 'conn_sample',
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sample_conn_id = sample_conn_id
        self.mode = mode
        self.endpoint = endpoint
        self.headers = headers or {}
        self.sql = sql or {}
        self.extra_options = extra_options or {}
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")

    def execute(self, context: Dict[str, Any]) -> Any:
        if not self.sql:
            raise AirflowException('no sql')
        hook = OpenMLDBAPIHook('POST', openmldb_conn_id=self.sample_conn_id)

        self.log.info("Call HTTP method")

        response = hook.run(self.endpoint, data, self.headers)

        return response.text
