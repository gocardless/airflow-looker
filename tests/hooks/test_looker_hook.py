import io
import json
import requests
import unittest
import requests_mock
from unittest import mock
from requests import Response
from airflow import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow_looker.hooks.looker_hook import LookerHook


class TestLookerHook(unittest.TestCase):
    def setUp(self):
        self.looker_host = "http://127.0.0.1:8080/"
        self.airflow_looker_conn_id = "connexion_defacto"
        self.looker_airflow_connection = Connection(
            conn_id=self.airflow_looker_conn_id,
            conn_type='http',
            host=self.looker_host,
            login='looker_api_client_id',
            password='looker_api_client_secret'
        )
        self.looker_auth_token = "fancy-pancy-access-token"
        self.login_response_payload = {"access_token": self.looker_auth_token}
        self.default_hook = LookerHook()
        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)

    @requests_mock.mock()
    @mock.patch.object(BaseHook, "get_connection")
    def test_create_custom_connection(self, mock_request, mock_get_connection):
        looker_auth_url = "{}{}".format(self.looker_host, "login")

        mock_request.post(
            looker_auth_url,
            status_code=200,
            text=json.dumps(self.login_response_payload),
            reason='OK'
        )
        mock_get_connection.return_value = self.looker_airflow_connection
        hook = LookerHook(looker_conn_id=self.airflow_looker_conn_id)
        conn = hook.get_conn()
        self.assertEqual(self.airflow_looker_conn_id, hook.looker_conn_id)
        self.assertEqual(self.looker_host, hook.api_endpoint)
        self.assertEqual("token {}".format(self.looker_auth_token), conn.headers['Authorization'])

    @requests_mock.mock()
    @mock.patch.object(BaseHook, "get_connection")
    def test_create_default_connection(self, mock_request, mock_get_connection):
        looker_auth_url = "{}{}".format(self.looker_host, "login")

        mock_request.post(
            looker_auth_url,
            status_code=200,
            text=json.dumps(self.login_response_payload),
            reason='OK'
        )

        self.looker_airflow_connection.conn_id = "looker_default"
        mock_get_connection.return_value = self.looker_airflow_connection
        conn = self.default_hook.get_conn()
        self.assertEqual("looker_default", self.default_hook.looker_conn_id)
        self.assertEqual(self.looker_host, self.default_hook.api_endpoint)
        self.assertEqual("token {}".format(self.looker_auth_token), conn.headers['Authorization'])

    @mock.patch.object(BaseHook, "get_connection")
    def test_create_hook_without_airflow_connection(self, mock_get_connection):
        # Here a default connection will be created with looker_conn_id='looker_default' but wont have password, login
        # attributes
        mock_get_connection.return_value = Connection(
            conn_id=self.airflow_looker_conn_id,
            conn_type='http'
        )
        with self.assertRaises(AirflowException):
            self.default_hook.get_conn()

    @mock.patch.object(BaseHook, "get_connection")
    def test_create_hook_without_airflow_connection_password(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id=self.airflow_looker_conn_id,
            conn_type='http',
            host=self.looker_host,
            login='looker_api_client_id'
        )
        with self.assertRaises(AirflowException):
            self.default_hook.get_conn()

    @mock.patch.object(BaseHook, "get_connection")
    def test_create_hook_without_airflow_connection_login(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id=self.airflow_looker_conn_id,
            conn_type='http',
            host=self.looker_host,
            password='looker_api_client_secret'
        )
        with self.assertRaises(AirflowException):
            self.default_hook.get_conn()

    @mock.patch.object(BaseHook, "get_connection")
    def test_create_hook_without_airflow_connection_host(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id=self.airflow_looker_conn_id,
            conn_type='http',
            login='looker_api_client_id',
            password='looker_api_client_secret'
        )
        with self.assertRaises(AirflowException):
            self.default_hook.get_conn()

    @requests_mock.mock()
    @mock.patch.object(BaseHook, "get_connection")
    def test_call_method_with_http_get(self, mock_request, mock_get_connection):
        mock_get_connection.return_value = self.looker_airflow_connection
        looker_auth_url = "{}{}".format(self.looker_host, "login")
        looks_url = "{}{}".format(self.looker_host, "looks")

        mock_request.post(
            looker_auth_url,
            status_code=200,
            text=json.dumps(self.login_response_payload),
            reason='OK'
        )

        mock_request.get(
            looks_url,
            status_code=200,
            text='{"object":"looker_looks_resource"}',
            reason='OK'
        )
        response = self.default_hook.call(method="GET", endpoint="looks", data=None)
        self.assertEqual("GET", response.request.method)
        self.assertEqual(looks_url, response.request.url)
        self.assertEqual(200, response.status_code)

    @requests_mock.mock()
    @mock.patch.object(BaseHook, "get_connection")
    def test_call_method_with_http_get_params(self, mock_request, mock_get_connection):
        mock_get_connection.return_value = self.looker_airflow_connection
        looker_auth_url = "{}{}".format(self.looker_host, "login")
        looks_url = "{}{}".format(self.looker_host, "looks")

        mock_request.post(
            looker_auth_url,
            status_code=200,
            text=json.dumps(self.login_response_payload),
            reason='OK'
        )

        mock_request.get(
            looks_url,
            status_code=200,
            text='{"object":"looker_looks_resource"}',
            reason='OK'
        )
        param_key = "include"
        param_value = "obsolete"
        response = self.default_hook.call(method="GET", endpoint="looks", data={param_key: param_value})
        self.assertEqual("GET", response.request.method)
        self.assertEqual("{}?{}={}".format(looks_url, param_key, param_value), response.request.url)

    @requests_mock.mock()
    @mock.patch.object(BaseHook, "get_connection")
    def test_call_method_with_http_head(self, mock_request, mock_get_connection):
        mock_get_connection.return_value = self.looker_airflow_connection
        looker_auth_url = "{}{}".format(self.looker_host, "login")
        looks_url = "{}{}".format(self.looker_host, "looks")

        mock_request.post(
            looker_auth_url,
            status_code=200,
            text=json.dumps(self.login_response_payload),
            reason='OK'
        )

        mock_request.head(
            looks_url,
            status_code=200,
            text='{"object":"looker_looks_resource"}',
            reason='OK'
        )
        response = self.default_hook.call(method="HEAD", endpoint="looks", data=None)
        self.assertEqual("HEAD", response.request.method)
        self.assertEqual(looks_url, response.request.url)
        self.assertEqual(200, response.status_code)

    @requests_mock.mock()
    @mock.patch.object(BaseHook, "get_connection")
    def test_call_method_with_request_headers(self, mock_request, mock_get_connection):
        mock_get_connection.return_value = self.looker_airflow_connection
        looker_auth_url = "{}{}".format(self.looker_host, "login")
        looks_url = "{}{}".format(self.looker_host, "looks")

        mock_request.post(
            looker_auth_url,
            status_code=200,
            text=json.dumps(self.login_response_payload),
            reason='OK'
        )

        mock_request.head(
            looks_url,
            status_code=200,
            text='{"object":"looker_looks_resource"}',
            reason='OK'
        )
        request_headers = {"looker-api-version": "2020-01-01"}
        response = self.default_hook.call(method="HEAD", endpoint="looks", data=None, headers=request_headers)
        self.assertEqual(request_headers['looker-api-version'], response.request.headers['looker-api-version'])
        self.assertEqual("HEAD", response.request.method)
        self.assertEqual(looks_url, response.request.url)
        self.assertEqual(200, response.status_code)

    @requests_mock.mock()
    @mock.patch.object(BaseHook, "get_connection")
    def test_call_method_with_http_post(self, mock_request, mock_get_connection):
        mock_get_connection.return_value = self.looker_airflow_connection
        looker_auth_url = "{}{}".format(self.looker_host, "login")
        looks_url = "{}{}".format(self.looker_host, "looks")

        mock_request.post(
            looker_auth_url,
            status_code=200,
            text=json.dumps(self.login_response_payload),
            reason='OK'
        )

        mock_request.post(
            looks_url,
            status_code=200,
            text='{"object":"looker_looks_resource"}',
            reason='OK'
        )
        request_payload = '{"post_object":"looker_looks_resource"}'
        response = self.default_hook.call(method="POST", endpoint="looks", data=request_payload)
        self.assertEqual("POST", response.request.method)
        self.assertEqual(looks_url, response.request.url)
        self.assertEqual(json.dumps(request_payload), response.request.body)
        self.assertEqual(200, response.status_code)

    @mock.patch.object(LookerHook, 'call')
    def test_get_look_sql_correct_args(self, mock_request):
        mock_response = Response()
        mock_response.status_code = 200
        mock_sql_query = b'''
SELECT * FROM music_albums
WHERE artist = "cardi b"
'''
        mock_response.raw = io.BytesIO(mock_sql_query)
        mock_request.return_value = mock_response

        look_id = 1234
        query_fetch_method = 'GET'
        endpoint = "{}/{}/run/{}".format('api/3.0/looks', look_id, 'sql')

        query = self.default_hook.get_look_sql(look_id)
        mock_request.assert_called_once_with(
            data=None,
            endpoint=endpoint,
            method=query_fetch_method
        )
        self.assertEqual(mock_sql_query.decode('utf8'), query)

    @mock.patch.object(LookerHook, 'call')
    def test_get_look_sql_with_looker_unsuccessful_response(self, mock_request):
        mock_response = Response()
        mock_response.status_code = 400
        mock_sql_query = b'''
        SELECT * FROM music_albums
        WHERE artist = "cardi b"
        '''
        mock_response.raw = io.BytesIO(mock_sql_query)
        mock_request.return_value = mock_response

        with self.assertRaises(AirflowException):
            self.default_hook.get_look_sql(134)

    def test_get_look_sql_missing_look_id(self):
        with self.assertRaises(AirflowException):
            self.default_hook.get_look_sql()


suite = unittest.TestLoader().loadTestsFromTestCase(TestLookerHook)
unittest.TextTestRunner(verbosity=2).run(suite)
