import requests
import json
from urllib.parse import urljoin

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class LookerHook(BaseHook):
    """
    A hook to talk to the Looker API over HTTP
    """
    def __init__(self,
                 looker_conn_id='looker_default',
                 verify=True):
        """
        :param looker_conn_id: connection that has the host i.e
        https://looker.company.com:19999/api/3.0/, the login (client_id) and
        the password (client_secret).
        :type looker_conn_id: string
        :param verify: true if we should verify API calls. Defaults to true.
        :type verify: boolean
        """
        self.looker_conn_id = looker_conn_id
        self.verify = verify
        self.api_endpoint = None

    def get_conn(self):
        """
        Returns http session for use with requests
        """
        conn = self.get_connection(self.looker_conn_id)

        if conn.host is None:
            _message = "Failed to initialize looker airflow connector, connection host not provided"
            self.logger.error(_message)
            raise AirflowException(_message)

        if conn.login is None:
            _message = "Failed to initialize looker Airflow connector, connection login not provided"
            self.logger.error(_message)
            raise AirflowException(_message)

        if conn.password is None:
            _message = "Failed to initialize looker Airflow connector, connection password not provided"
            self.logger.error(_message)
            raise AirflowException(_message)

        session = requests.Session()

        self.api_endpoint = conn.host
        token_request = requests.post(
            url=urljoin(self.api_endpoint, "login"),
            data={
                "client_id": conn.login,
                "client_secret": conn.password
            },
            verify=self.verify,
        )

        token = token_request.json()["access_token"]
        headers = {"Authorization": "token " + token}
        session.headers.update(headers)

        return session

    def call(self, method, endpoint, data, headers=None):
        """
        Call the Looker API and return results
        :param endpoint: the endpoint to be called i.e. looks/run/1
        :type endpoint: str
        :param data: payload to be uploaded or request parameters
        :type data: dict
        :param headers: additional headers to be passed through as a dictionary
        :type headers: dict
        """

        session = self.get_conn()
        url = urljoin(self.api_endpoint, endpoint)

        req = None
        if method == 'GET':
            # GET uses params
            req = requests.Request(method,
                                   url,
                                   params=data,
                                   headers=headers)
        elif method == 'HEAD':
            # HEAD doesn't use params
            req = requests.Request(method,
                                   url,
                                   headers=headers)
        else:
            # Others use data
            req = requests.Request(method,
                                   url,
                                   data=json.dumps(data),
                                   headers=headers)

        prepped_request = session.prepare_request(req)
        self.log.info("Sending '%s' to url: %s: %s", method, url, data)
        response = session.send(prepped_request, verify=self.verify)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            self.log.error("HTTP error: %s", response.reason)
            self.log.error(response.text)
            raise AirflowException(str(response.status_code) + ":" + response.reason)
        return response

    def get_look_sql(self, look_id=None):
        """
        Gets a SQL query from a Looker look resource
        :param look_id: unique identifier for a look resource
        :type look_id: int
        :return: sql string
        """
        if look_id is None:
            _message = "No look_id provided"
            self.log.error(_message)
            raise AirflowException(_message)

        endpoint = '{}/{}/run/{}'.format('api/3.0/looks', look_id, 'sql')
        self.log.info("Fetching looker %s query", endpoint)
        response = self.call(method='GET', endpoint=endpoint, data=None)

        if response.status_code > 200:
            _message = 'Failed to fetch query {}'.format(response.reason)
            self.log.error(_message)
            raise AirflowException(_message)
        return response.text
