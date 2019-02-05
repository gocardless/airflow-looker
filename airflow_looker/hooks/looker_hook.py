import requests
import json

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
        session = requests.Session()

        self.api_endpoint = conn.host

        token_request = requests.post(
            url=self.api_endpoint + "login",
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
        url = self.api_endpoint + endpoint

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
