import time
from _hooks.looker_hook import LookerHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LookerOperator(BaseOperator):
    ui_color = '#615286'

    @apply_defaults
    def __init__(self, looker_conn_id='looker_default', *args, **kwargs):
        super(LookerOperator, self).__init__(*args, **kwargs)
        self.looker_conn_id = looker_conn_id

    def _get_hook(self):
        return LookerHook(looker_conn_id=self.looker_conn_id)


class LookerUpdateDataGroupByIDOperator(LookerOperator):
    """
    Update a datagroup using the specified params.

    :param datagroup_id: The Datagroup ID to update. Required.
    :type datagroup_id: int
    :param looker_conn_id: reference to a specific Looker connection.
    :type looker_conn_id: string
    :param stale_before: The datagroup is stale if refreshed before this time. Defaults to now.
    :type stale_before: time
    """
    @apply_defaults
    def __init__(self, looker_conn_id='looker_default', datagroup_id=None, stale_before=time.time(), *args, **kwargs):
        super(LookerUpdateDataGroupByIDOperator, self).__init__(looker_conn_id=looker_conn_id, *args, **kwargs)
        self.datagroup_id = datagroup_id
        self.stale_before = stale_before

    def execute(self, context):
        # get hook and call
        looker = self._get_hook()

        endpoint = '{}/{}'.format('api/3.0/datagroups', self.datagroup_id)
        body = {
            'stale_before': self.stale_before
        }

        self.log.info("Calling: %s with body: %s", endpoint, body)
        looker.call(method='PATCH', endpoint=endpoint, data=body)
