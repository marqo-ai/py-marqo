import time
from typing import Optional

import requests
from requests.exceptions import Timeout

from marqo.errors import (
    MarqoCloudIndexNotFoundError,
    MarqoCloudIndexNotReadyError,
)
from marqo.instance_mappings import InstanceMappings
from marqo.marqo_logging import mq_logger


class MarqoCloudInstanceMappings(InstanceMappings):
    def __init__(self, control_base_url, api_key=None, url_cache_duration: int = 15):
        self.latest_index_mappings_refresh_timestamp = time.time() - url_cache_duration
        self._urls_mapping = {"READY": {}, "CREATING": {}}
        self.api_key = api_key
        self.url_cache_duration = url_cache_duration
        self._control_base_url = control_base_url

    def get_control_base_url(self) -> str:
        return f"{self._control_base_url}/api"

    def get_index_base_url(self, index_name: str) -> str:
        self._refresh_urls_if_needed(index_name)
        if index_name in self._urls_mapping['READY']:
            return self._urls_mapping['READY'][index_name]
        if index_name in self._urls_mapping['CREATING']:
            raise MarqoCloudIndexNotReadyError(index_name)
        raise MarqoCloudIndexNotFoundError(index_name)

    def is_remote(self):
        return True

    def _refresh_urls_if_needed(self, index_name):
        if index_name not in self._urls_mapping['READY'] and \
                time.time() - self.latest_index_mappings_refresh_timestamp > self.url_cache_duration:
            # fast refresh to catch if index was created
            self._refresh_urls()
        if index_name in self._urls_mapping['READY'] and \
                time.time() - self.latest_index_mappings_refresh_timestamp > 360:
            # slow refresh in case index was deleted
            self._refresh_urls(timeout=3)

    def _refresh_urls(self, timeout=None):
        try:
            response = requests.get(f'{self.get_control_base_url()}/indexes',
                                    headers={"x-api-key": self.api_key}, timeout=timeout)
        except Timeout:
            mq_logger.warning(
                f"Timeout getting and caching URLs for Marqo Cloud indexes from the"
                f" /api/indexes/ endpoint. Please contact marqo support at support@marqo.ai if this message"
                f" persists."
            )
            return None

        if not response.ok:
            mq_logger.warning(response.text)
        response_json = response.json()
        for index in response_json['results']:
            if index.get('index_status') in ["READY", "CREATING"]:
                self._urls_mapping[index['index_status']][index['index_name']] = index.get('endpoint')
        if self._urls_mapping:
            self.latest_index_mappings_refresh_timestamp = time.time()
