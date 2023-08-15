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
from marqo.enums import IndexStatus


class MarqoCloudInstanceMappings(InstanceMappings):

    def __init__(self, control_base_url, api_key=None, url_cache_duration: int = 15):
        self.latest_index_mappings_refresh_timestamp = time.time() - url_cache_duration
        self._urls_mapping = {IndexStatus.READY: {}, IndexStatus.CREATING: {}}
        self.api_key = api_key
        self.url_cache_duration = url_cache_duration
        self._control_base_url = control_base_url

    def get_control_base_url(self) -> str:
        return f"{self._control_base_url}/api"

    def get_index_base_url(self, index_name: str) -> str:
        """Returns the index_name's base URL regardless of its status.

        Raises:
            MarqoCloudIndexNotFoundError: if index_name is not found in any status.
        """
        self._refresh_urls_if_needed(index_name)

        for cloud_status, indexes in self._urls_mapping.items():
            if index_name in indexes:
                return indexes[index_name]

        raise MarqoCloudIndexNotFoundError(index_name)

    def is_remote(self):
        return True

    def _refresh_urls_if_needed(self, index_name: Optional[str] = None):
        if index_name is None or index_name not in self._urls_mapping[IndexStatus.READY]:
            if time.time() - self.latest_index_mappings_refresh_timestamp > self.url_cache_duration:
                self._refresh_urls(timeout=15)

    def _refresh_urls(self, timeout=None):
        mq_logger.debug("Refreshing Marqo Cloud index URL cache")
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
            return None
        response_json = response.json()
        self._urls_mapping = {IndexStatus.READY: {}, IndexStatus.CREATING: {}}
        for index in response_json['results']:
            if index.get('index_status') in [IndexStatus.READY, IndexStatus.MODIFYING]:
                self._urls_mapping[IndexStatus.READY][index['index_name']] = index.get('endpoint')
            elif index.get('index_status') == IndexStatus.CREATING:
                self._urls_mapping[IndexStatus.CREATING][index['index_name']] = index.get('endpoint')
        if self._urls_mapping:
            self.latest_index_mappings_refresh_timestamp = time.time()

    def index_http_error_handler(self, index_name: str, http_status: Optional[int] = None) -> None:
        mq_logger.debug(f'Triggering cache refresh due to error on index {index_name}')

        self._refresh_urls_if_needed()

    def is_index_usage_allowed(self, index_name: str) -> bool:
        """Checks the status of the index in self._urls_mapping.

        Note that this method does not request a refresh of the mappings, so the result
        of this method is a best effort.

        If the index_name cannot be found in the self._urls_mapping, then False will be returned.
        """

        if index_name in self._urls_mapping[IndexStatus.READY]:
            return True
        else:
            return False