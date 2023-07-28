import time
from marqo.marqo_logging import mq_logger
import requests
from requests.exceptions import Timeout
from marqo.errors import (
    MarqoCloudIndexNotFoundError,
    MarqoCloudIndexNotReadyError,
)


class MarqoUrlResolver:
    def __init__(self, api_key=None, expiration_time: int = 15):
        """ URL Resolver is a cache for urls that are resolved to their respective indices only for marqo cloud. """
        self.timestamp = time.time() - expiration_time
        self._urls_mapping = {"READY": {}, "CREATING": {}}
        self.api_key = api_key
        self.expiration_time = expiration_time

    def refresh_urls_if_needed(self, index_name):
        if index_name not in self._urls_mapping['READY'] and time.time() - self.timestamp > self.expiration_time:
            # fast refresh to catch if index was created
            self._refresh_urls()
        if index_name in self._urls_mapping['READY'] and time.time() - self.timestamp > 360:
            # slow refresh in case index was deleted
            self._refresh_urls(timeout=3)

    def __getitem__(self, item):
        self.refresh_urls_if_needed(item)
        if item in self._urls_mapping['READY']:
            return self._urls_mapping['READY'][item]
        if item in self._urls_mapping['CREATING']:
            raise MarqoCloudIndexNotReadyError(item)
        raise MarqoCloudIndexNotFoundError(item)

    def _refresh_urls(self, timeout=None):
        try:
            response = requests.get('https://api.marqo.ai/api/indexes',
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
            self.timestamp = time.time()
