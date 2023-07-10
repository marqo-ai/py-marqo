import time

import requests


class MarqoUrlResolver:
    def __init__(self, api_key=None, expiration_time: int = 60):
        """ URL Resolver is a cache for urls that are resolved to their respective indices only for marqo cloud. """
        self.timestamp = time.time() - expiration_time
        self._urls_mapping = {}
        self.api_key = api_key
        self.expiration_time = expiration_time

    def refresh_urls_if_needed(self, index_name):
        if index_name not in self._urls_mapping and time.time() - self.timestamp > self.expiration_time:
            self._refresh_urls()
            if self._urls_mapping:
                self.timestamp = time.time()

    @property
    def urls_mapping(self):
        return self._urls_mapping

    def _refresh_urls(self):
        response = requests.get('https://api.marqo.ai/api/indexes',
                                headers={"x-api-key": self.api_key}).json()
        self._urls_mapping = {
            index['index_name']: index['load_balancer_dns_name'] for index in response['indices']
            if index['index_status'] == "READY"
        }
