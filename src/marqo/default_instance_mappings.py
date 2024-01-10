from typing import Optional

from marqo import utils
from marqo.instance_mappings import InstanceMappings


class DefaultInstanceMappings(InstanceMappings):
    def __init__(self, url: str, main_user: str = None, main_password: str = None):
        self._url = url.lower()

        if main_user is not None and main_password is not None:
            self._url = utils.construct_authorized_url(self._url, main_user, main_password)

        local_host_markers = ["localhost", "0.0.0.0", "127.0.0.1"]
        if any([marker in self._url for marker in local_host_markers]):
            self._is_remote = False
        else:
            self._is_remote = True

    def get_index_base_url(self, index_name: str) -> str:
        return self._url

    def get_control_base_url(self, path: str = "") -> str:
        return self._url

    def is_remote(self):
        return self._is_remote

    def is_index_usage_allowed(self, index_name: str) -> bool:
        return True

    def index_http_error_handler(self, index_name: str, http_status: Optional[int] = None) -> None:
        return None

