from typing import Optional, Union
from marqo import enums, utils
import urllib3
import warnings

from marqo.instance_mapping import MarqoInstanceMappings
from marqo.marqo_url_resolver import MarqoUrlResolver


class Config:
    """
    Client's credentials and configuration parameters
    """

    def __init__(
        self,
        url: str,
        instance_mappings: Optional[MarqoInstanceMappings] = None,
        use_telemetry: bool = False,
        timeout: Optional[int] = None,
        api_key: str = None
    ) -> None:
        """
        Parameters
        ----------
        url:
            The url to the Marqo instance (ex: http://localhost:8882)
        """
        self.cluster_is_remote = False
        self.cluster_is_s2search = False
        self.cluster_is_marqo = False
        self.marqo_url_resolver = None
        self.api_key = api_key
        self.url = self.set_url(url)
        self.instance_mapping = instance_mappings
        if self.cluster_is_marqo:
            self.marqo_url_resolver = MarqoUrlResolver(api_key=self.api_key, expiration_time=15)
            self.instance_mapping = MarqoInstanceMappings(
                url_resolver=self.marqo_url_resolver, prioritize_resolver=True
            )

        self.timeout = timeout
        # suppress warnings until we figure out the dependency issues:
        # warnings.filterwarnings("ignore")
        self.use_telemetry = use_telemetry

    def set_url(self, url):
        """Set the URL, and infers whether that url is remote"""
        lowered_url = url.lower()
        local_host_markers = ["localhost", "0.0.0.0", "127.0.0.1"]
        if any([marker in lowered_url for marker in local_host_markers]):
            # urllib3.disable_warnings()
            self.cluster_is_remote = False
        else:
            # warnings.resetwarnings()
            self.cluster_is_remote = True
            if "s2search.io" in lowered_url:
                self.cluster_is_s2search = True
            if "api.marqo.ai" in lowered_url:
                self.cluster_is_marqo = True
                url += "/api"
        self.url = url
        return self.url

    def get_url(self, index_name: str = None):
        """Get the URL, and infers whether that url requires index_mapping resolution
        and if it is targeting a specific index resolves the index-specific url"""
        if index_name and self.instance_mapping is not None:
            return self.instance_mapping[index_name]
        return self.url
