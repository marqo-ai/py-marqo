from abc import ABC, abstractmethod

from typing import Dict, Optional


class InstanceMappings(ABC):
    """
    Abstract class for mapping index names to URLs.

    Implementations of this class must ensure asymptotic average-case time complexity of O(1) for all methods, i.e.
    for a large number of calls per second, the time taken to process each call should be constant on average.
    An inefficient implementation of this class can cause the Marqo client to be slow.

    The namespace of index names is with respect to a Client instance.
    Index names must be unique across all Marqo URLs referenced in the Mappings.
    If you want to support the same index name on different Marqo URLs, please use separate Client objects.
    """
    @abstractmethod
    def get_index_base_url(self, index_name: str) -> str:
        """
        Return the base URL for the given index.

        Args:
            index_name: The index name
        """
        pass

    @abstractmethod
    def get_control_base_url(self, path: str = "") -> str:
        """
        Return the base URL for index control operations such as index creation and deletion.
        """
        pass

    @abstractmethod
    def is_remote(self):
        pass

    @abstractmethod
    def is_index_usage_allowed(self, index_name: str) -> bool:
        """
        Return whether the given index is allowed to be used.

        Currently, it is just used during the version check. If False
        is returned, the version check will not be attempted.
        """
        pass

    @abstractmethod
    def index_http_error_handler(self, index_name: str, http_status: Optional[int] = None) -> None:
        """
        Called when an HTTP error occurs on a Marqo index operation.

        Args:
            index_name: The index name
            http_status: The HTTP status code
        """
        pass
