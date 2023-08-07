from abc import ABC, abstractmethod

from typing import Dict, Optional


class InstanceMappings(ABC):
    """
    Abstract class for mapping index names to URLs.

    Implementations of this class must ensure at least asymptotically constant computational complexity for all methods.
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
    def get_control_base_url(self) -> str:
        """
        Return the base URL for index control operations such as index creation and deletion.
        """
        pass

    @abstractmethod
    def is_remote(self):
        pass
