from abc import ABC, abstractmethod

from typing import Dict, Optional


class InstanceMappings(ABC):
    """
    Abstract class for mapping index names to URLs.

    Implementations of this class must ensure at least asymptotically constant computational complexity for all methods.
    An inefficient implementation of this class can cause the Marqo client to be slow.
    Index names must be unique across all instances.
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
