from abc import ABC, abstractmethod

from typing import Dict, Optional


class InstanceMappings(ABC):
    @abstractmethod
    def get_url(self, index_name: str) -> str:
        """
        Return the base URL for the given index.

        Args:
            index_name: The index name
        """
        pass

    @abstractmethod
    def get_control_url(self) -> str:
        """
        Return the base URL for index control operations such as index creation and deletion.
        """
        pass

    @abstractmethod
    def is_remote(self):
        pass
