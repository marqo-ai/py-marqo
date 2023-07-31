from typing import Optional, Dict
from marqo.marqo_url_resolver import MarqoUrlResolver


class MarqoInstanceMappings:
    def __init__(self,
                 mappings: Optional[Dict] = None,
                 url_resolver: Optional[MarqoUrlResolver] = None,
                 prioritize_resolver: bool = False):
        self._mappings = mappings
        self._url_resolver = url_resolver
        self.prioritize_resolver = prioritize_resolver

    def index_mappings(self) -> dict[str, str]:
        """
        Return a dictionary of index mappings to Marqo instance base URLs.
        """
        if self.prioritize_resolver:
            if self._url_resolver is None:
                raise ValueError("No url resolver provided but prioritize_resolver is set to True")
            self._mappings = self._url_resolver.get_mappings()
        return self._mappings

    def __getitem__(self, item):
        """
        Return a Marqo instance base URL for a given index name.
        """
        if self.prioritize_resolver:
            if self._url_resolver is None:
                raise ValueError("No url resolver provided but prioritize_resolver is set to True")
            return self._url_resolver[item]
        return self._mappings[item]
