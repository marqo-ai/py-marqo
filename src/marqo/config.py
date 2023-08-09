from typing import Optional

from marqo.instance_mappings import InstanceMappings


class Config:
    """
    Client's credentials and configuration parameters
    """

    def __init__(
            self,
            instance_mappings: Optional[InstanceMappings] = None,
            is_marqo_cloud: bool = False,
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
        self.instance_mapping = instance_mappings
        self.is_marqo_cloud = is_marqo_cloud
        self.use_telemetry = use_telemetry
        self.timeout = timeout
        self.api_key = api_key
        # suppress warnings until we figure out the dependency issues:
        # warnings.filterwarnings("ignore")
