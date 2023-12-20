from typing import Optional

from pydantic import Field

from marqo.models.create_index_settings import IndexSettings


class CloudIndexSettings(IndexSettings):
    inferenceType: Optional[str] = Field(None, alias="inference_type")
    storageClass: Optional[str] = Field(None, alias="storage_class")
    numberOfReplicas: Optional[int] = Field(None, alias="number_of_replicas")
    numberOfShards: Optional[int] = Field(None, alias="number_of_shards")
    numberOfInferences: Optional[int] = Field(None, alias="number_of_inferences")