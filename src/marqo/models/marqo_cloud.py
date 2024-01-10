from typing import Optional
from enum import Enum

from pydantic import Field

from marqo.models.create_index_settings import IndexSettings
from marqo.models.marqo_models import MarqoBaseModel
from marqo.enums import IndexStatus


class CloudIndexSettings(IndexSettings):
    inferenceType: Optional[str] = Field(None, alias="inference_type")
    storageClass: Optional[str] = Field(None, alias="storage_class")
    numberOfReplicas: Optional[int] = Field(None, alias="number_of_replicas")
    numberOfShards: Optional[int] = Field(None, alias="number_of_shards")
    numberOfInferences: Optional[int] = Field(None, alias="number_of_inferences")


class ListIndexesResponse(CloudIndexSettings):
    created: Optional[str] = Field(None, alias="Created")
    indexName: Optional[str] = None
    errorMsg: Optional[str] = None
    marqoEndpoint: Optional[str] = None
    # TODO Remove this attribute after cloud is updated
    index_defaults: Optional[dict] = None
    marqoVersion: Optional[str] = None
    indexStatus: Optional[str] = None


class IndexStatusResponse(MarqoBaseModel):
    indexStatus: Optional[IndexStatus] = None


