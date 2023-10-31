from typing import Dict, Any, Optional, List
from enum import Enum
import json

import marqo.models.marqo_index as core
from marqo.models.strict_base_model import StrictBaseModel


class EnumEncoder(json.JSONEncoder):
    """A help class to encode enums as strings in json.dumps()"""
    def default(self, o: Any) -> Any:
        if isinstance(o, Enum):
            return o.value
        return super().default(o)


class Field(StrictBaseModel):
    name: str
    type: core.FieldType
    features: List[core.FieldFeature] = []
    dependent_fields: Optional[Dict[str, float]]


class AnnParameters(StrictBaseModel):
    space_type: core.DistanceMetric
    parameters: core.HnswConfig


class IndexSettings(StrictBaseModel):
    type: Optional[core.IndexType]
    all_fields: Optional[List[Field]]
    tensor_fields: Optional[List[str]]
    model: Optional[str]
    model_properties: Optional[Dict[str, Any]]
    normalize_embeddings: Optional[bool]
    text_preprocessing: Optional[core.TextPreProcessing]
    image_preprocessing: Optional[core.ImagePreProcessing]
    vector_numeric_type: Optional[core.VectorNumericType]
    ann_parameters: Optional[AnnParameters]

    @property
    def request_body(self):
        return self.dict(exclude_none=True)


class CreateIndexSettings(StrictBaseModel):
    """A wrapper to create an index and a request body with explicit settings parameters
    or with a settings dict"""
    index_settings: IndexSettings
    settings_dict: Optional[Dict]

    @property
    def request_body(self) -> str:
        """A json encoded string of the request body"""
        request_body = self.settings_dict if self.settings_dict is not None \
            else self.index_settings.request_body
        return json.dumps(request_body, cls=EnumEncoder)
