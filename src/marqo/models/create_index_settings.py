from typing import Dict, Any, Optional, List

from marqo.models import marqo_index
from pydantic import root_validator
from marqo.models.strict_base_model import StrictBaseModel


class AnnParameters(StrictBaseModel):
    space_type: marqo_index.DistanceMetric
    parameters: marqo_index.HnswConfig


class IndexSettings(StrictBaseModel):
    """
    Args:
        type: The type of the index. Can be structured or unstructured.
        all_fields: A list of all fields in the index.
        tensor_fields: A list of all tensor fields in the index.
        model: The name of the model to use for the index.
        model_properties: A dictionary of model properties.
        normalize_embeddings: Whether to normalize embeddings.
        text_preprocessing: The text preprocessing method to use.
        image_preprocessing: The image preprocessing method to use.
        vector_numeric_type: The numeric type of the vector.
        ann_parameters: The ANN parameters to use.

    Please note, we don't note set default values in the py-marqo side. All the
    values are set to be None and will not be sent to Marqo in the HttpRequest.
    Refer to Marqo for the default values.
    """

    type: Optional[marqo_index.IndexType]
    all_fields: Optional[List[marqo_index.FieldRequest]]
    tensor_fields: Optional[List[str]]
    treat_urls_and_pointers_as_images: Optional[bool]
    model: Optional[str]
    model_properties: Optional[Dict[str, Any]]
    normalize_embeddings: Optional[bool]
    text_preprocessing: Optional[marqo_index.TextPreProcessing]
    image_preprocessing: Optional[marqo_index.ImagePreProcessing]
    vector_numeric_type: Optional[marqo_index.VectorNumericType]
    ann_parameters: Optional[AnnParameters]

    @property
    def request_body(self) -> dict:
        return self.dict(exclude_none=True)


class CreateIndexSettings(StrictBaseModel):
    """A wrapper to create an index and a request body with explicit settings parameters
    or with a settings dict"""
    index_settings: IndexSettings
    settings_dict: Optional[Dict]

    @property
    def request_body(self) -> dict:
        """A json encoded string of the request body"""
        return self.settings_dict if self.settings_dict is not None \
            else self.index_settings.request_body

    @root_validator(pre=True)
    def check_settings_dict_compatibility(cls, values):
        if values.get("settings_dict") is not None and \
                values.get("index_settings").dict(exclude_none=True) != {}:
            raise ValueError("settings_dict and index_settings cannot be used together")
        return values
