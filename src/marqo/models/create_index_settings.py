from typing import Dict, Any, Optional, List

from marqo.models import marqo_index
from pydantic import root_validator, Field
from marqo.models.strict_base_model import StrictBaseModel


class AnnParameters(StrictBaseModel):
    spaceType: marqo_index.DistanceMetric
    parameters: marqo_index.HnswConfig


class IndexSettings(StrictBaseModel):
    """
    Args:
        type: The type of the index. Can be structured or unstructured.
        allFields: A list of all fields in the index.
        tensorFields: A list of all tensor fields in the index.
        model: The name of the model to use for the index.
        modelProperties: A dictionary of model properties.
        normalizeEmbeddings: Whether to normalize embeddings.
        textPreprocessing: The text preprocessing method to use.
        imagePreprocessing: The image preprocessing method to use.
        vectorNumeric_type: The numeric type of the vector.
        annParameters: The ANN parameters to use.

    Please note, we don't note set default values in the py-marqo side. All the
    values are set to be None and will not be sent to Marqo in the HttpRequest.
    Refer to Marqo for the default values.
    """

    type: Optional[marqo_index.IndexType]
    allFields: Optional[List[marqo_index.FieldRequest]]
    tensorFields: Optional[List[str]]
    treatUrlsAndPointersAsImages: Optional[bool]
    model: Optional[str]
    modelProperties: Optional[Dict[str, Any]]
    normalizeEmbeddings: Optional[bool]
    textPreprocessing: Optional[marqo_index.TextPreProcessing]
    imagePreprocessing: Optional[marqo_index.ImagePreProcessing]
    vectorNumericType: Optional[marqo_index.VectorNumericType]
    annParameters: Optional[AnnParameters]

    @property
    def request_body(self) -> dict:
        return self.dict(exclude_none=True)


class CreateIndexSettings(StrictBaseModel):
    """A wrapper to create an index and a request body with explicit settings parameters
    or with a settings dict"""
    indexSettings: IndexSettings
    settingsDict: Optional[Dict]

    @property
    def request_body(self) -> dict:
        """A json encoded string of the request body"""
        return self.settingsDict if self.settingsDict is not None \
            else self.indexSettings.request_body

    @root_validator(pre=True)
    def check_settings_dict_compatibility(cls, values):
        if values.get("settings_dict") is not None and \
                values.get("index_settings").dict(exclude_none=True) != {}:
            raise ValueError("settings_dict and index_settings cannot be used together")
        return values
