from typing import Dict, Any, Optional, List

from marqo.models import marqo_index
from pydantic import root_validator, Field
from marqo.models.marqo_models import StrictBaseModel


class AnnParameters(StrictBaseModel):
    spaceType: Optional[marqo_index.DistanceMetric] = Field(None, alias="space_type")
    parameters: Optional[marqo_index.HnswConfig] = None


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
        vectorNumericType: The numeric type of the vector.
        annParameters: The ANN parameters to use.

    Please note, we don't note set default values in the py-marqo side. All the
    values are set to be None and will not be sent to Marqo in the HttpRequest.
    Refer to Marqo for the default values.
    """

    type: Optional[marqo_index.IndexType] = Field(None, alias="type")
    allFields: Optional[List[marqo_index.FieldRequest]] = Field(None, alias="all_fields")
    tensorFields: Optional[List[str]] = Field(None, alias="tensor_fields")
    treatUrlsAndPointersAsImages: Optional[bool] = Field(None, alias="treat_urls_and_pointers_as_images")
    model: Optional[str] = None
    modelProperties: Optional[Dict[str, Any]] = Field(None, alias="model_properties")
    normalizeEmbeddings: Optional[bool] = Field(None, alias="normalize_embeddings")
    textPreprocessing: Optional[marqo_index.TextPreProcessing] = Field(None, alias="text_preprocessing")
    imagePreprocessing: Optional[marqo_index.ImagePreProcessing] = Field(None, alias="image_preprocessing")
    vectorNumericType: Optional[marqo_index.VectorNumericType] = Field(None, alias="vector_numeric_type")
    annParameters: Optional[AnnParameters] = Field(None, alias="ann_parameters")

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
