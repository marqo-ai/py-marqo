from typing import Dict, Any, Optional, List

from marqo.models import marqo_index
from pydantic import root_validator, Field
from marqo.models.marqo_models import StrictBaseModel


class IndexSettings(StrictBaseModel):
    """
    Args:
        type: The type of the index. Can be structured or unstructured.
        allFields: A list of all fields in the index.
        tensorFields: A list of all tensor fields in the index.
        treatUrlsAndPointersAsImages: Whether to treat urls and pointers as images.
        shortStringLengthThreshold: The threshold for short string length.
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
    settingsDict: Optional[Dict] = Field(None, alias="settings_dict")
    tensorFields: Optional[List[str]] = Field(None, alias="tensor_fields")
    treatUrlsAndPointersAsImages: Optional[bool] = Field(None, alias="treat_urls_and_pointers_as_images")
    shortStringLengthThreshold: Optional[int] = Field(None, alias="short_string_length_threshold")
    model: Optional[str] = None
    modelProperties: Optional[Dict[str, Any]] = Field(None, alias="model_properties")
    normalizeEmbeddings: Optional[bool] = Field(None, alias="normalize_embeddings")
    textPreprocessing: Optional[marqo_index.TextPreProcessing] = Field(None, alias="text_preprocessing")
    imagePreprocessing: Optional[marqo_index.ImagePreProcessing] = Field(None, alias="image_preprocessing")
    vectorNumericType: Optional[marqo_index.VectorNumericType] = Field(None, alias="vector_numeric_type")
    annParameters: Optional[marqo_index.AnnParameters] = Field(None, alias="ann_parameters")

    def generate_request_body(self) -> dict:
        """A json encoded string of the request body"""
        if self.settingsDict:
            return self.settingsDict
        else:
            return self.dict(exclude_none=True, exclude={"settingsDict"})

    @root_validator(pre=True)
    def check_settings_dict_compatibility(cls, values):
        """ Ensures that settingsDict is not specified along with other parameters.

        Notes:
            if you want to add a new parameter that can be specified along with settings_dict, add it to
            SETTINGS_DICT_COMPATIBLE_PARAMS.
        """
        if values.get("settings_dict") is not None and any(arg_name for arg_name in values):
            raise ValueError(f"settings_dict cannot be specified with other index creation parameters.")
        return values