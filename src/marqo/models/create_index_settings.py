from typing import Dict, Any, Optional, List

from marqo.models import marqo_index
from pydantic import root_validator, Field
from marqo.models.marqo_models import MarqoBaseModel


class IndexSettings(MarqoBaseModel):
    """
    Args:
        type: The type of the index. Can be structured or unstructured.
        allFields: A list of all fields in the index.
        settingsDict: A dictionary of all the settings of the index. All fields should be in camel case.
            Can not be specified with other parameters.
        tensorFields: A list of all tensor fields in the index.
        treatUrlsAndPointersAsImages: Whether to treat urls and pointers as images.
            This unstructured index only parameter.
        filterStringMaxLength: The max length of the filter string in unstructured index
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

    type: Optional[marqo_index.IndexType] = None
    allFields: Optional[List[marqo_index.FieldRequest]] = None
    settingsDict: Optional[Dict] = None
    tensorFields: Optional[List[str]] = None
    treatUrlsAndPointersAsImages: Optional[bool] = None
    filterStringMaxLength: Optional[int] = None
    model: Optional[str] = None
    modelProperties: Optional[Dict[str, Any]] = None
    normalizeEmbeddings: Optional[bool] = None
    textPreprocessing: Optional[marqo_index.TextPreProcessing] = None
    imagePreprocessing: Optional[marqo_index.ImagePreProcessing] = None
    vectorNumericType: Optional[marqo_index.VectorNumericType] = None
    annParameters: Optional[marqo_index.AnnParameters] = None
    textQueryPrefix: Optional[str] = None
    textChunkPrefix: Optional[str] = None

    def generate_request_body(self) -> dict:
        """A json encoded string of the request body"""
        # We return settingsDict if it is not None, otherwise we return the dict
        if self.settingsDict is not None:
            return self.settingsDict
        else:
            return self.dict(exclude_none=True, exclude={"settingsDict"})

    @root_validator(pre=True)
    def check_settings_dict_compatibility(cls, values):
        """ Ensures that settingsDict is not specified along with other parameters."""
        if values.get("settings_dict") is not None and any(arg_name for arg_name in values):
            raise ValueError(f"settings_dict cannot be specified with other index creation parameters.")
        return values