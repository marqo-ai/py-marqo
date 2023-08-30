from typing import Dict, List, Optional, Union
from pydantic import BaseModel

from marqo.marqo_logging import mq_logger


class BaseMarqoModel(BaseModel):
    class Config:
        extra: str = "forbid"
    pass


class SearchBody(BaseMarqoModel):
    q: Union[str, Dict[str, float]]
    searchableAttributes: Union[None, List[str]] = None
    searchMethod: Union[None, str] = "TENSOR"
    limit: int = 10
    offset: int = 0
    showHighlights: bool = True
    reRanker: str = None
    filter: str = None
    attributesToRetrieve: Union[None, List[str]] = None
    boost: Optional[Dict] = None
    image_download_headers: Optional[Dict] = None
    context: Optional[Dict] = None
    scoreModifiers: Optional[Dict] = None
    modelAuth: Optional[Dict] = None


class BulkSearchBody(SearchBody):
    index: str


class BulkSearchQuery(BaseMarqoModel):
    queries: List[BulkSearchBody]


index_defaults_deprecated_to_new_params_mapping = {
    "inference_node_type": "inference_type",
    "storage_node_type": "storage_class",
    "inference_node_count": "number_of_inferences",
    "storage_node_count": "number_of_shards",
    "replicas_count": "number_of_replicas",
}


class CreateIndexSettings:
    """
    Default configuration settings for the Create Index function.

    Args:
        treat_urls_and_pointers_as_images (bool): Whether to treat URLs and pointers as images.
        model (str): Model to be used.
        normalize_embeddings (bool): Whether to normalize embeddings.
        sentences_per_chunk (int): Number of sentences per chunk.
        sentence_overlap (int): Sentence overlap.
        image_preprocessing_method (str): Image preprocessing method.
        settings_dict (dict): If specified, overwrites all other setting parameters, and is
        passed directly as the index's index_settings.
        inference_node_type (str, deprecated): Inference type for the index.
        storage_node_type (str, deprecated): Storage type for the index.
        inference_node_count (int, deprecated): Number of inference nodes for the index.
        storage_node_count (int, deprecated): Number of storage nodes for the index.
        replicas_count (int, deprecated): Number of replicas for the index.
        wait_for_readiness (bool): Marqo Cloud specific, whether to wait until the operation is completed or to proceed
        without waiting for status; won't do anything if config.is_marqo_cloud=False.
        inference_type (str): Inference type for the index.
        storage_class (str): Storage class for the index.
        number_of_inferences (int): Number of inferences for the index.
        number_of_shards (int): Number of shards for the index.
        number_of_replicas (int): Number of replicas for the index.

    This class is used to manage default configuration settings for the Create Index function.
    When creating an instance of this class, you can specify values for the configuration settings.
    If a setting is not explicitly provided, its default value will be used.

    Additionally, this class performs checks and raises deprecation warnings for deprecated parameters.
    It also ensures that certain parameters, like `settings_dict`, are not specified along with other parameters.

    Example:
        >>> settings = CreateIndexSettings(model="my_model", normalize_embeddings=False)
        >>> settings.model  # Accessing the model parameter
        'my_model'
        >>> settings.normalize_embeddings
        False
    """

    treat_urls_and_pointers_as_images = False
    model = None
    normalize_embeddings = True
    sentences_per_chunk = 2
    sentence_overlap = 0
    image_preprocessing_method = None
    settings_dict: dict = None
    inference_node_type: str = None
    storage_node_type: str = None
    inference_node_count: int = 1
    storage_node_count: int = 1
    replicas_count: int = 0
    wait_for_readiness: bool = True
    inference_type: str = None
    storage_class: str = None
    number_of_inferences: int = 1
    number_of_shards: int = 1
    number_of_replicas: int = 0

    def __init__(self, treat_urls_and_pointers_as_images, model, normalize_embeddings,
                 sentences_per_chunk, sentence_overlap, image_preprocessing_method, settings_dict,
                 inference_node_type, storage_node_type, inference_node_count, storage_node_count,
                 replicas_count, wait_for_readiness, inference_type, storage_class, number_of_inferences,
                 number_of_shards, number_of_replicas):
        self.specified_values = []
        for arg_name, arg_value in locals().items():
            if arg_name != 'self':
                self._set_value(arg_value, arg_name)
        if any([arg_name in index_defaults_deprecated_to_new_params_mapping.keys() for arg_name in self.specified_values]):
            self._raise_deprecated_warning()
            for deprecated_arg in index_defaults_deprecated_to_new_params_mapping.keys():
                if deprecated_arg in self.specified_values:
                    setattr(
                        self, index_defaults_deprecated_to_new_params_mapping[deprecated_arg],
                        self._use_deprecated_if_new_not_given(deprecated_arg)
                    )

    def _set_value(self, value, parameter_name):
        if value is not None:
            setattr(self, parameter_name, value)
            self.specified_values.append(parameter_name)
            if "settings_dict" in self.specified_values and len(self.specified_values) > 1:
                raise ValueError("settings_dict cannot be specified with other parameters.")

    def _raise_deprecated_warning(self):
        used_deprecated_params = [
            arg for arg in index_defaults_deprecated_to_new_params_mapping.keys() if arg in self.specified_values
        ]
        if used_deprecated_params:
            used_deprecated_params_string = ", ".join(used_deprecated_params)
            warning_msg = \
                f"The parameter(s) {used_deprecated_params_string} are deprecated. " \
                f"Please refer to the documentation " \
                f"https://docs.marqo.ai/1.3.0/Using-Marqo-Cloud/indexes/#create-index " \
                f"for updated parameters names. These parameters will be removed in Marqo 2.0.0."
            mq_logger.warn(warning_msg)

    def _use_deprecated_if_new_not_given(self, deprecated_parameter_name):
        if deprecated_parameter_name in self.specified_values and \
                index_defaults_deprecated_to_new_params_mapping[deprecated_parameter_name] in self.specified_values:
            raise ValueError(
                f"Both {deprecated_parameter_name} and "
                f"{index_defaults_deprecated_to_new_params_mapping[deprecated_parameter_name]} "
                f"are specified. Please consider using only one of them as they both refer to the same value."
            )
        if deprecated_parameter_name in self.specified_values:
            return getattr(self, deprecated_parameter_name)
