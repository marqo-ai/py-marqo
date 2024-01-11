from marqo1.marqo_logging import mq_logger
from pydantic import root_validator, BaseModel, validator

DEPRECATED_TO_NEW_PARAMS_MAPPING = {
    "inference_node_type": "inference_type",
    "storage_node_type": "storage_class",
    "inference_node_count": "number_of_inferences",
    "storage_node_count": "number_of_shards",
    "replicas_count": "number_of_replicas",
}
SETTINGS_DICT_COMPATIBLE_PARAMS = ["wait_for_readiness", "settings_dict"]


class CreateIndexSettings(BaseModel):
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
            inference_node_type (str, deprecated): Inference type for the index. Deprecated, use inference_type instead.
            storage_node_type (str, deprecated): Storage type for the index. Deprecated, use storage_class instead.
            inference_node_count (int, deprecated): Number of inference nodes for the index. Deprecated,
            use number_of_inferences instead.
            storage_node_count (int, deprecated): Number of storage nodes for the index. Deprecated,
            use number_of_shards instead.
            replicas_count (int, deprecated): Number of replicas for the index. Deprecated,
            use number_of_replicas instead.
            wait_for_readiness (bool): Marqo Cloud specific, whether to wait until the operation is completed or to
            proceed without waiting for status; won't do anything if config.is_marqo_cloud=False.
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
    treat_urls_and_pointers_as_images: bool = False
    model: str = None
    normalize_embeddings: bool = True
    sentences_per_chunk: int = 2
    sentence_overlap: int = 0
    image_preprocessing_method: str = None
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

    def __init__(self, **passed_parameters):
        passed_parameters_without_none_values = {
            key: value for key, value in passed_parameters.items() if value is not None
        }
        super().__init__(**passed_parameters_without_none_values)

    @root_validator(pre=True)
    def handle_deprecated_parameters(cls, values):
        """ Handles deprecated parameters and raises deprecation warnings.
        Ensures that deprecated parameters are not specified along with their new counterparts.
        If both deprecated and new parameters are specified, raises a ValueError.
        If only deprecated parameters are specified, sets new parameters to the values of deprecated parameters.
        DEPRECATED_TO_NEW_PARAMS_MAPPING is a dictionary that maps deprecated parameters to
        their new counterparts.

        Notes:
            if you want to add a new deprecated parameter, add it to DEPRECATED_TO_NEW_PARAMS_MAPPING.
        """
        deprecated_parameters_passed = [
            deprecated_parameter for deprecated_parameter in DEPRECATED_TO_NEW_PARAMS_MAPPING
            if deprecated_parameter in values
        ]
        if deprecated_parameters_passed:
            cls.raise_deprecated_warning(deprecated_parameters_passed)
        for deprecated_parameter in deprecated_parameters_passed:
            if DEPRECATED_TO_NEW_PARAMS_MAPPING[deprecated_parameter] in values:
                raise ValueError(
                    f"Both {DEPRECATED_TO_NEW_PARAMS_MAPPING[deprecated_parameter]} and "
                    f"it's deprecated reference {deprecated_parameter} "
                    f"are specified. Please use only "
                    f"{DEPRECATED_TO_NEW_PARAMS_MAPPING[deprecated_parameter]}."
                )
            values[DEPRECATED_TO_NEW_PARAMS_MAPPING[deprecated_parameter]] = values.pop(
                deprecated_parameter
            )
        return values

    @root_validator(pre=True)
    def check_settings_dict_compatibility(cls, values):
        """ Ensures that settings_dict is not specified along with other parameters.
        SETTINGS_DICT_COMPATIBLE_PARAMS is a list of parameters that can be specified along with settings_dict.
        If any other parameter is specified along with settings_dict, raises a ValueError
        with a message that settings_dict cannot be specified with other parameters.

        Notes:
            if you want to add a new parameter that can be specified along with settings_dict, add it to
            SETTINGS_DICT_COMPATIBLE_PARAMS.
        """
        if values.get("settings_dict") is not None and any(
                [arg_name for arg_name in values if arg_name not in SETTINGS_DICT_COMPATIBLE_PARAMS]
        ):
            raise ValueError(f"settings_dict cannot be specified with other index creation parameters, "
                             f"besides [{', '.join(SETTINGS_DICT_COMPATIBLE_PARAMS)}]")
        return values

    @staticmethod
    def raise_deprecated_warning(deprecated_parameters_passed):
        warning_msg = \
            f"The parameter(s) {', '.join(deprecated_parameters_passed)} are deprecated. " \
            f"Please refer to the documentation " \
            f"https://docs.marqo.ai/1.3.0/Using-Marqo-Cloud/indexes/#create-index " \
            f"for updated parameters names. These parameters will be removed in Marqo 2.0.0."
        mq_logger.warning(warning_msg)
