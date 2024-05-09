import base64
import os
from typing import Any, Dict, List, Optional

from pydantic import error_wrappers

from marqo.cloud_helpers import cloud_wait_for_index_status
from marqo.default_instance_mappings import DefaultInstanceMappings
from marqo.index import Index
from marqo.config import Config
from marqo.instance_mappings import InstanceMappings
from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings
from marqo.models.search_models import BulkSearchBody, BulkSearchQuery
from marqo._httprequests import HttpRequests
from marqo import utils, enums
from marqo import errors
from marqo.models import marqo_index


class Client:
    """
    A client for the marqo API

    A client instance is needed for every marqo API method to know the location of
    marqo and its permissions.
    """

    def __init__(
            self, url: Optional[str] = "http://localhost:8882",
            instance_mappings: Optional[InstanceMappings] = None,
            main_user: str = None, main_password: str = None,
            return_telemetry: bool = False,
            api_key: str = None
    ) -> None:
        """
        Parameters
        ----------
        url:
            The url to the Marqo API (ex: http://localhost:8882) If MARQO_CLOUD_URL environment variable is set, when
            matching url is passed, the client will use the Marqo Cloud instance mappings.
        instance_mappings:
            An instance of InstanceMappings that maps index names to urls
        return_telemetry:
            If True, returns telemetry object with HTTP responses. Used for measuring timing.
        api_key:
            The api key to use for authentication with the Marqo API
        """
        if url is not None and instance_mappings is not None:
            raise ValueError("Cannot specify both url and instance_mappings")

        is_marqo_cloud = False
        if url is not None:
            if url.lower().startswith(os.environ.get("MARQO_CLOUD_URL", "https://api.marqo.ai")):
                instance_mappings = MarqoCloudInstanceMappings(control_base_url=url, api_key=api_key)
                is_marqo_cloud = True
            else:
                instance_mappings = DefaultInstanceMappings(url, main_user, main_password)

        self.config = Config(
            instance_mappings=instance_mappings,
            is_marqo_cloud=is_marqo_cloud,
            use_telemetry=return_telemetry,
            api_key=api_key
        )
        self.http = HttpRequests(self.config)

    def create_index(
        self, index_name: str,
        type: Optional[marqo_index.IndexType] = None,
        settings_dict: Optional[Dict[str, Any]] = None,
        treat_urls_and_pointers_as_images: Optional[bool] = None,
        filter_string_max_length: Optional[int] = None,
        all_fields: Optional[List[marqo_index.FieldRequest]] = None,
        tensor_fields: Optional[List[str]] = None,
        model: Optional[str] = None,
        model_properties: Optional[Dict[str, Any]] = None,
        normalize_embeddings: Optional[bool] = None,
        text_preprocessing: Optional[marqo_index.TextPreProcessing] = None,
        image_preprocessing: Optional[marqo_index.ImagePreProcessing] = None,
        vector_numeric_type: Optional[marqo_index.VectorNumericType] = None,
        ann_parameters: Optional[marqo_index.AnnParameters] = None,
        wait_for_readiness: bool = True,
        inference_type: Optional[str] = None,
        storage_class: Optional[str] = None,
        number_of_shards: Optional[int] = None,
        number_of_replicas: Optional[int] = None,
        number_of_inferences: Optional[int] = None,
        text_query_prefix: Optional[str] = None,
        text_chunk_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create the index. Please refer to the marqo cloud to see options for inference and storage node types.
        Calls Index.create() with the same parameters.
        All parameters are optional, and will be set to None if not specified.
        We leave the default values to be set by Marqo.

        Args:
            index_name: name of the index.
            type: type of the index, structure or unstructured
            settings_dict: if specified, overwrites all other setting
                parameters, and is passed directly as the index's
                index_settings
            treat_urls_and_pointers_as_images: whether to treat urls and pointers as images
            filter_string_max_length: threshold for short string length in unstructured indexes,
                Marqo can filter on short strings but can not filter on long strings
            all_fields: list of all the fields in the structured index
            tensor_fields: list of fields to be tensorized
            model: name of the model to be used for the index
            model_properties: properties of the model to be used for the index
            normalize_embeddings: whether to normalize embeddings
            text_preprocessing: text preprocessing settings
            image_preprocessing: image preprocessing settings
            vector_numeric_type: vector numeric type
            ann_parameters: approximate nearest neighbors parameters
            wait_for_readiness: Marqo Cloud specific, whether to wait until
                operation is completed or to proceed without waiting for status,
                won't do anything if config.is_marqo_cloud=False
            inference_type: inference type for the index
            storage_class: storage class for the index
            number_of_inferences: number of inferences for the index
            number_of_shards: number of shards for the index
            number_of_replicas: number of replicas for the index
        Note:
            wait_for_readiness, inference_type, storage_class, number_of_inferences,
            number_of_shards, number_of_replicas are Marqo Cloud specific parameters,



        Returns:
            Response body, containing information about index creation result
        """
        return Index.create(
            config=self.config, index_name=index_name,
            type=type, settings_dict=settings_dict,
            treat_urls_and_pointers_as_images=treat_urls_and_pointers_as_images,
            filter_string_max_length=filter_string_max_length,
            all_fields=all_fields, tensor_fields=tensor_fields,
            model=model, model_properties=model_properties,
            normalize_embeddings=normalize_embeddings,
            text_preprocessing=text_preprocessing,
            image_preprocessing=image_preprocessing,
            vector_numeric_type=vector_numeric_type,
            ann_parameters=ann_parameters,
            wait_for_readiness=wait_for_readiness,
            inference_type=inference_type,
            storage_class=storage_class,
            number_of_shards=number_of_shards,
            number_of_replicas=number_of_replicas,
            number_of_inferences=number_of_inferences,
            text_query_prefix=text_query_prefix,
            text_chunk_prefix=text_chunk_prefix,
        )

    def delete_index(self, index_name: str, wait_for_readiness=True) -> Dict[str, Any]:
        """Deletes an index

        Args:
            index_name: name of the index
            wait_for_readiness: Marqo Cloud specific, whether to wait until
                operation is completed or to proceed without waiting for status,
                won't do anything if config.is_marqo_cloud=False
        Returns:
            response body about the result of the delete request
        """
        try:
            res = self.http.delete(path=f"indexes/{index_name}")
            if self.config.is_marqo_cloud and wait_for_readiness:
                cloud_wait_for_index_status(self.http, index_name, enums.IndexStatus.DELETED)
            return res
        except errors.MarqoWebError as e:
            return e.message

    def get_index(self, index_name: str) -> Index:
        """Get the index.
        This index should already exist.

        Args:
            index_name: name of the index

        Returns:
            An Index instance containing the information of the fetched index.

        Raises:
        """
        ix = Index(self.config, index_name)
        # verify it exists:
        self.http.get(path=f"indexes/{index_name}/stats", index_name=index_name)
        return ix

    def index(self, index_name: str) -> Index:
        """Create a local reference to an index identified by index_name,
        without doing an HTTP call.

        Calling this method doesn't create an index on the Marqo instance, but
         grants access to all the other methods in the Index class.

        Args:
            index_name: name of the index

        Returns:
            An Index instance.
        """
        if index_name is not None:
            return Index(self.config, index_name=index_name)
        raise Exception('The index UID should not be None')

    def get_indexes(self) -> Dict[str, List[Dict[str, str]]]:
        """Get all indexes.

        Returns:
        Indexes, a dictionary with the name of indexes.
        """
        response = self.http.get(path='indexes')
        return {
            "results": [
                {"indexName": index_info["indexName"]} for index_info in response["results"]
            ]
        }

    def bulk_search(self, queries: List[Dict[str, Any]], device: Optional[str] = None) -> Dict[str, Any]:
        try:
            parsed_queries = [BulkSearchBody(**q) for q in queries]
        except error_wrappers.ValidationError as e:
            raise errors.InvalidArgError(f"some parameters in search query(s) are invalid. Errors are: {e.errors()}")

        self._validate_all_indexes_belong_to_the_same_cluster(parsed_queries)

        translated_device_param = f"{f'?&device={utils.translate_device_string_for_url(device)}' if device is not None else ''}"
        return self.http.post(
            f"indexes/bulk/search{translated_device_param}",
            body=BulkSearchQuery(queries=parsed_queries).json(),
            index_name=parsed_queries[0].index
        )

    @staticmethod
    def _base64url_encode(
            data: bytes
    ) -> str:
        return base64.urlsafe_b64encode(data).decode('utf-8').replace('=', '')

    @staticmethod
    def raise_error_for_cloud(function_name: str = None):
        raise errors.BadRequestError(
            f"The `mq.{function_name}()` API is not supported on Marqo Cloud. "
            f"Please Use `mq.index('your-index-name').{function_name}()` instead. "
            "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details.")

    def _validate_all_indexes_belong_to_the_same_cluster(self, parsed_queries: List[BulkSearchBody]):
        """
        Validates that all indices in the bulk request belong to the same cluster.

        This method checks whether all the specified indices in a bulk search request
        are associated with the same cluster. It ensures that the indexes are not spread
        across multiple clusters, as a bulk search operation should be performed within
        a single cluster to guarantee consistency and reliability.

        Args:
            parsed_queries (List[BulkSearchBody]): A list of parsed bulk search queries.

        Raises:
            errors.InvalidArgError: If the indices belong to different clusters.

        Returns:
            bool: True if all indices belong to the same cluster, False otherwise.
        """
        cluster = None
        index_names = set([q.index for q in parsed_queries])
        for index_name in index_names:
            self.index(index_name)  # it will perform all basic checks for index readiness
            if cluster is None:
                cluster = self.config.instance_mapping.get_index_base_url(index_name)
            if cluster != self.config.instance_mapping.get_index_base_url(index_name):
                raise errors.InvalidArgError(
                    "All indexes in a bulk search request must belong to the same Marqo cluster.\n"
                    "- If you are using Marqo Cloud, make sure all search requests"
                    " in your bulk search use the same index"
                )
        return True