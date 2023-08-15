import base64
import os
from typing import Any, Dict, List, Optional, Union

from pydantic import error_wrappers
from requests.exceptions import RequestException
from typing_extensions import deprecated

from marqo.cloud_helpers import cloud_wait_for_index_status
from marqo.default_instance_mappings import DefaultInstanceMappings
from marqo.index import Index
from marqo.config import Config
from marqo.instance_mappings import InstanceMappings
from marqo.marqo_cloud_instance_mappings import MarqoCloudInstanceMappings
from marqo.models import BulkSearchBody, BulkSearchQuery
from marqo._httprequests import HttpRequests
from marqo import utils, enums
from marqo import errors
from marqo.marqo_logging import mq_logger
from marqo.errors import MarqoWebError
# we want to avoid name conflicts with marqo.version
from json import JSONDecodeError

# A dictionary to cache the marqo url and version for compatibility check


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
            treat_urls_and_pointers_as_images=False, model=None,
            normalize_embeddings=True,
            sentences_per_chunk=2,
            sentence_overlap=0,
            image_preprocessing_method=None,
            settings_dict=None,
            inference_node_type=None,
            storage_node_type=None,
            inference_node_count=1,
            storage_node_count=1,
            replicas_count=0,
            wait_for_readiness=True
    ) -> Dict[str, Any]:
        """Create the index. Please refer to the marqo cloud to see options for inference and storage node types.

        Args:
            index_name: name of the index.
            treat_urls_and_pointers_as_images:
            model:
            normalize_embeddings:
            sentences_per_chunk:
            sentence_overlap:
            image_preprocessing_method:
            settings_dict: if specified, overwrites all other setting
                parameters, and is passed directly as the index's
                index_settings
            inference_node_type:
            storage_node_type:
            inference_node_count;
            storage_node_count:
            replicas_count:
            wait_for_readiness:
        Returns:
            Response body, containing information about index creation result
        """
        return Index.create(
            config=self.config, index_name=index_name,
            treat_urls_and_pointers_as_images=treat_urls_and_pointers_as_images,
            model=model, normalize_embeddings=normalize_embeddings,
            sentences_per_chunk=sentences_per_chunk, sentence_overlap=sentence_overlap,
            image_preprocessing_method=image_preprocessing_method,
            settings_dict=settings_dict, inference_node_type=inference_node_type, storage_node_type=storage_node_type,
            storage_node_count=storage_node_count, replicas_count=replicas_count,
            inference_node_count=inference_node_count, wait_for_readiness=wait_for_readiness
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

    def get_indexes(self) -> Dict[str, List[Index]]:
        """Get all indexes.

        Returns:
        Indexes, a dictionary with the name of indexes.
        """
        response = self.http.get(path='indexes')
        response['results'] = [
            Index(
                config=self.config,
                index_name=index_info["index_name"],
            )
            for index_info in response["results"]
        ]
        return response

    def bulk_search(self, queries: List[Dict[str, Any]], device: Optional[str] = None) -> Dict[str, Any]:
        try:
            parsed_queries = [BulkSearchBody(**q) for q in queries]
        except error_wrappers.ValidationError as e:
            raise errors.InvalidArgError(f"some parameters in search query(s) are invalid. Errors are: {e.errors()}")

        translated_device_param = f"{f'?&device={utils.translate_device_string_for_url(device)}' if device is not None else ''}"
        return self.http.post(
            f"indexes/bulk/search{translated_device_param}",
            body=BulkSearchQuery(queries=parsed_queries).json()
        )

    @staticmethod
    def _base64url_encode(
            data: bytes
    ) -> str:
        return base64.urlsafe_b64encode(data).decode('utf-8').replace('=', '')

    @deprecated(
        "This method is deprecated and will be removed in Marqo 2.0.0. "
        "Please use `mq.index(index_name).get_marqo()` instead. "
        "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details."
    )
    def get_marqo(self):
        if self.config.is_marqo_cloud:
            self.raise_error_for_cloud("get_marqo")
        return self.http.get(path="")

    @deprecated(
        "This method is deprecated and will be removed in Marqo 2.0.0. "
        "Please use `mq.index(index_name).health()` instead. "
        "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details."
    )
    def health(self):
        if self.config.is_marqo_cloud:
            self.raise_error_for_cloud("health")
        try:
            return self.http.get(path="health")
        except (MarqoWebError, RequestException, TypeError, KeyError) as e:
            raise errors.BadRequestError("Marqo encountered an error trying to check the health of the Marqo instance. "
                                         "If you are trying to check the health on Marqo Cloud, please note that "
                                         "the `client.health()` API is not supported on Marqo Cloud and will be removed in "
                                         "Marqo 2.0.0. Please Use `client.index('your-index-name').health()` instead. "
                                         "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details.")

    @deprecated(
        "This method is deprecated and will be removed in Marqo 2.0.0. "
        "Please use 'mq.index(index_name).eject_model() instead. "
        "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details."
    )
    def eject_model(self, model_name: str, model_device: str):
        if self.config.is_marqo_cloud:
            self.raise_error_for_cloud("eject_model")
        return self.http.delete(path=f"models?model_name={model_name}&model_device={model_device}")

    @deprecated(
        "This method is deprecated and will be removed in Marqo 2.0.0. "
        "Please use 'mq.index(index_name).get_loaded_models() instead. "
        "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details."
    )
    def get_loaded_models(self):
        if self.config.is_marqo_cloud:
            self.raise_error_for_cloud("get_loaded_models")
        return self.http.get(path="models")

    @deprecated(
        "This method is deprecated and will be removed in Marqo 2.0.0. "
        "Please use 'mq.index(index_name).get_cuda_info() instead. "
        "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details."
    )
    def get_cuda_info(self):
        if self.config.is_marqo_cloud:
            self.raise_error_for_cloud("get_cuda_info")
        return self.http.get(path="device/cuda")

    @deprecated(
        "This method is deprecated and will be removed in Marqo 2.0.0. "
        "Please use 'mq.index(index_name).get_cpu_info() instead. "
        "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details."
    )
    def get_cpu_info(self):
        if self.config.is_marqo_cloud:
            self.raise_error_for_cloud("get_cpu_info")
        return self.http.get(path="device/cpu")

    @staticmethod
    def raise_error_for_cloud(function_name: str = None):
        raise errors.BadRequestError(
            f"The `mq.{function_name}()` API is not supported on Marqo Cloud. "
            f"Please Use `mq.index('your-index-name').{function_name}()` instead. "
            "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details.")

