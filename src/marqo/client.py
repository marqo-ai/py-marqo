import base64
from typing import Any, Dict, List, Optional, Union

from pydantic import error_wrappers
from requests.exceptions import RequestException

from marqo.index import Index
from marqo.config import Config
from marqo.models import BulkSearchBody, BulkSearchQuery
from marqo._httprequests import HttpRequests
from marqo import utils, enums
from marqo import errors
from marqo.version import minimum_supported_marqo_version
from marqo.marqo_logging import mq_logger
from marqo.errors import MarqoWebError
# we want to avoid name conflicts with marqo.version
from packaging import version as versioning_helpers
from json import JSONDecodeError

# A dictionary to cache the marqo url and version for compatibility check
marqo_url_and_version_cache = {}


class Client:
    """
    A client for the marqo API

    A client instance is needed for every marqo API method to know the location of
    marqo and its permissions.
    """

    def __init__(
            self, url: str = "http://localhost:8882",
            main_user: str = None, main_password: str = None,
            return_telemetry: bool = False,
            api_key: str = None
    ) -> None:
        """
        Parameters
        ----------
        url:
            The url to the S2Search API (ex: http://localhost:8882)
        """
        self.main_user = main_user
        self.main_password = main_password
        if (main_user is not None) and (main_password is not None):
            self.url = utils.construct_authorized_url(url_base=url, username=main_user, password=main_password)
        else:
            self.url = url
        self.config = Config(self.url, use_telemetry=return_telemetry, api_key=api_key)
        self.http = HttpRequests(self.config)
        self._marqo_minimum_supported_version_check()

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
            inference_node_count=inference_node_count,
        )

    def delete_index(self, index_name: str) -> Dict[str, Any]:
        """Deletes an index

        Args:
            index_name: name of the index

        Returns:
            response body about the result of the delete request
        """
        try:
            res = self.http.delete(path=f"indexes/{index_name}")
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

    def enrich(self, documents: List[Dict], enrichment: Dict, device: str = None, ):
        """Enrich documents"""
        translated = utils.translate_device_string_for_url(device)
        response = self.http.post(path=f'enrichment?device={translated}', body={
            "documents": documents,
            "enrichment": enrichment
        })
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

    def get_marqo(self):
        return self.http.get(path="")

    def health(self):
        mq_logger.warning('The `client.health()` API has been deprecated and will be removed in '
                          'Marqo 2.0.0. Use `client.index(index_name).health()` instead. '
                          'Check `https://docs.marqo.ai/latest/API-Reference/indexes/` for more details.')
        try:
            return self.http.get(path="health")
        except (MarqoWebError, RequestException, TypeError, KeyError) as e:
            raise errors.BadRequestError("Marqo encountered an error trying to check the health of the Marqo instance. "
                                         "If you are trying to check the health on Marqo Cloud, please note that "
                                         "the `client.health()` API is not supported on Marqo Cloud and will be removed in "
                                         "Marqo 2.0.0. Please Use `client.index('your-index-name').health()` instead. "
                                         "Check `https://docs.marqo.ai/1.1.0/API-Reference/indexes/` for more details.")

    def eject_model(self, model_name: str, model_device: str):
        return self.http.delete(path=f"models?model_name={model_name}&model_device={model_device}")

    def get_loaded_models(self):
        return self.http.get(path="models")

    def get_cuda_info(self):
        return self.http.get(path="device/cuda")

    def get_cpu_info(self):
        return self.http.get(path="device/cpu")

    def _marqo_minimum_supported_version_check(self):
        min_ver = minimum_supported_marqo_version()
        skip_warning_message = (
            f"Marqo encountered a problem trying to check the Marqo version found at `{self.url}`. "
            f"The minimum supported Marqo version for this client is {min_ver}. "
            f"If you are sure your Marqo version is compatible with this client, you can ignore this message. ")

        # Skip the check if the url is previously labelled as "_skipped"
        if self.url in marqo_url_and_version_cache and marqo_url_and_version_cache[self.url] == "_skipped":
            mq_logger.warning(skip_warning_message)
            return

        # Skip the check for Marqo CloudV2 APIs right now
        skip_version_check_url = ["https://api.marqo.ai", "https://cloud.marqo.ai"]
        if self.url in skip_version_check_url:
            marqo_url_and_version_cache[self.url] = "_skipped"
            mq_logger.warning(skip_warning_message)
            return

        # Do version check
        try:
            if self.url not in marqo_url_and_version_cache:
                marqo_url_and_version_cache[self.url] = self.get_marqo()["version"]
            marqo_version = marqo_url_and_version_cache[self.url]
            if versioning_helpers.parse(marqo_version) < versioning_helpers.parse(min_ver):
                mq_logger.warning(f"Your Marqo Python client requires a minimum Marqo version of "
                                  f"{minimum_supported_marqo_version()} to function properly, but your Marqo version is {marqo_version}. "
                                  f"Please upgrade your Marqo instance to avoid potential errors. "
                                  f"If you have already changed your Marqo instance but still get this warning, please restart your Marqo client Python interpreter.")
        except (MarqoWebError, RequestException, TypeError, KeyError) as e:
            mq_logger.warning(skip_warning_message)
            marqo_url_and_version_cache[self.url] = "_skipped"
        return
