import functools
import inspect
import json
import pprint
import time

from requests import RequestException

from marqo1 import defaults
from marqo1.models.create_index_settings import CreateIndexSettings
from marqo1.cloud_helpers import cloud_wait_for_index_status
from marqo1.enums import IndexStatus
import typing
from urllib import parse
from datetime import datetime
from timeit import default_timer as timer
from typing import Any, Dict, Generator, List, Optional, Union
from marqo1._httprequests import HttpRequests
from marqo1.config import Config
from marqo1.enums import SearchMethods, Devices
from marqo1 import errors, utils
from marqo1.errors import MarqoWebError, UnsupportedOperationError, MarqoCloudIndexNotFoundError
from marqo1.marqo_logging import mq_logger
from marqo1.version import minimum_supported_marqo_version
from packaging import version as versioning_helpers

marqo_url_and_version_cache: Dict[str, str] = {}


class Index:
    """
    Wraps the /indexes/ endpoint
    """

    def __init__(
        self,
        config: Config,
        index_name: str,
        created_at: Optional[Union[datetime, str]] = None,
        updated_at: Optional[Union[datetime, str]] = None,
    ) -> None:
        """

        Args:
            config: config object location and other info of marqo.
            index_name: name of the index
            created_at:
            updated_at:
        """
        self.config = config
        self.http = HttpRequests(config)
        self.index_name = index_name
        self.created_at = self._maybe_datetime(created_at)
        self.updated_at = self._maybe_datetime(updated_at)

        skip_version_check = False
        # trying to get index url to verify that index is mapped
        try:
            self.config.instance_mapping.get_index_base_url(self.index_name)
        except errors.MarqoError as e:
            mq_logger.debug(
                f'Cache update on index object instantiation could not retrieve index URL: {e}')
            skip_version_check = True

        if (self.config.instance_mapping.is_index_usage_allowed(index_name=self.index_name)
                and not skip_version_check):
            self._marqo_minimum_supported_version_check()

    def delete(self, wait_for_readiness=True) -> Dict[str, Any]:
        """Delete the index.

        Args:
            wait_for_readiness: Marqo Cloud specific, whether to wait until
                operation is completed or to proceed without waiting for status,
                won't do anything if config.is_marqo_cloud=False
        """
        response = self.http.delete(path=f"indexes/{self.index_name}")
        if self.config.is_marqo_cloud and wait_for_readiness:
            cloud_wait_for_index_status(self.http, self.index_name, IndexStatus.DELETED)
        return response

    @staticmethod
    def create(config: Config, index_name: str,
               treat_urls_and_pointers_as_images=None,
               model=None,
               normalize_embeddings=None,
               sentences_per_chunk=None,
               sentence_overlap=None,
               image_preprocessing_method=None,
               settings_dict=None,
               inference_node_type=None,
               storage_node_type=None,
               inference_node_count=None,
               storage_node_count=None,
               replicas_count=None,
               wait_for_readiness=None,
               inference_type=None,
               storage_class=None,
               number_of_inferences=None,
               number_of_shards=None,
               number_of_replicas=None
               ) -> Dict[str, Any]:
        """Create the index. Please refer to the marqo cloud to see options for inference and storage node types.
        Creates CreateIndexSettings object and then uses it to create the index.
        CreateIndexSettings object sets all parameters to their default values if not specified.

        All parameters are optional, and will be set to their default values if not specified.
        Default values can be found in models/create_index_settings.py CreateIndexSettings class.

        Args:
            config: config instance
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
            inference_node_type (deprecated): inference type for the index. replaced by inference_type
            storage_node_type (deprecated): storage type for the index. replaced by storage_class
            inference_node_count (deprecated): number of inference nodes for the index. replaced by number_of_inferences
            storage_node_count (deprecated): number of storage nodes for the index. replaced by number_of_shards
            replicas_count (deprecated): number of replicas for the index. replaced by number_of_replicas
            wait_for_readiness: Marqo Cloud specific, whether to wait until
                operation is completed or to proceed without waiting for status,
                won't do anything if config.is_marqo_cloud=False
            inference_type: inference type for the index
            storage_class: storage class for the index
            number_of_inferences: number of inferences for the index
            number_of_shards: number of shards for the index
            number_of_replicas: number of replicas for the index
        Returns:
            Response body, containing information about index creation result
        """
        req = HttpRequests(config)
        create_index_settings = CreateIndexSettings(
            treat_urls_and_pointers_as_images=treat_urls_and_pointers_as_images,
            model=model,
            normalize_embeddings=normalize_embeddings,
            sentences_per_chunk=sentences_per_chunk,
            sentence_overlap=sentence_overlap,
            image_preprocessing_method=image_preprocessing_method,
            settings_dict=settings_dict,
            inference_node_type=inference_node_type,
            storage_node_type=storage_node_type,
            inference_node_count=inference_node_count,
            storage_node_count=storage_node_count,
            replicas_count=replicas_count,
            inference_type=inference_type,
            storage_class=storage_class,
            number_of_inferences=number_of_inferences,
            number_of_shards=number_of_shards,
            number_of_replicas=number_of_replicas,
            wait_for_readiness=wait_for_readiness
        )

        if create_index_settings.settings_dict is not None and create_index_settings.settings_dict:
            response = req.post(f"indexes/{index_name}", body=create_index_settings.settings_dict)
            if config.is_marqo_cloud and create_index_settings.wait_for_readiness:
                cloud_wait_for_index_status(req, index_name, IndexStatus.READY)
            return response

        if config.api_key is not None:
            # making the keyword settings params override the default cloud
            #  settings
            cl_settings = defaults.get_cloud_default_index_settings()
            cl_ix_defaults = cl_settings['index_defaults']
            cl_ix_defaults['treat_urls_and_pointers_as_images'] = \
                create_index_settings.treat_urls_and_pointers_as_images
            if create_index_settings.model is not None:
                cl_ix_defaults['model'] = create_index_settings.model
            else:
                cl_ix_defaults.pop('model', None)
            cl_ix_defaults['normalize_embeddings'] = create_index_settings.normalize_embeddings
            cl_text_preprocessing = cl_ix_defaults['text_preprocessing']
            cl_text_preprocessing['split_overlap'] = create_index_settings.sentence_overlap
            cl_text_preprocessing['split_length'] = create_index_settings.sentences_per_chunk
            cl_img_preprocessing = cl_ix_defaults['image_preprocessing']
            if image_preprocessing_method is not None:
                cl_img_preprocessing['patch_method'] = create_index_settings.image_preprocessing_method
            else:
                cl_img_preprocessing.pop('patch_method', None)
            if not config.is_marqo_cloud:
                return req.post(f"indexes/{index_name}", body=cl_settings)
            if create_index_settings.inference_type is not None:
                cl_settings['inference_type'] = create_index_settings.inference_type
            else:
                cl_settings.pop('inference_type', None)
            if create_index_settings.storage_class is not None:
                cl_settings['storage_class'] = create_index_settings.storage_class
            else:
                cl_settings.pop('storage_class', None)
            cl_settings['number_of_inferences'] = create_index_settings.number_of_inferences
            cl_settings['number_of_replicas'] = create_index_settings.number_of_replicas
            cl_settings['number_of_shards'] = create_index_settings.number_of_shards
            response = req.post(f"indexes/{index_name}", body=cl_settings)
            if create_index_settings.wait_for_readiness:
                cloud_wait_for_index_status(req, index_name, IndexStatus.READY)
            return response

        return req.post(f"indexes/{index_name}", body={
            "index_defaults": {
                "treat_urls_and_pointers_as_images": create_index_settings.treat_urls_and_pointers_as_images,
                "model": create_index_settings.model,
                "normalize_embeddings": create_index_settings.normalize_embeddings,
                "text_preprocessing": {
                    "split_overlap": create_index_settings.sentence_overlap,
                    "split_length": create_index_settings.sentences_per_chunk,
                    "split_method": "sentence"
                },
                "image_preprocessing": {
                    "patch_method": create_index_settings.image_preprocessing_method
                }
            }
        })

    def refresh(self):
        """refreshes the index"""
        return self.http.post(path=F"indexes/{self.index_name}/refresh", index_name=self.index_name,)

    def get_status(self):
        """gets the status of the index"""
        if self.config.is_marqo_cloud:
            return self.http.get(path=F"indexes/{self.index_name}/status")
        else:
            raise UnsupportedOperationError("This operation is only supported for Marqo Cloud")

    def search(self, q: Optional[Union[str, dict]] = None, searchable_attributes: Optional[List[str]] = None,
               limit: int = 10, offset: int = 0, search_method: Union[SearchMethods.TENSOR, str] = SearchMethods.TENSOR,
               highlights=None, device: Optional[str] = None, filter_string: str = None,
               show_highlights=True, reranker=None, image_download_headers: Optional[Dict] = None,
               attributes_to_retrieve: Optional[List[str]] = None, boost: Optional[Dict[str,List[Union[float, int]]]] = None,
               context: Optional[dict] = None, score_modifiers: Optional[dict] = None, model_auth: Optional[dict] = None,
               text_query_prefix: Optional[str] = None
               ) -> Dict[str, Any]:
        """Search the index.

        Args:
            q: String to search, or a dictionary of weighted strings to search
                (with the structure <search string>:<weight float>). Strings
                to search are text or a pointer/url to an image if the index
                has treat_urls_and_pointers_as_images set to True. 

                If queries are weighted, each weight act as a (possibly negative)
                multiplier for that query, relative to the other queries.

                Optional. Marqo will evaluate whether context is given.
            searchable_attributes:  attributes to search
            limit: The max number of documents to be returned
            offset: The number of search results to skip (for pagination)
            search_method: Indicates TENSOR or LEXICAL (keyword) search
            show_highlights: True if highlights are to be returned
            reranker:
            device: the device used to index the data. Examples include "cpu",
                "cuda" and "cuda:2".
            filter_string: a filter string, used to prefilter documents during the
                search. For example: "car_colour:blue"
            attributes_to_retrieve: a list of document attributes to be
                retrieved. If left as None, then all attributes will be
                retrieved.
            context: a dictionary to allow you to bring your own vectors and more into search.
            score_modifiers: a dictionary to modify the score based on field values, for tensor search only
            model_auth: authorisation that lets Marqo download a private model, if required
            text_query_prefix: a string to prefix all text queries
        Returns:
            Dictionary with hits and other metadata
        """

        start_time_client_request = timer()
        if highlights is not None:
            mq_logger.warning("Deprecation warning for parameter 'highlights'. "
                              "Please use the 'showHighlights' instead. ")
            show_highlights = highlights if show_highlights is True else show_highlights

        path_with_query_str = (
            f"indexes/{self.index_name}/search"
            f"{f'?&device={utils.translate_device_string_for_url(device)}' if device is not None else ''}"
        )
        body = {
            "searchableAttributes": searchable_attributes,
            "limit": limit,
            "offset": offset,
            "searchMethod": search_method,
            "showHighlights": show_highlights,
            "reRanker": reranker,
            "boost": boost,
        }
        if q is not None:
            body["q"] = q
        if attributes_to_retrieve is not None:
            body["attributesToRetrieve"] = attributes_to_retrieve
        if filter_string is not None:
            body["filter"] = filter_string
        if image_download_headers is not None:
            body["image_download_headers"] = image_download_headers
        if context is not None:
            body["context"] = context
        if score_modifiers is not None:
            body["scoreModifiers"] = score_modifiers
        if model_auth is not None:
            body["modelAuth"] = model_auth
        if text_query_prefix is not None:
            body["textQueryPrefix"] = text_query_prefix
        
        res = self.http.post(
            path=path_with_query_str,
            body=body,
            index_name=self.index_name,
        )

        num_results = len(res["hits"])
        end_time_client_request = timer()
        total_client_request_time = end_time_client_request - start_time_client_request

        search_time_log = (f"search ({search_method.lower()}): took {(total_client_request_time):.3f}s to send query "
                           f"and received {num_results} results from Marqo (roundtrip).")
        if 'processingTimeMs' in res:
            search_time_log += f" Marqo itself took {(res['processingTimeMs'] * 0.001):.3f}s to execute the search."

        mq_logger.debug(search_time_log)
        return res

    def get_document(self, document_id: str, expose_facets=None) -> Dict[str, Any]:
        """Get one document with given an ID.

        Args:
            document_id: ID of the document.
            expose_facets: If True, tensor facets will be returned for the the
                document. Each facets' embedding is accessible via the
                _embedding field.

        Returns:
            Dictionary containing the documents information.
        """
        url_string = f"indexes/{self.index_name}/documents/{document_id}"
        if expose_facets is not None:
            url_string += f"?expose_facets={expose_facets}"
        return self.http.get(url_string, index_name=self.index_name,)

    def get_documents(self, document_ids: List[str], expose_facets=None) -> Dict[str, Any]:
        """Gets a selection of documents based on their IDs.

        Args:
            document_ids: IDs to be searched
            expose_facets: If True, tensor facets will be returned for the the
                document. Each facets' embedding is accessible via the
                _embedding field.

        Returns:
            Dictionary containing the documents information.
        """
        url_string = f"indexes/{self.index_name}/documents"
        if expose_facets is not None:
            url_string += f"?expose_facets={expose_facets}"
        return self.http.get(
            url_string,
            body=document_ids,
            index_name=self.index_name,
        )

    def add_documents(
        self,
        documents: List[Dict[str, Any]],
        auto_refresh: bool = None,
        client_batch_size: int = None,
        device: str = None,
        tensor_fields: List[str] = None,
        non_tensor_fields: List[str] = None,
        use_existing_tensors: bool = False,
        image_download_headers: dict = None,
        mappings: dict = None,
        model_auth: dict = None,
        text_chunk_prefix: str = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Add documents to this index. Does a partial update on existing documents,
        based on their ID. Adds unseen documents to the index.

        Args:
            documents: List of documents. Each document should be a dictionary.
            auto_refresh: Automatically refresh the index. If you are making
                lots of requests, it is advised to set this to False to
                increase performance.
            client_batch_size: if it is set, documents will be indexed into batches
                in the client, before being sent off. Otherwise documents are unbatched
                client-side.
            device: the device used to index the data. Examples include "cpu",
                "cuda" and "cuda:2"
            tensor_fields: fields within documents to create and store tensors against.
            non_tensor_fields: fields within documents to not create and store tensors against. Cannot be used with
                tensor_fields.
                .. deprecated:: 2.0.0
                    This parameter has been deprecated and will be removed in Marqo 2.0.0. User tensor_fields instead.
            use_existing_tensors: use vectors that already exist in the docs.
            image_download_headers: a dictionary of headers to be passed while downloading images,
                for URLs found in documents
            mappings: a dictionary to help handle the object fields. e.g., multimodal_combination field
            model_auth: used to authorise a private model
            text_chunk_prefix: a string to prefix all text chunks with internally
        Returns:
            Response body outlining indexing result
        """
        if tensor_fields is not None and non_tensor_fields is not None:
            raise errors.InvalidArgError('Cannot define `non_tensor_fields` when `tensor_fields` is defined. '
                                         '`non_tensor_fields` has been deprecated and will be removed in Marqo 2.0.0. '
                                         'Its use is discouraged.')

        if tensor_fields is None and non_tensor_fields is None:
            raise errors.InvalidArgError('You must include the `tensor_fields` parameter. '
                                         'Use `tensor_fields=[]` to index for lexical-only search.')

        if non_tensor_fields is not None:
            mq_logger.warning('The `non_tensor_fields` parameter has been deprecated and will be removed in '
                              'Marqo 2.0.0. Use `tensor_fields` instead.')

        if image_download_headers is None:
            image_download_headers = dict()
        return self._add_docs_organiser(
            documents=documents, auto_refresh=auto_refresh,
            client_batch_size=client_batch_size, device=device, tensor_fields=tensor_fields,
            non_tensor_fields=non_tensor_fields, use_existing_tensors=use_existing_tensors,
            image_download_headers=image_download_headers, mappings=mappings, model_auth=model_auth, text_chunk_prefix=text_chunk_prefix
        )

    def _add_docs_organiser(
        self,
        documents: List[Dict[str, Any]],
        auto_refresh=None,
        client_batch_size: int = None,
        device: str = None,
        tensor_fields: List = None,
        non_tensor_fields: List = None,
        use_existing_tensors: bool = False,
        image_download_headers: dict = None,
        mappings: dict = None,
        model_auth: dict = None,
        text_chunk_prefix: str = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:

        if (tensor_fields is None and non_tensor_fields is None) \
                or (tensor_fields is not None and non_tensor_fields is not None):
            raise ValueError("Exactly one of tensor_fields or non_tensor_fields must be provided.")

        error_detected_message = ('Errors detected in add documents call. '
                                  'Please examine the returned result object for more information.')

        num_docs = len(documents)

        # ADD DOCS TIMER-LOGGER (1)
        t0 = timer()
        start_time_client_process = timer()
        base_path = f"indexes/{self.index_name}/documents"
        # Note: refresh is not included here since if the request is client batched, the refresh is explicity called after all batches are added.
        # telemetry is not included here since it is implemented at the client level, not the request level.
        query_str_params = (
            f"{f'device={utils.translate_device_string_for_url(device)}' if device is not None else ''}"
        )

        base_body = {
            "useExistingTensors" : use_existing_tensors,
            "imageDownloadHeaders" : image_download_headers,
            "mappings" : mappings,
            "modelAuth": model_auth,
        }
        if tensor_fields is not None:
            base_body['tensorFields'] = tensor_fields
        else:
            base_body['nonTensorFields'] = non_tensor_fields

        if text_chunk_prefix is not None:
            base_body['textChunkPrefix'] = text_chunk_prefix
        
        end_time_client_process = timer()
        total_client_process_time = end_time_client_process - start_time_client_process
        mq_logger.debug(f"add_documents pre-processing: took {(total_client_process_time):.3f}s for {num_docs} docs.")

        if client_batch_size is not None:
            if client_batch_size <= 0:
                raise errors.InvalidArgError("Batch size can't be less than 1!")
            res = self._batch_request(
                base_path=base_path, auto_refresh=auto_refresh,
                docs=documents, verbose=False,
                query_str_params=query_str_params, batch_size=client_batch_size, base_body = base_body
            )

        else:
            # no Client Batching

            # Build the query string
            path_with_query_str = f"{base_path}"
            if auto_refresh is not None:
                # Add refresh if it has been user-specified
                path_with_query_str += f"?refresh={str(auto_refresh).lower()}"
                if query_str_params:
                    # Also add device if it has been user-specified
                    path_with_query_str += f"&{query_str_params}"
            else:
                if query_str_params:
                    # Only add device if it has been user-specified
                    path_with_query_str += f"?{query_str_params}"

            # ADD DOCS TIMER-LOGGER (2)
            start_time_client_request = timer()

            body = {"documents": documents, **base_body}
            res = self.http.post(
                path=path_with_query_str, body=body, index_name=self.index_name,
            )
            end_time_client_request = timer()
            total_client_request_time = end_time_client_request - start_time_client_request

            mq_logger.debug(f"add_documents roundtrip: took {(total_client_request_time):.3f}s to send {num_docs} "
                            f"docs to Marqo (roundtrip, unbatched).")
            errors_detected = False

            if 'processingTimeMs' in res:       # Only outputs log if response is non-empty
                mq_logger.debug(f"add_documents Marqo index: took {(res['processingTimeMs'] / 1000):.3f}s for Marqo to process & index {num_docs} "
                                f"docs.")
            if 'errors' in res and res['errors']:
                mq_logger.info(error_detected_message)
            if errors_detected:
                mq_logger.info(error_detected_message)
        total_add_docs_time = timer() - t0
        mq_logger.debug(f"add_documents completed. total time taken: {(total_add_docs_time):.3f}s.")
        return res

    def delete_documents(self, ids: List[str], auto_refresh: bool = None) -> Dict[str, int]:
        """Delete documents from this index by a list of their ids.

        Args:
            ids: List of identifiers of documents.
            auto_refresh: if true refreshes the index after deletion

        Returns:
            A dict with information about the delete operation.
        """
        base_path = f"indexes/{self.index_name}/documents/delete-batch"
        path_with_refresh = base_path if auto_refresh is None else base_path + f"?refresh={str(auto_refresh).lower()}"

        return self.http.post(path=path_with_refresh, body=ids, index_name=self.index_name,)

    def get_stats(self) -> Dict[str, Any]:
        """Get stats about the index"""
        return self.http.get(path=f"indexes/{self.index_name}/stats", index_name=self.index_name,)

    @staticmethod
    def _maybe_datetime(the_date: Optional[Union[datetime, str]]) -> Optional[datetime]:
        """This should handle incoming timestamps from Marqo, including
         parsing if necessary."""
        if the_date is None or not the_date:
            return None

        if isinstance(the_date, datetime):
            return the_date
        elif isinstance(the_date, str):
            parsed_date = datetime.strptime(the_date, "%Y-%m-%dT%H:%M:%S.%f")
            return parsed_date

    def _batch_request(
            self, docs: List[Dict],  base_path: str,
            query_str_params: str, base_body: dict, verbose: bool = True,
            auto_refresh: bool = None, batch_size: int = 50,
    ) -> List[Dict[str, Any]]:
        """Batches a large chunk of documents to be sent as multiple
        add_documents invocations

        Args:
            docs: A list of documents
            batch_size: Size of a batch passed into a single add_documents
                call
            base_path: The base path for the add_documents call
            query_str_params: The query string parameters for the add_documents call
            base_body: The base body for the add_documents call
            auto_refresh: If true, refreshes the index AFTER all batches are added. Not included in any batch query string.
            verbose: If true, prints out info about the documents

        Returns:
            A list of responses, which have information about the batch
            operation
        """
        path_with_query_str = f"{base_path}?refresh=false"
        if query_str_params:
            # Only add device if it has been user-specified
            path_with_query_str += f"&{query_str_params}"

        mq_logger.debug(f"starting batch ingestion with batch size {batch_size}")
        error_detected_message = ('Errors detected in add documents call. '
                                  'Please examine the returned result object for more information.')

        deeper = ((doc, i, batch_size) for i, doc in enumerate(docs))
        def batch_requests(gathered, doc_tuple):
            doc, i, the_batch_size = doc_tuple
            if i % the_batch_size == 0:
                gathered.append([doc, ])
            else:
                gathered[-1].append(doc)
            return gathered

        batched = functools.reduce(lambda x, y: batch_requests(x, y), deeper, [])

        def verbosely_add_docs(i, docs):
            errors_detected = False

            t0 = timer()
            body = {"documents": docs, **base_body}
            res = self.http.post(path=path_with_query_str, body=body, index_name=self.index_name)

            total_batch_time = timer() - t0
            num_docs = len(docs)

            if isinstance(res, list):
                # with Server Batching (show processing time for each batch)
                mq_logger.info(
                    f"    add_documents batch {i} roundtrip: took {(total_batch_time):.3f}s to add {num_docs} docs.")

                if isinstance(res[0], list):
                    # for multiprocess, timing messages should be arranged by process, then batch
                    for process in range(len(res)):
                        mq_logger.debug(f"       process {process}:")

                        for batch in range(len(res[process])):
                            server_batch_result_count = len(res[process][batch]["items"])
                            mq_logger.debug(f"           marqo server batch {batch}: "
                                            f"processed {server_batch_result_count} docs in {(res[process][batch]['processingTimeMs'] / 1000):.3f}s.")
                            if 'errors' in res[process][batch] and res[process][batch]['errors']:
                                errors_detected = True

                else:
                    # for single process, timing messages should be arranged by batch ONLY
                    for batch in range(len(res)):
                        server_batch_result_count = len(res[batch]["items"])
                        mq_logger.debug(f"       marqo server batch {batch}: "
                                        f"processed {server_batch_result_count} docs in {(res[batch]['processingTimeMs'] / 1000):.3f}s.")
                        if 'errors' in res[batch] and res[batch]['errors']:
                            errors_detected = True
            else:
                # no Server Batching
                if 'processingTimeMs' in res:       # Only outputs log if response is non-empty
                    mq_logger.info(
                        f"    add_documents batch {i}: took {(res['processingTimeMs'] / 1000):.3f}s for Marqo to process & index {num_docs} docs."
                        f" Roundtrip time: {(total_batch_time):.3f}s.")
                    if 'errors' in res and res['errors']:
                        errors_detected = True

            if errors_detected:
                mq_logger.info(f"    add_documents batch {i}: {error_detected_message}")
            if verbose:
                mq_logger.info(f"results from indexing batch {i}: {res}")
            return res

        results = [verbosely_add_docs(i, docs) for i, docs in enumerate(batched)]
        if auto_refresh:
            self.refresh()
        mq_logger.debug('completed batch ingestion.')
        return results

    def get_settings(self) -> dict:
        """Get all settings of the index"""
        return self.http.get(path=f"indexes/{self.index_name}/settings", index_name=self.index_name,)

    def health(self) -> dict:
        """Check the health of an index"""
        return self.http.get(path=f"indexes/{self.index_name}/health", index_name=self.index_name)

    def get_loaded_models(self):
        return self.http.get(path="models", index_name=self.index_name)

    def get_cuda_info(self):
        return self.http.get(path="device/cuda", index_name=self.index_name)

    def get_cpu_info(self):
        return self.http.get(path="device/cpu", index_name=self.index_name)

    def get_marqo(self):
        return self.http.get(path="", index_name=self.index_name)

    def eject_model(self, model_name: str, model_device: str):
        return self.http.delete(
            path=f"models?model_name={model_name}&model_device={model_device}", index_name=self.index_name
        )

    def _marqo_minimum_supported_version_check(self):
        min_ver = minimum_supported_marqo_version()
        # in case we have a problem getting the index's URL:
        skip_warning_message = (
            f"Marqo encountered a problem trying to check the Marqo version for index_name `{self.index_name}`. "
            f"The minimum supported Marqo version for this client is {min_ver}. "
            f"If you are sure your Marqo version is compatible with this client, you can ignore this message. ")

        url = None

        # Do version check
        try:
            url = self.config.instance_mapping.get_index_base_url(self.index_name)
            skip_warning_message = (
                f"Marqo encountered a problem trying to check the Marqo version found at `{url}`. "
                f"The minimum supported Marqo version for this client is {min_ver}. "
                f"If you are sure your Marqo version is compatible with this client, you can ignore this message. ")

            if url not in marqo_url_and_version_cache:
                # self.get_marqo() uses get_index_base_url(), so it should be available
                marqo_url_and_version_cache[url] = self.get_marqo()["version"]
            else:
                # we already have the version cached, and therefor also logged a warning if needed
                return

            marqo_version = marqo_url_and_version_cache[url]

            if marqo_version == "_skipped":
                return

            if versioning_helpers.parse(marqo_version) < versioning_helpers.parse(min_ver):
                mq_logger.warning(f"Your Marqo Python client requires a minimum Marqo version of "
                                  f"{minimum_supported_marqo_version()} to function properly, but your Marqo version is {marqo_version}. "
                                  f"Please upgrade your Marqo instance to avoid potential errors. "
                                  f"If you have already changed your Marqo instance but still get this warning, please restart your Marqo client Python interpreter.")
        except (MarqoWebError, RequestException, TypeError, KeyError, MarqoCloudIndexNotFoundError,
                versioning_helpers.InvalidVersion) as e:
            # skip the check if this is a cloud index that is still being created:
            if not (self.config.is_marqo_cloud and not
                    self.config.instance_mapping.is_index_usage_allowed(index_name=self.index_name)):
                mq_logger.warning(skip_warning_message)
            if url is not None:
                marqo_url_and_version_cache[url] = "_skipped"
        return
