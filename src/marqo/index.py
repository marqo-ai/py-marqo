import functools
from datetime import datetime
from timeit import default_timer as timer
from typing import Any, Dict, List, Optional, Union

from packaging import version as versioning_helpers
from requests import RequestException

from marqo import errors, utils
from marqo._httprequests import HttpRequests
from marqo.cloud_helpers import cloud_wait_for_index_status
from marqo.config import Config
from marqo.enums import IndexStatus, InterpolationMethod, EmbedContentType
from marqo.enums import SearchMethods
from marqo.errors import MarqoWebError, UnsupportedOperationError, MarqoCloudIndexNotFoundError
from marqo.marqo_logging import mq_logger
from marqo.models import marqo_index
from marqo.models.create_index_settings import IndexSettings
from marqo.models.marqo_cloud import CloudIndexSettings
from marqo.version import minimum_supported_marqo_version

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
    def create(config: Config,
               index_name: str,
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
               inference_type: Optional[str] = None,
               storage_class: Optional[str] = None,
               number_of_shards: Optional[int] = None,
               number_of_replicas: Optional[int] = None,
               number_of_inferences: Optional[int] = None,
               wait_for_readiness: bool = True,
               text_chunk_prefix: Optional[str] = None,
               text_query_prefix: Optional[str] = None,
               ) -> Dict[str, Any]:
        """Create the index. Please refer to the marqo cloud to see options for inference and storage node types.
        Creates CreateIndexSettings object and then uses it to create the index.
        CreateIndexSettings object sets all parameters to their default values if not specified.

        All parameters are optional, and will be set to their default values if not specified.
        Default values can be found in models/create_index_settings.py CreateIndexSettings class.

        Args:
            config: config instance
            index_name: name of the index.
            type: type of the index, structure or unstructured
            settings_dict: if specified, overwrites all other setting
                parameters, and is passed directly as the index's
                index_settings
            treat_urls_and_pointers_as_images: whether to treat urls and pointers as images in unstructured indexes
            filter_string_max_length: threshold for short string length in unstructured indexes,
                Marqo can filter on short strings but can not filter on long strings
            all_fields: list of fields in the structured index
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
        req = HttpRequests(config)

        # py-marqo against local Marqo
        if config.api_key is None:
            local_create_index_settings: IndexSettings = IndexSettings(
                type=type,
                allFields=all_fields,
                settingsDict=settings_dict,
                treatUrlsAndPointersAsImages=treat_urls_and_pointers_as_images,
                filterStringMaxLength=filter_string_max_length,
                tensorFields=tensor_fields,
                model=model,
                modelProperties=model_properties,
                normalizeEmbeddings=normalize_embeddings,
                textPreprocessing=text_preprocessing,
                imagePreprocessing=image_preprocessing,
                vectorNumericType=vector_numeric_type,
                annParameters=ann_parameters,
                textChunkPrefix=text_chunk_prefix,
                textQueryPrefix=text_query_prefix,
            )

            return req.post(f"indexes/{index_name}", body=local_create_index_settings.generate_request_body())

        # py-marqo against Marqo Cloud
        else:
            cloud_index_settings: CloudIndexSettings = CloudIndexSettings(
                type=type,
                allFields=all_fields,
                settingsDict=settings_dict,
                treatUrlsAndPointersAsImages=treat_urls_and_pointers_as_images,
                filterStringMaxLength=filter_string_max_length,
                tensorFields=tensor_fields,
                model=model,
                modelProperties=model_properties,
                normalizeEmbeddings=normalize_embeddings,
                textPreprocessing=text_preprocessing,
                imagePreprocessing=image_preprocessing,
                vectorNumericType=vector_numeric_type,
                annParameters=ann_parameters,
                numberOfInferences=number_of_inferences,
                inferenceType=inference_type,
                numberOfShards=number_of_shards,
                numberOfReplicas=number_of_replicas,
                storageClass=storage_class,
                textChunkPrefix=text_chunk_prefix,
                textQueryPrefix=text_query_prefix,
            )

            response = req.post(f"indexes/{index_name}", body=cloud_index_settings.generate_request_body())
            if wait_for_readiness:
                cloud_wait_for_index_status(req, index_name, IndexStatus.READY)
            return response

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
               attributes_to_retrieve: Optional[List[str]] = None,
               boost: Optional[Dict[str, List[Union[float, int]]]] = None,
               context: Optional[dict] = None, score_modifiers: Optional[dict] = None,
               model_auth: Optional[dict] = None,
               ef_search: Optional[int] = None, approximate: Optional[bool] = None,
               text_query_prefix: Optional[str] = None,
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
            ef_search: the size of the list of candidates during graph traversal, for tensor search only
            approximate: whether to use approximate nearest neighbors search or not, for tensor search only
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
            "q": q,
            "attributesToRetrieve": attributes_to_retrieve,
            "filter": filter_string,
            "image_download_headers": image_download_headers,
            "context": context,
            "scoreModifiers": score_modifiers,
            "modelAuth": model_auth,
            "efSearch": ef_search,
            "approximate": approximate,
            "searchableAttributes": searchable_attributes,
            "limit": limit,
            "offset": offset,
            "searchMethod": search_method,
            "showHighlights": show_highlights,
            "reRanker": reranker,
            "boost": boost,
            "textQueryPrefix": text_query_prefix,
        }

        body = {k: v for k, v in body.items() if v is not None}

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

    def recommend(self, documents: Union[List[str], Dict[str, float]],
                  tensor_fields: Optional[List[str]] = None,
                  interpolation_method: Optional[InterpolationMethod] = None,
                  exclude_input_documents: Optional[bool] = None,
                  searchable_attributes: Optional[List[str]] = None,
                  limit: Optional[int] = None,
                  offset: Optional[int] = None,
                  filter_string: Optional[str] = None,
                  show_highlights: Optional[bool] = None,
                  reranker: Optional[str] = None,
                  attributes_to_retrieve: Optional[List[str]] = None,
                  score_modifiers: Optional[dict] = None,
                  ef_search: Optional[int] = None,
                  approximate: Optional[bool] = None
                  ) -> Dict[str, Any]:
        """Search the index.

        Args:
            documents: List of document IDs or a dictionary of document IDs with weights
            tensor_fields: Tensor fields within documents to use to generate recommendations
            interpolation_method: Interpolation method to use for combining document embeddings. If not specified,
                Marqo will choose the best method for the index.
            exclude_input_documents: Whether to exclude the input documents from the search results. By default,
                Marqo will exclude the input documents.
            searchable_attributes:  attributes to search
            limit: The max number of documents to be returned
            offset: The number of search results to skip (for pagination)
            show_highlights: True if highlights are to be returned
            reranker:
            device: the device used to index the data. Examples include "cpu",
                "cuda" and "cuda:2".
            filter_string: a filter string, used to prefilter documents during the
                search. For example: "car_colour:blue"
            attributes_to_retrieve: a list of document attributes to be
                retrieved. If left as None, then all attributes will be
                retrieved.
            score_modifiers: a dictionary to modify the score based on field values, for tensor search only
            model_auth: authorisation that lets Marqo download a private model, if required
            ef_search: the size of the list of candidates during graph traversal, for tensor search only
            approximate: whether to use approximate nearest neighbors search or not, for tensor search only
        Returns:
            Dictionary with hits and other metadata
        """

        start_time_client_request = timer()

        path_with_query_str = (
            f"indexes/{self.index_name}/recommend"
        )
        body = {
            "documents": documents,
            "tensorFields": tensor_fields,
            "interpolationMethod": interpolation_method,
            "excludeInputDocuments": exclude_input_documents,
            "limit": limit,
            "offset": offset,
            "efSearch": ef_search,
            "approximate": approximate,
            "searchableAttributes": searchable_attributes,
            "showHighlights": show_highlights,
            "reRanker": reranker,
            "filter": filter_string,
            "attributesToRetrieve": attributes_to_retrieve,
            "scoreModifiers": score_modifiers,
        }

        body = {k: v for k, v in body.items() if v is not None}

        res = self.http.post(
            path=path_with_query_str,
            body=body,
            index_name=self.index_name,
        )

        num_results = len(res["hits"])
        end_time_client_request = timer()
        total_client_request_time = end_time_client_request - start_time_client_request

        recommend_time_log = (f"recommend took {(total_client_request_time):.3f}s to send query "
                              f"and received {num_results} results from Marqo (roundtrip).")
        if 'processingTimeMs' in res:
            recommend_time_log += f" Marqo itself took {(res['processingTimeMs'] * 0.001):.3f}s to " \
                                  f"generate recommendations."

        mq_logger.debug(recommend_time_log)
        return res

    def embed(self, content: Union[Union[str, Dict[str, float]], List[Union[str, Dict[str, float]]]],
              device: Optional[str] = None, image_download_headers: Optional[Dict] = None,
              model_auth: Optional[dict] = None, content_type: Optional[EmbedContentType] = EmbedContentType.Query):
        """Retrieve embeddings for content or list of content.
        Args:
            content: string, dictionary of weighted strings, or list of either. Strings
                to search are text or a pointer/url to an image if the index
                has treat_urls_and_pointers_as_images set to True.

                If queries are weighted, each weight act as a (possibly negative)
                multiplier for that query, relative to the other queries.

            device: the device used to index the data. Examples include "cpu",
                "cuda" and "cuda:2".

            image_download_headers: a dictionary of headers to be passed while downloading images,
                for URLs found in documents
            model_auth: authorisation that lets Marqo download a private model, if required
            content_type: the type of prefix the user wants. "query", "document", or None.
        Returns:
            Dictionary of content, embeddings, and processingTimeMs.
        """

        start_time_client_request = timer()

        path_with_query_str = (
            f"indexes/{self.index_name}/embed"
            f"{f'?&device={utils.translate_device_string_for_url(device)}' if device is not None else ''}"
        )
        body = {
            "content": content,
            "content_type": content_type,
        }

        if image_download_headers is not None:
            body["image_download_headers"] = image_download_headers
        if model_auth is not None:
            body["modelAuth"] = model_auth
        

        res = self.http.post(
            path=path_with_query_str,
            body=body,
            index_name=self.index_name,
        )

        num_results = len(res["embeddings"])
        end_time_client_request = timer()
        total_client_request_time = end_time_client_request - start_time_client_request

        embed_time_log = (f"embed: took {(total_client_request_time):.3f}s to embed content"
                          f"and received {num_results} embeddings from Marqo (roundtrip).")
        if 'processingTimeMs' in res:
            embed_time_log += f" Marqo itself took {(res['processingTimeMs'] * 0.001):.3f}s to execute the embed request."

        mq_logger.debug(embed_time_log)
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
        return self.http.get(url_string, index_name=self.index_name, )

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
            client_batch_size: int = None,
            device: str = None,
            tensor_fields: List[str] = None,
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
            client_batch_size: if it is set, documents will be indexed into batches
                in the client, before being sent off. Otherwise documents are unbatched
                client-side.
            device: the device used to index the data. Examples include "cpu",
                "cuda" and "cuda:2"
            tensor_fields: fields within documents to create and store tensors against.
            use_existing_tensors: use vectors that already exist in the docs.
            image_download_headers: a dictionary of headers to be passed while downloading images,
                for URLs found in documents
            mappings: a dictionary to help handle the object fields. e.g., multimodal_combination field
            model_auth: used to authorise a private model
            text_chunk_prefix: the request level prefix for adding docs
        Returns:
            Response body outlining indexing result
        """

        if image_download_headers is None:
            image_download_headers = dict()
        return self._add_docs_organiser(
            documents=documents,
            client_batch_size=client_batch_size, device=device, tensor_fields=tensor_fields,
            use_existing_tensors=use_existing_tensors,
            image_download_headers=image_download_headers, mappings=mappings, model_auth=model_auth,
            text_chunk_prefix=text_chunk_prefix
        )

    def _add_docs_organiser(
            self,
            documents: List[Dict[str, Any]],
            client_batch_size: int = None,
            device: str = None,
            tensor_fields: List = None,
            use_existing_tensors: bool = False,
            image_download_headers: dict = None,
            mappings: dict = None,
            model_auth: dict = None,
            text_chunk_prefix: str = None,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
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
            "useExistingTensors": use_existing_tensors,
            "imageDownloadHeaders": image_download_headers,
            "mappings": mappings,
            "modelAuth": model_auth,
        }

        if tensor_fields is not None:
            base_body['tensorFields'] = tensor_fields

        if text_chunk_prefix is not None:
            base_body['textChunkPrefix'] = text_chunk_prefix

        end_time_client_process = timer()
        total_client_process_time = end_time_client_process - start_time_client_process
        mq_logger.debug(f"add_documents pre-processing: took {(total_client_process_time):.3f}s for {num_docs} docs.")

        if client_batch_size is not None:
            if client_batch_size <= 0:
                raise errors.InvalidArgError("Batch size can't be less than 1!")
            res = self._batch_request(
                base_path=base_path,
                docs=documents, verbose=False,
                query_str_params=query_str_params, batch_size=client_batch_size, base_body=base_body
            )

        else:
            # no Client Batching

            # Build the query string
            path_with_query_str = f"{base_path}"
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

            if 'processingTimeMs' in res:  # Only outputs log if response is non-empty
                mq_logger.debug(
                    f"add_documents Marqo index: took {(res['processingTimeMs'] / 1000):.3f}s for Marqo to process & index {num_docs} "
                    f"docs.")
            if 'errors' in res and res['errors']:
                mq_logger.info(error_detected_message)
            if errors_detected:
                mq_logger.info(error_detected_message)
        total_add_docs_time = timer() - t0
        mq_logger.debug(f"add_documents completed. total time taken: {(total_add_docs_time):.3f}s.")
        return res

    def update_documents(self, documents: List[Dict], client_batch_size: Optional[int] = None) \
            -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Update documents in this index. Does a partial update on existing documents."""

        t0 = timer()

        error_detected_message = ('Errors detected in update_documents call. '
                                  'Please examine the returned result object for more information.')

        if client_batch_size is not None:
            if (not isinstance(client_batch_size, int)) or client_batch_size <= 0:
                raise errors.InvalidArgError("Batch size must be a positive integer")
            res = self._batch_update_documents(documents, client_batch_size)
        else:
            start_time_client_request = timer()
            num_docs = len(documents)

            base_path = f"indexes/{self.index_name}/documents"
            body = {"documents": documents}

            res = self.http.patch(
                path=base_path, body=body, index_name=self.index_name,
            )
            end_time_client_request = timer()
            total_client_request_time = end_time_client_request - start_time_client_request

            mq_logger.debug(f"update_documents roundtrip: took {(total_client_request_time):.3f}s to send {num_docs} "
                            f"docs to Marqo (roundtrip, unbatched).")
            errors_detected = False

            if 'processingTimeMs' in res:  # Only outputs log if response is non-empty
                mq_logger.debug(
                    f"update_documents Marqo index: took {(res['processingTimeMs'] / 1000):.3f}s for Marqo to process & index {num_docs} "
                    f"docs.")
            if 'errors' in res and res['errors']:
                mq_logger.info(error_detected_message)
            if errors_detected:
                mq_logger.info(error_detected_message)
        total_add_docs_time = timer() - t0
        mq_logger.debug(f"update_documents completed. total time taken: {(total_add_docs_time):.3f}s.")
        return res

    def _update_documents(self, documents: List[Dict]) -> Dict[str, Any]:
        """Update documents in this index. Does a partial update on existing documents."""
        base_path = f"indexes/{self.index_name}/documents/update"
        return self.http.post(path=base_path, body=documents, index_name=self.index_name, )

    def _batch_update_documents(self, documents, client_batch_size) -> List[Dict[str, Any]]:
        """Update documents in this index with batched requests. Does a partial update on existing documents."""

        deeper = ((doc, i, client_batch_size) for i, doc in enumerate(documents))
        base_path = f"indexes/{self.index_name}/documents"

        error_detected_message = ('Errors detected in update_documents call. '
                                  'Please examine the returned result object for more information.')

        def batch_requests(gathered, doc_tuple):
            doc, i, the_batch_size = doc_tuple
            if i % the_batch_size == 0:
                gathered.append([doc, ])
            else:
                gathered[-1].append(doc)
            return gathered

        batched = functools.reduce(lambda x, y: batch_requests(x, y), deeper, [])

        def update_batch_documents(batch_number, docs):
            errors_detected = False

            t0 = timer()

            body = {"documents": docs}
            res = self.http.patch(path=base_path, body=body, index_name=self.index_name)

            total_batch_time = timer() - t0
            num_docs = len(docs)

            if 'processingTimeMs' in res:  # Only outputs log if response is non-empty
                mq_logger.info(
                    f"    update_documents batch {batch_number}: took {(res['processingTimeMs'] / 1000):.3f}s "
                    f"for Marqo to process & index {num_docs} docs. Roundtrip time: {(total_batch_time):.3f}s.")
                if 'errors' in res and res['errors']:
                    errors_detected = True

            if errors_detected:
                mq_logger.info(f"    update_documents batch {batch_number}: {error_detected_message}")
            return res

        results = [update_batch_documents(batch_number, docs) for batch_number, docs in enumerate(batched)]
        mq_logger.debug('completed batch ingestion.')
        return results

    def delete_documents(self, ids: List[str]) -> Dict[str, int]:
        """Delete documents from this index by a list of their ids.

        Args:
            ids: List of identifiers of documents.

        Returns:
            A dict with information about the delete operation.
        """
        base_path = f"indexes/{self.index_name}/documents/delete-batch"

        return self.http.post(path=base_path, body=ids, index_name=self.index_name, )

    def get_stats(self) -> Dict[str, Any]:
        """Get stats about the index"""
        return self.http.get(path=f"indexes/{self.index_name}/stats", index_name=self.index_name, )

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
            self, docs: List[Dict], base_path: str,
            query_str_params: str, base_body: dict, verbose: bool = True, batch_size: int = 50,
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
                if 'processingTimeMs' in res:  # Only outputs log if response is non-empty
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
        mq_logger.debug('completed batch ingestion.')
        return results

    def get_settings(self) -> dict:
        """Get all settings of the index"""
        return self.http.get(path=f"indexes/{self.index_name}/settings", index_name=self.index_name, )

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
                if versioning_helpers.parse(marqo_version).major == 1:
                    mq_logger.warning(
                        f"Your current Marqo Python client requires a minimum Marqo version of "
                        f"{minimum_supported_marqo_version()} to function properly. "
                        f"Please upgrade your Marqo instance to avoid potential errors. "
                        f"If you intended to use Marqo 1.x, please use Python client 2.x (pip install marqo==2). "
                        f"If you have already changed your Marqo instance but still get this warning, "
                        f"please restart your Python interpreter.")
                else:
                    mq_logger.warning(f"Your Marqo Python client requires a minimum Marqo version of "
                                      f"{minimum_supported_marqo_version()} to function properly, "
                                      f"but your Marqo version is {marqo_version}. "
                                      f"Please upgrade your Marqo instance to avoid potential errors. "
                                      f"If you have already changed your Marqo instance but still get this warning, "
                                      f"please restart your Python interpreter.")
        except (MarqoWebError, RequestException, TypeError, KeyError, MarqoCloudIndexNotFoundError,
                versioning_helpers.InvalidVersion) as e:
            # skip the check if this is a cloud index that is still being created:
            if not (self.config.is_marqo_cloud and not
            self.config.instance_mapping.is_index_usage_allowed(index_name=self.index_name)):
                mq_logger.warning(skip_warning_message)
            if url is not None:
                marqo_url_and_version_cache[url] = "_skipped"
        return
