import functools
import json
import logging
import pprint
from marqo import defaults
import typing
from urllib import parse
from datetime import datetime
from timeit import default_timer as timer
from typing import Any, Dict, Generator, List, Optional, Union
from marqo._httprequests import HttpRequests
from marqo.config import Config
from marqo.enums import SearchMethods, Devices
from marqo import errors, utils
from marqo.marqo_logging import mq_logger


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

    def delete(self) -> Dict[str, Any]:
        """Delete the index.
        """
        return self.http.delete(path=f"indexes/{self.index_name}")

    @staticmethod
    def create(config: Config, index_name: str,
               treat_urls_and_pointers_as_images=False,
               model=None,
               normalize_embeddings=True,
               sentences_per_chunk=2,
               sentence_overlap=0,
               image_preprocessing_method=None,
               settings_dict: dict = None
               ) -> Dict[str, Any]:
        """Create the index.

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
        Returns:
            Response body, containing information about index creation result
        """
        req = HttpRequests(config)

        if settings_dict is not None and settings_dict:
            return req.post(f"indexes/{index_name}", body=settings_dict)

        if config.api_key is not None:
            # making the keyword settings params override the default cloud
            #  settings
            cl_settings = defaults.get_cloud_default_index_settings()
            cl_ix_defaults = cl_settings['index_defaults']
            cl_ix_defaults['treat_urls_and_pointers_as_images'] = treat_urls_and_pointers_as_images
            cl_ix_defaults['model'] = model
            cl_ix_defaults['normalize_embeddings'] = normalize_embeddings
            cl_text_preprocessing = cl_ix_defaults['text_preprocessing']
            cl_text_preprocessing['split_overlap'] = sentence_overlap
            cl_text_preprocessing['split_length'] = sentences_per_chunk
            cl_img_preprocessing = cl_ix_defaults['image_preprocessing']
            cl_img_preprocessing['patch_method'] = image_preprocessing_method
            return req.post(f"indexes/{index_name}", body=cl_settings)

        return req.post(f"indexes/{index_name}", body={
            "index_defaults": {
                "treat_urls_and_pointers_as_images": treat_urls_and_pointers_as_images,
                "model": model,
                "normalize_embeddings": normalize_embeddings,
                "text_preprocessing": {
                    "split_overlap": sentence_overlap,
                    "split_length": sentences_per_chunk,
                    "split_method": "sentence"
                },
                "image_preprocessing": {
                    "patch_method": image_preprocessing_method
                }
            }
        })

    def refresh(self):
        """refreshes the index"""
        return self.http.post(path=F"indexes/{self.index_name}/refresh")

    def search(self, q: str, searchable_attributes: Optional[List[str]] = None,
               limit: int = 10, search_method: Union[SearchMethods.TENSOR, str] = SearchMethods.TENSOR,
               highlights=None, device: Optional[str] = None, filter_string: str = None,
               show_highlights=True, reranker=None,
               attributes_to_retrieve: Optional[List[str]] = None
               ) -> Dict[str, Any]:
        """Search the index.

        Args:
            q: string to search, or a pointer/url to an image if the index has
                treat_urls_and_pointers_as_images set to True
            searchable_attributes:  attributes to search
            limit: The max number of documents to be returned
            search_method: Indicates TENSOR or LEXICAL (keyword) search
            show_highlights: True if highlights are to be returned
            reranker:
            device: the device used to index the data. Examples include "cpu",
                "cuda" and "cuda:2". Overrides the Client's default device.
            filter_string: a filter string, used to prefilter documents during the
                search. For example: "car_colour:blue"
            attributes_to_retrieve: a list of document attributes to be
                retrieved. If left as None, then all attributes will be
                retrieved.

        Returns:
            Dictionary with hits and other metadata
        """

        start_time_client_request = timer()
        if highlights is not None:
            logging.warning("Deprecation warning for parameter 'highlights'. "
                            "Please use the 'showHighlights' instead. ")
            show_highlights = highlights if show_highlights is True else show_highlights

        selected_device = device if device is not None else self.config.search_device
        path_with_query_str = (
            f"indexes/{self.index_name}/search?"
            f"&device={utils.translate_device_string_for_url(selected_device)}"
        )
        body = {
            "q": q,
            "searchableAttributes": searchable_attributes,
            "limit": limit,
            "searchMethod": search_method,
            "showHighlights": show_highlights,
            "reRanker": reranker,
        }
        if attributes_to_retrieve is not None:
            body["attributesToRetrieve"] = attributes_to_retrieve
        if filter_string is not None:
            body["filter"] = filter_string
        res = self.http.post(
            path=path_with_query_str,
            body=body
        )
        
        num_results = len(res["hits"])
        end_time_client_request = timer()
        total_client_request_time = end_time_client_request - start_time_client_request
        
        search_time_log = f"search ({search_method.lower()}): took {(total_client_request_time):.3f}s to send query and received {num_results} results from Marqo (roundtrip)."
        if 'processingTimeMs' in res:
            search_time_log += f" Marqo itself took {(res['processingTimeMs'] * 0.001):.3f}s to execute the search."
        
        mq_logger.info(search_time_log)
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
        return self.http.get(url_string)

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
            body=document_ids
        )

    def add_documents(
        self,
        documents: List[Dict[str, Any]],
        auto_refresh=True,
        server_batch_size: int = None,
        client_batch_size: int = None,
        processes: int = None,
        device: str = None,
        non_tensor_fields: List[str] = []
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Add documents to this index. Does a partial update on existing documents,
        based on their ID. Adds unseen documents to the index.

        Args:
            documents: List of documents. Each document should be a dictionary.
            auto_refresh: Automatically refresh the index. If you are making
                lots of requests, it is advised to set this to False to
                increase performance.
            server_batch_size: if it is set, documents will be indexed into batches
                on the server as they are indexed. Otherwise documents are unbatched
                server-side.
            client_batch_size: if it is set, documents will be indexed into batches
                in the client, before being sent off. Otherwise documents are unbatched
                client-side.
            processes: number of processes for the server to use, to do indexing,
            device: the device used to index the data. Examples include "cpu",
                "cuda" and "cuda:2"
            non_tensor_fields: fields within documents to not create and store tensors against.

        Returns:
            Response body outlining indexing result
        """
        return self._generic_add_update_docs(
            update_method="replace",
            documents=documents, auto_refresh=auto_refresh, server_batch_size=server_batch_size,
            client_batch_size=client_batch_size, processes=processes, device=device, non_tensor_fields=non_tensor_fields
        )

    def update_documents(
        self,
        documents: List[Dict[str, Any]],
        auto_refresh=True,
        server_batch_size: int = None,
        client_batch_size: int = None,
        processes: int = None,
        device: str = None,
        non_tensor_fields: List[str] = []
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Add documents to this index. Does a partial updates on existing documents,
        based on their ID. Adds unseen documents to the index.

        Args:
            documents: List of documents. Each document should be a dictionary.
            auto_refresh: Automatically refresh the index. If you are making
                lots of requests, it is advised to turn this to false to
                increase performance.
            server_batch_size: if it is set, documents will be indexed into batches
                on the server as they are indexed. Otherwise documents are unbatched
                server-side.
            client_batch_size: if it is set, documents will be indexed into batches
                in the client, before being sent of. Otherwise documents are unbatched
                client-side.
            processes: number of processes for the server to use, to do indexing,
            device: the device used to index the data. Examples include "cpu",
                "cuda" and "cuda:2"
            non_tensor_fields: fields within documents to not create and store tensors against.

        Returns:
            Response body outlining indexing result
        """
        return self._generic_add_update_docs(
            update_method="update",
            documents=documents, auto_refresh=auto_refresh, server_batch_size=server_batch_size,
            client_batch_size=client_batch_size, processes=processes, device=device, non_tensor_fields=non_tensor_fields
        )

    def _generic_add_update_docs(
        self,
        update_method: str,
        documents: List[Dict[str, Any]],
        auto_refresh=True,
        server_batch_size: int = None,
        client_batch_size: int = None,
        processes: int = None,
        device: str = None,
        non_tensor_fields: List[str] = []
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        selected_device = device if device is not None else self.config.indexing_device
        num_docs = len(documents)

        # ADD DOCS TIMER-LOGGER (1)
        t0 = timer()
        start_time_client_process = timer()
        base_path = f"indexes/{self.index_name}/documents"
        non_tensor_fields_query_param = utils.convert_list_to_query_params("non_tensor_fields", non_tensor_fields)
        query_str_params = (
            f"{f'&device={utils.translate_device_string_for_url(selected_device)}'}"
            f"{f'&processes={processes}' if processes is not None else ''}"
            f"{f'&batch_size={server_batch_size}' if server_batch_size is not None else ''}"
            f"{f'&{non_tensor_fields_query_param}' if len(non_tensor_fields) > 0 else ''}"
        )
        end_time_client_process = timer()
        total_client_process_time = end_time_client_process - start_time_client_process
        mq_logger.info(f"add_documents pre-processing: took {(total_client_process_time):.3f}s for {num_docs} docs, for an average of {(total_client_process_time / num_docs):.3f}s per doc.")

        if client_batch_size is not None:
            if client_batch_size <= 0:
                raise errors.InvalidArgError("Batch size can't be less than 1!")
            res = self._batch_request(
                base_path=base_path, auto_refresh=auto_refresh,
                update_method=update_method, docs=documents, verbose=False,
                query_str_params=query_str_params, batch_size=client_batch_size
            )
            
        else:
            # no Client Batching
            refresh_option = f"?refresh={str(auto_refresh).lower()}"
            path_with_query_str = f"{base_path}{refresh_option}{query_str_params}"

            # ADD DOCS TIMER-LOGGER (2)
            start_time_client_request = timer()

            if update_method == 'update':
                res = self.http.put(path=path_with_query_str, body=documents)
            elif update_method == 'replace':
                res = self.http.post(path=path_with_query_str, body=documents)
            else:
                raise ValueError(f'Received unknown update_method `{update_method}`. '
                                 f'Allowed update_methods: ["replace", "update"] ')
            
            
            end_time_client_request = timer()
            total_client_request_time = end_time_client_request - start_time_client_request
            
            mq_logger.info(f"add_documents roundtrip: took {(total_client_request_time):.3f}s to send {num_docs} "
                            f"docs to Marqo (roundtrip, unbatched), for an average of {(total_client_request_time / num_docs):.3f}s per doc.")

            if server_batch_size is not None:
                # with Server Batching (show processing time for each batch)
                mq_logger.info(f"add_documents Marqo index (server-side batch reports): ")

                if processes is not None and processes > 1:
                    # for multiprocess, timing messages should be arranged by process, then batch
                    for process in range(len(res)):
                        mq_logger.info(f"   process {process}:")

                        for batch in range(len(res[process])):
                            server_batch_result_count = len(res[process][batch]["items"])
                            mq_logger.info(f"       marqo server batch {batch}: "
                                            f"processed {server_batch_result_count} docs in {(res[process][batch]['processingTimeMs'] / 1000):.3f}s, "
                                            f"for an average of {(res[process][batch]['processingTimeMs'] / (1000 * server_batch_result_count)):.3f}s per doc.")
                else:
                    # for single process, timing messages should be arranged by batch ONLY
                    for batch in range(len(res)):
                        server_batch_result_count = len(res[batch]["items"])
                        mq_logger.info(f"   marqo server batch {batch}: "
                                        f"processed {server_batch_result_count} docs in {(res[batch]['processingTimeMs'] / 1000):.3f}s, "
                                        f"for an average of {(res[batch]['processingTimeMs'] / (1000 * server_batch_result_count)):.3f}s per doc.")
            else:
                # no Server Batching
                if 'processingTimeMs' in res:       # Only outputs log if response is non-empty
                    mq_logger.info(f"add_documents Marqo index: took {(res['processingTimeMs'] / 1000):.3f}s for Marqo to process & index {num_docs} "
                                    f"docs (server unbatched), for an average of {(res['processingTimeMs'] / (1000 * num_docs)):.3f}s per doc.")

        total_add_docs_time = timer() - t0
        mq_logger.info(f"add_documents completed. total time taken: {(total_add_docs_time):.3f}s.")
        return res

    def delete_documents(self, ids: List[str], auto_refresh: bool = None) -> Dict[str, int]:
        """Delete documents from this index by a list of their ids.

        Args:
            ids: List of identifiers of documents.
            auto_refresh: if true refreshes the index

        Returns:
            A dict with information about the delete operation.
        """
        base_path = f"indexes/{self.index_name}/documents/delete-batch"
        path_with_refresh = base_path if auto_refresh is None else base_path + f"?refresh={str(auto_refresh).lower()}"

        return self.http.post(
            path=path_with_refresh, body=ids
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get stats about the index"""
        return self.http.get(path=f"indexes/{self.index_name}/stats")

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
            query_str_params: str,
            update_method: str, verbose: bool = True,
            auto_refresh: bool = True, batch_size: int = 50
    ) -> List[Dict[str, Any]]:
        """Batches a large chunk of documents to be sent as multiple
        add_documents invocations

        Args:
            docs: A list of documents
            batch_size: Size of a batch passed into a single add_documents
                call
            verbose: If true, prints out info about the documents

        Returns:
            A list of responses, which have information about the batch
            operation
        """
        path_with_query_str = f"{base_path}?refresh=false{query_str_params}"

        mq_logger.info(f"starting batch ingestion with batch size {batch_size}")

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
            t0 = timer()
            if update_method == 'replace':
                res = self.http.post(path=path_with_query_str, body=docs)
            elif update_method == 'update':
                res = self.http.put(path=path_with_query_str, body=docs)
            else:
                raise ValueError(f'Received unknown update_method `{update_method}`. '
                                 f'Allowed update_methods: ["replace", "update"] ')
            total_batch_time = timer() - t0
            num_docs = len(docs)
            mq_logger.info(
                f"   add_documents batch {i} roundtrip: took {(total_batch_time):.3f}s to add {num_docs} docs, "
                f"for an average of {(total_batch_time/num_docs):.3f}s per doc.")

            if isinstance(res, list):
                # with Server Batching (show processing time for each batch)

                if isinstance(res[0], list):
                    # for multiprocess, timing messages should be arranged by process, then batch
                    for process in range(len(res)):
                        mq_logger.info(f"       process {process}:")

                        for batch in range(len(res[process])):
                            server_batch_result_count = len(res[process][batch]["items"])
                            mq_logger.info(f"           marqo server batch {batch}: "
                                            f"processed {server_batch_result_count} docs in {(res[process][batch]['processingTimeMs'] / 1000):.3f}s, "
                                            f"for an average of {(res[process][batch]['processingTimeMs'] / (1000 * server_batch_result_count)):.3f}s per doc.")
                else:
                    # for single process, timing messages should be arranged by batch ONLY
                    for batch in range(len(res)):
                        server_batch_result_count = len(res[batch]["items"])
                        mq_logger.info(f"       marqo server batch {batch}: "
                                        f"processed {server_batch_result_count} docs in {(res[batch]['processingTimeMs'] / 1000):.3f}s, "
                                        f"for an average of {(res[batch]['processingTimeMs'] / (1000 * server_batch_result_count)):.3f}s per doc.")
            else:
                # no Server Batching
                if 'processingTimeMs' in res:       # Only outputs log if response is non-empty
                    mq_logger.info(f"   add_documents batch {i} Marqo processing: took {(res['processingTimeMs'] / 1000):.3f}s for Marqo to process & index {num_docs} "
                                    f"docs (server unbatched), for an average of {(res['processingTimeMs'] / (1000 * num_docs)):.3f}s per doc.")


            if verbose:
                mq_logger.info(f"results from indexing batch {i}: {res}")
            return res

        results = [verbosely_add_docs(i, docs) for i, docs in enumerate(batched)]
        if auto_refresh:
            self.refresh()
        mq_logger.info('completed batch ingestion.')
        return results

