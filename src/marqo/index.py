import functools
import json
import logging
from urllib import parse
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Union
from marqo._httprequests import HttpRequests
from marqo.config import Config
from marqo.marqo_logging import logger
from marqo.enums import SearchMethods
from marqo import errors

# pylint: disable=too-many-public-methods
class Index():
    """
    Indexes routes wrapper.

    Index class gives access to all indexes routes and child routes (inherited).
    https://docs.marqo.com/reference/api/indexes.html
    """

    def __init__(
        self,
        config: Config,
        index_name: str,
        primary_key: Optional[str] = None,
        created_at: Optional[Union[datetime, str]] = None,
        updated_at: Optional[Union[datetime, str]] = None,
    ) -> None:
        """
        Parameters
        ----------
        config:
            Config object containing permission and location of marqo.
        index_name:
            UID of the index on which to perform the index actions.
        primary_key:
            Primary-key of the index.
        """
        self.config = config
        self.http = HttpRequests(config)
        self.index_name = index_name
        self.primary_key = primary_key
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

        Returns:
            Response body, containing information about index creation result
        """
        req = HttpRequests(config)
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
                "image_preprocessing":{
                    "patch_method": image_preprocessing_method
                }
            }
        })

    def refresh(self):
        """refreshes the index"""
        # might need to rename to hit Marqo or add if not it does exist
        return self.http.post(path=F"{self.index_name}/_refresh")

    def search(self, q: str, searchable_attributes: Optional[List[str]]=None,
               limit: int=10, search_method: Union[SearchMethods.NEURAL, str] = SearchMethods.NEURAL,
               highlights=True, reranker=None
               ) -> Dict[str, Any]:
        """Search in the index.

        Parameters
        ----------
        query:
            String containing the searched word(s)
        opt_params (optional):
            Dictionary containing optional query parameters
            https://docs.marqo.com/reference/api/search.html#search-in-an-index
        searchable_attributes: a subset of attributes to search through

        Returns
        -------
        results:
            Dictionary with hits, offset, limit, processingTime and initial query

        Raises
        ------
        s2SearchApiError
            An error containing details about why marqo can't process your request. marqo error codes are described here: https://docs.marqo.com/errors/#marqo-errors
        """
        return self.http.post(path=f"indexes/{self.index_name}/search")
        return neural_search.search(
            config=self.config, index_name=self.index_name, text=q, return_doc_ids=True,
            searchable_attributes=searchable_attributes, search_method=search_method, result_count=limit,
            highlights=highlights, reranker=reranker)

    def get_document(self, document_id: Union[str, int]) -> Dict[str, Any]:
        """Get one document with given document identifier.

        Args:
            document_id: Unique identifier of the document.

        Returns:
            Dictionary containing the documents information.
        """
        return self.http.get(f"indexes/{self.index_name}/documents/{document_id}")

    def add_documents(
        self,
        documents: List[Dict[str, Any]],
        auto_refresh=True,
        batch_size: int = None,
        use_parallel: bool = False,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Add documents to a Marqo index

        Args:
            documents: List of documents. Each document should be a dictionary.
            auto_refresh: Automatically refresh the index. If you are making
                lots of requests, it is advised to turn this to false to
                increase performance.
            batch_size: if it is set, documents will be indexed into batches
                of this size. Otherwise documents are unbatched.
            use_parallel:

        Returns:
            Response body outlining indexing result
        """
        if batch_size is None:
            return self.http.post(path=f"indexes/{self.index_name}/documents?refresh={auto_refresh}", body=documents)
        elif use_parallel:
            raise NotImplementedError("Parallel add docs not yet available, from the Cclient!")
        else:
            if batch_size <= 0:
                raise errors.InvalidArgError("Batch size can't be less than 1!")
            return self._batch_request(docs=documents, batch_size=batch_size, verbose=False)

    def delete_documents(self, ids: List[str], auto_refresh: bool = None) -> Dict[str, int]:
        """

        Args:
            ids: List of unique identifiers of documents.
            auto_refresh: if true refreshes the index

        Returns:

        """
        base_path = f"indexes/{self.index_name}/documents/delete-batch"
        path_with_refresh = base_path if auto_refresh is None else base_path + f"?refresh={auto_refresh}"

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

    def _batch_request(self, docs: List[Dict], batch_size: int = 100, verbose: bool=True) -> List[Dict[str, Any]]:
        """Batches a large chunk of documents to be sent as multiple
        add_documents invocations

        Args:
            docs: A list of documents
            batch_size: Size of a batch passed into a single add_documents
                call
            verbose: If true, prints out info about the documents

        Returns:

        """
        logger.info(f"starting batch ingestion in sizes of {batch_size}")

        deeper = ((doc, i, batch_size) for i, doc in enumerate(docs))

        def batch_requests(gathered, doc_tuple):
            doc, i, the_batch_size = doc_tuple
            if i % the_batch_size == 0:
                gathered.append([doc,])
            else:
                gathered[-1].append(doc)
            return gathered

        batched = functools.reduce(lambda x, y: batch_requests(x, y), deeper, [])

        def verbosely_add_docs(i, docs):
            t0 = datetime.now()
            res = self.http.post(path=f"indexes/{self.index_name}/documents?refresh=false", body=docs)
            total_batch_time = datetime.now() - t0
            num_docs = len(docs)

            logger.info(f"    batch {i}: ingested {num_docs} docs. Time taken: {total_batch_time}. "
                        f"Average timer per doc {total_batch_time/num_docs}")
            if verbose:
                logger.info(f"        results from indexing batch {i}: {res}")
            return res

        results = [verbosely_add_docs(i, docs) for i, docs in enumerate(batched)]
        logger.info('completed batch ingestion.')
        return results

