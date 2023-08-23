"""
Before running the tests locally, ensure that you have a running Marqo instance to test against!
Pass its settings to the local_marqo_settings.

To run Cloud V2 tests, execute `tox -e cloud_tests`.
When running Cloud V2 tests, make sure to set the following environment variables:
- MARQO_CLOUD_URL: The URL that the config class uses to recognize the cloud environment.
- MARQO_API_KEY: The API key used for authentication.
- MARQO_URL: The URL of the Marqo cloud instance.

When `cluster.is_marqo_cloud` is set to True, the tests will have different setUp and tearDown procedures:
- Indices will not be deleted after each test.
- However, documents will be deleted on every call for index.

The function `prepare_cloud_index_for_test` will be triggered whenever an index is called for the cloud tests.
It performs cleanup for index documents and mocks the 'add_documents' method to call
add_documents_and_mark_for_cleanup_patch instead which stores list of added documents,
for future cleanup of documents.

We will not actually create and delete real cloud indexes
during this test suite, because this slows down py-marqo <> Marqo cloud tests.
We should still test these methods with mocking. End-to-end
creation and deletion tests will be done elsewhere.
"""

import logging
import time
from collections import defaultdict
from functools import wraps
import json
import os
from random import choice
from string import ascii_letters

from pydantic import BaseModel
from typing import Any, Callable, Dict, List, Optional, Union
from unittest import mock, TestCase
from tests.cloud_tests.cloud_test_index import CloudTestIndex

import marqo
from marqo._httprequests import HTTP_OPERATIONS
from marqo.client import Client
from marqo.errors import InternalError, MarqoWebError, MarqoError
from marqo.cloud_helpers import cloud_wait_for_index_status
from marqo.index import Index


class MockHTTPTraffic(BaseModel):
    # Request parameters
    http_operation: HTTP_OPERATIONS
    path: str
    body: Optional[Any] = None
    content_type: Optional[str] = None

    # Verification parameters
    response: Optional[Union[Any, InternalError]]
    expected_calls: Optional[int] = None

    class Config:
        arbitrary_types_allowed: bool = True

    def __str__(self):
        return f"MockHTTPTraffic({json.dumps(self.dict(), indent=2)})"

def raise_(ex):
    raise ex

def mock_http_traffic(mock_config: List[MockHTTPTraffic], forbid_extra_calls: bool = False):
    """Function decorator to mock HTTP traffic to the Marqo instance. Can either return a dictionary, or raise an Exception."""
    def decorator(test_func):
        @wraps(test_func)
        def wrapper(self, *args, **kwargs):
            call_count = defaultdict(int)  # Used to ensure expected_calls for each MockHTTPTraffic

            with mock.patch("marqo._httprequests.HttpRequests.send_request") as mock_send_request:
                def side_effect(http_operation, path, body=None, content_type=None, index_name=""):
                    if isinstance(body, str):
                        body = json.loads(body)
                    for i, config in enumerate(mock_config):
                        if http_operation == config.http_operation and path == config.path and body == config.body and content_type == config.content_type:
                            response = config.response if config.response else {}
                            call_count[i] += 1

                            if isinstance(response, InternalError):
                                raise response
                            return response

                    if forbid_extra_calls:
                        raise ValueError(
                            f"Unexpected HTTP call to Marqo. Request (\n"
                            f"  http_operation={http_operation},\n"
                            f"  path={path},\n"
                            f"  body={body},\n"
                            f"  content_type={content_type}\n"
                        )
                mock_send_request.side_effect = side_effect

                test_func(self, *args, **kwargs)
            
                for i, config in enumerate(mock_config):
                    if config.expected_calls is not None and call_count[i] != config.expected_calls:
                        raise ValueError(f"Expected config to be called {config.expected_calls} times, but it was called {call_count[i]} times, for config: {config}")

        return wrapper
    return decorator


def with_documents(index_to_documents_fn: Callable[[], Dict[str, List[Dict[str, Any]]]], warmup_query: Optional[str] = None):
    """Function decorator to create indices and add documents to them.
    Created index has default settings and CloudTestIndex.basic_index is used during cloud tests."""
    def decorator(test_func):
        @wraps(test_func)
        def wrapper(self, *args, **kwargs):
            index_to_documents = index_to_documents_fn(self)
            new_args = list(args)
            for index_name, docs in index_to_documents.items():
                if len(docs) == 0:
                    continue
                self.create_test_index(
                    cloud_test_index_to_use=CloudTestIndex.basic_index,
                    open_source_test_index_name=index_name
                )
                self.client.index(index_name).add_documents(docs, non_tensor_fields=[])
                if self.IS_MULTI_INSTANCE:
                    self.warm_request(self.client.bulk_search, [{
                        "index": index_name,
                        "q": list(docs[0].values())[0] if not warmup_query else warmup_query
                    }])
                new_args.append(docs)
            return test_func(self, *new_args, **kwargs)
        return wrapper
    return decorator


class MarqoTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        local_marqo_settings = {
            "url": os.environ.get("MARQO_URL", 'http://localhost:8882'),
        }
        api_key = os.environ.get("MARQO_API_KEY", None)
        if (api_key):
            local_marqo_settings["api_key"] = api_key
        cls.index_suffix = os.environ.get("MQ_TEST_RUN_IDENTIFIER", "")
        cls.client_settings = local_marqo_settings
        cls.authorized_url = cls.client_settings["url"]
        cls.index_to_documents_cleanup_mapping = {}
        cls.generic_test_index_name = 'test-index'  # used as a prefix when index is created with settings
        cls.generic_test_index_name_2 = cls.generic_test_index_name + '-2'

        # class property to indicate if test is being run on multi
        cls.IS_MULTI_INSTANCE = (True if os.environ.get("IS_MULTI_INSTANCE", False) in ["True", "TRUE", "true", True] else False)

    @classmethod
    def tearDownClass(cls) -> None:
        """Delete commonly used test indexes after all tests are run
        """
        client = marqo.Client(**cls.client_settings)
        for index in client.get_indexes()['results']:
            if index.index_name.startswith(cls.generic_test_index_name):
                if not client.config.is_marqo_cloud:
                    try:
                        index.delete()
                    except marqo.errors.MarqoApiError as e:
                        logging.debug(f'received error `{e}` from index deletion request.')

    def setUp(self) -> None:
        self.client = Client(**self.client_settings)
        if not self.client.config.is_marqo_cloud:
            for index in self.client.get_indexes()['results']:
                if index.index_name.startswith(self.generic_test_index_name):
                    try:
                        index.delete()
                    except marqo.errors.MarqoApiError as e:
                        logging.debug(f'received error `{e}` from index deletion request.')

    def tearDown(self) -> None:
        if self.client.config.is_marqo_cloud:
            if hasattr(self, 'add_documents_and_mark_for_cleanup_patch'):
                self.add_documents_and_mark_for_cleanup_patch.stop()
        else:
            for index in self.client.get_indexes()['results']:
                if index.index_name.startswith(self.generic_test_index_name):
                    try:
                        index.delete()
                    except marqo.errors.MarqoApiError as e:
                        logging.debug(f'received error `{e}` from index deletion request.')

    def warm_request(self, func, *args, **kwargs):
        '''
        Takes in a function object, func, and executes the function 5 times to warm search results.
        Any arguments passed to args and kwargs are passed as arguments to the function.
        This solves the occurence of tests failing due to eventual consistency implemented in marqo cloud.
        '''
        for i in range(5):
            func(*args, **kwargs)

    def create_test_index(
            self,
            cloud_test_index_to_use: CloudTestIndex, open_source_test_index_name: str,
            open_source_index_settings: dict = None, open_source_index_kwargs: dict = None,
    ):
        """Determines whether the test is executed in a cloud environment or within an open-source setup.
        If it's running in an open-source environment,
        this function creates the corresponding index and returns its name.

        In the case of cloud testing, it provides the name of the cloud index to be used.
        Additionally, it applies a unique run identifier to the index name and performs
        cleanup operations on documents associated with the index.

        Returns:
            The name of the index to be used, depending on the testing environment.

        """
        client = marqo.Client(**self.client_settings)
        if client.config.is_marqo_cloud:
            if cloud_test_index_to_use is None:
                raise ValueError("cloud_test_index_to_use must be specified for cloud tests")
            index_name_to_return = f"{cloud_test_index_to_use.value}-{self.index_suffix}"
            self.prepare_cloud_index_for_test(index_name_to_return)
        else:
            index_name_to_return = self.create_open_source_index(
                open_source_test_index_name, open_source_index_settings, open_source_index_kwargs
            )
        return index_name_to_return

    def prepare_cloud_index_for_test(self, index_name: str):
        """
        Cleans up documents from the specified cloud index and prepares it for testing.

        This method first cleans up any existing documents in the provided 'index_name'.
        It then ensures that the 'add_documents' method of the Marqo index is correctly mocked
        to perform it with proper handling by adding _ids of documents
        to the self.index_to_documents_cleanup_mapping.

        Also checks for existing patch and stops it if it exists.

        Args:
            index_name (str): The name of the cloud index to prepare for testing.

        """
        self.cleanup_documents_from_index(index_name)
        if not isinstance(marqo.index.Index.add_documents, mock.MagicMock):
            if hasattr(self, 'add_documents_and_mark_for_cleanup_patch'):
                self.add_documents_and_mark_for_cleanup_patch.stop()
            self.add_documents_and_mark_for_cleanup_patch = mock.patch.object(
                Index, 'add_documents', side_effect=lambda documents, **kwargs:
                self.mark_for_cleanup_and_add_documents(index_name, documents, **kwargs)
            )
            self.add_documents_and_mark_for_cleanup_patch.start()

    def create_open_source_index(self, index_name: str, settings_dict: dict = None, kwargs: dict = None):
        """Create an open source index with the given name and settings."""
        client = marqo.Client(**self.client_settings)
        if settings_dict and kwargs:
            raise ValueError("Only one of settings_dict and kwargs can be specified")
        if settings_dict:
            client.create_index(index_name, settings_dict=settings_dict)
        elif kwargs:
            client.create_index(index_name, **kwargs)
        else:
            client.create_index(index_name)
        return index_name

    def mark_for_cleanup_and_add_documents(self, index_name: str, documents: list, *args,   **kwargs):
        """Add documents to index and mark for cleanup after test is run."""
        self.add_documents_and_mark_for_cleanup_patch.stop()
        res = self.client.index(index_name).add_documents(documents, *args, **kwargs)
        self.add_documents_and_mark_for_cleanup_patch.start()
        res_list = [res] if type(res) is not list else res
        for result_to_process in res_list:
            if not result_to_process['errors']:
                if self.index_to_documents_cleanup_mapping.get(index_name) is None:
                    self.index_to_documents_cleanup_mapping[index_name] = set([doc['_id'] for doc in result_to_process['items']])
                else:
                    self.index_to_documents_cleanup_mapping[index_name].update([doc['_id'] for doc in result_to_process['items']])
        return res

    def cleanup_documents_from_index(self, index_to_cleanup: str):
        """"This is used for cloud tests only.
        Delete all documents from specified index.
        """
        idx = self.client.index(index_to_cleanup)
        print(f"Deleting documents from index {idx.index_name}")
        try:
            idx.refresh()
            # verifying that index is in the mapping
            self.client.config.instance_mapping.get_index_base_url(idx.index_name)

            if self.index_to_documents_cleanup_mapping.get(index_to_cleanup):
                res = idx.delete_documents(list(self.index_to_documents_cleanup_mapping[index_to_cleanup]),
                                           auto_refresh=True)
                if res['status'] == 'succeeded':
                    self.index_to_documents_cleanup_mapping[index_to_cleanup] = set()

            if idx.get_stats()["numberOfDocuments"] > 0:
                max_attempts = 100
                attempt = 0
                q = ""
                docs_to_delete = [i['_id'] for i in idx.search(q, limit=100)['hits']]
                while idx.get_stats()["numberOfDocuments"] > 0 or docs_to_delete:
                    docs_to_delete = [i['_id'] for i in idx.search(q, limit=100)['hits']]
                    if docs_to_delete:
                        verified_run = 0
                        idx.delete_documents(docs_to_delete, auto_refresh=True)
                    q = ''.join(choice(ascii_letters) for _ in range(8))
                    attempt += 1
                    if attempt > max_attempts:
                        raise MarqoError(f"Max attempts reached. Failed to delete documents from index {idx.index_name}")
        except MarqoError as e:
            print(f"Error deleting documents from index {idx.index_name}: {e}")
