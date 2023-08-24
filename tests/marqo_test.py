"""
Before running the tests locally, ensure that you have a running Marqo instance to test against (either a cloud account
or a locally running Marqo instance)!

Pass its settings to the local_marqo_settings.

# RUNNING OPEN SOURCE TESTS

1. Have a Marqo instance running (please see Marqo README)
2. Run the following command in the repo root directory:
tox


# RUNNING CLOUD TESTS

  To run Cloud V2 tests, execute `tox -e cloud_tests`.
  When running Cloud V2 tests, make sure you export the following environment variables:
    - MARQO_CLOUD_URL: The URL that the config class uses to recognize the cloud environment.
        For example: https://api.marqo.ai
    - MARQO_API_KEY: The API key used for authentication.
    - MARQO_URL: The URL of the Marqo cloud instance. This should be the same as: MARQO_CLOUD_URL.
        For example: https://api.marqo.ai

  Examples (running cloud tests)
    # To run all tests and have them run on new cloud indexes, if they don't yet exist, and have them persist
    # even after tests are done:
    tox -e cloud_tests -- create-indexes=True

    # To also delete the indexes after testing has complete:
    tox -e cloud_tests -- create-indexes=True delete-indexes=True

    # if the cloud indexes for testing already exist:
    tox -e cloud_tests

    # to run tests on cloud indexes that contain your own identifier do the following. This is
    # useful if multiple users are doing these tests on the same Marqo cloud account
    export MQ_TEST_RUN_IDENTIFIER=danyil
    tox -e cloud_tests -- create-indexes=True

  Examples (deleting existing cloud test indexes)

    # To delete all cloud test indexes on the cloud account
    python3 tests/cloud_tests/delete_all_cloud_test_indexes.py

    # To delete all cloud test indexes specified by a certain test run identifier:
    export MQ_TEST_RUN_IDENTIFIER=danyil
    tests/cloud_tests/delete_all_cloud_test_indexes.py

  For detailed information on available cloud test parameters and their functionality,
  please refer to the docstring in 'tests/cloud_tests/run_cloud_test.py'.


## NOTES ABOUT THE TEST SUITE BEHAVIOUR ##

When `cluster.is_marqo_cloud` is set to True, the tests will have different setUp and tearDown procedures:
- Indices will not be deleted after each test.
- However, documents will be deleted on every call for index.

The function prepare_cloud_index_for_test will be triggered whenever an index is called for the cloud tests.
It performs cleanup for index documents and mocks the 'add_documents' method to call
add_documents_and_mark_for_cleanup_patch instead, which acts as as a decorator for the original method.
This method stores a list of added documents for future cleanup of documents, and then performs the real request.

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

import requests
from pydantic import BaseModel
from typing import Any, Callable, Dict, List, Optional, Union, NamedTuple
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
                        # performs check for get indexes which needs to pass for mappings during cloud tests
                        if http_operation == "get" and path == "" and body is None and content_type is None:
                            return

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
    The created index has default settings. For cloud tests, CloudTestIndex.basic_index is used."""
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


class InstanceMappingIndexData(NamedTuple):
    index_name: str
    index_status: str
    endpoint: str

    @staticmethod
    def get_basic_index_data():
        return InstanceMappingIndexData(
            index_name="test-index",
            index_status="READY",
            endpoint="endpoint"
        )


def mock_instance_mappings(indexes_list: Union[List[InstanceMappingIndexData], None], to_return_mock: bool = False):
    """Function decorator to mock the instance mappings endpoint.

    This decorator is used to mock the behavior of the requests.get function
    within a specific test function. It allows you to set up mock responses for
    requests to the "/indexes" URL while allowing real requests to other URLs.

    Args:
        indexes_list (Union[List[InstanceMappingIndexData], None]): A list of
            InstanceMappingIndexData objects to be used for mocking responses
            to requests to the "/indexes" URL. If None, no mock data will be used.
        to_return_mock (bool): Indicates whether the decorated test function
            should return the mock object or not. If True, the test function
            will be called with the mock object as an argument; otherwise, it
            will be called without the mock object.

    Returns:
        function: The decorated test function.

    Example usage:
        @mock_instance_mappings([...])  # Pass your mock data here
        def test_example(mock_get):
            # Your test logic here

    """
    def decorator(test_func):
        @wraps(test_func)
        def wrapper(self, *args, **kwargs):
            with mock.patch("marqo.marqo_cloud_instance_mappings.requests.get") as mock_get:
                return_value_is_set = False

                def side_effect(url, *args, **kwargs):
                    nonlocal return_value_is_set
                    if url.endswith("/indexes"):
                        if not return_value_is_set and instance_mappings is not None:
                            mock_get.json.return_value = instance_mappings
                            return_value_is_set = True

                        return mock_get
                    else:
                        requests.get(url, *args, **kwargs)

                if indexes_list is not None:
                    instance_mappings = {"results": [index_data._asdict() for index_data in indexes_list]}
                else:
                    instance_mappings = None
                mock_get.side_effect = side_effect
                if not to_return_mock:
                    test_func(self, *args, **kwargs)
                else:
                    return test_func(self, mock_get, *args, **kwargs)
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
                else:
                    if index.index_name.endswith(cls.index_suffix):
                        cls.cleanup_documents_from_index(cls, index.index_name)


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
            cloud_test_index_to_use: Union[CloudTestIndex, None], open_source_test_index_name: Union[str, None],
            open_source_index_settings_dict: dict = None, open_source_index_kwargs: dict = None,
            delete_index_documents_before_test: bool = True
    ):
        """Determines whether the test is executed in a cloud environment or within an open-source setup.
        If it's running in an open-source environment,
        this function creates the corresponding index and returns its name.

        In the case of cloud testing, it provides the name of the cloud index to be used.
        Additionally, it applies a unique run identifier to the index name and performs
        cleanup operations on documents associated with the index if 'delete_index_documents_before_test' is True.

        Args:
            cloud_test_index_to_use (Union[CloudTestIndex, None]): The cloud test index to use in cloud environments.
                If None, an error is raised.
            open_source_test_index_name (Union[str, None]): The name of the open-source test index to create in
                open-source environments. If None, no open-source index is created.
            open_source_index_settings_dict (dict, optional): Additional settings to apply when creating
                the open-source test index.
            open_source_index_kwargs (dict, optional): Additional keyword arguments to pass when creating
                the open-source test index.
            delete_index_documents_before_test (bool, optional): If True, existing documents in the index will
                be deleted before preparing it for testing. Default is True. Used only for cloud testing.

        Returns:
            The name of the index to be used, depending on the testing environment.

        Raises:
            ValueError: If 'cloud_test_index_to_use' is None in cloud environments.

        """
        client = marqo.Client(**self.client_settings)
        if client.config.is_marqo_cloud:
            if cloud_test_index_to_use is None:
                raise ValueError("cloud_test_index_to_use must be specified for cloud tests")
            index_name_to_return = f"{cloud_test_index_to_use.value}-{self.index_suffix}"
            self.prepare_cloud_index_for_test(index_name_to_return, delete_index_documents_before_test)
        else:
            index_name_to_return = self.create_open_source_index(
                open_source_test_index_name, open_source_index_settings_dict, open_source_index_kwargs
            )
        return index_name_to_return

    def prepare_cloud_index_for_test(self, index_name: str, delete_index_documents_before_test: bool = True):
        """
        Cleans up documents from the specified cloud index and prepares it for testing.

        This method first cleans up any existing documents in the provided 'index_name'.
        It then ensures that the 'add_documents' method of the Marqo index is correctly mocked
        to perform it with proper handling by adding _ids of documents
        to the self.index_to_documents_cleanup_mapping.
        Those _ids will then be used by the 'cleanup_documents_from_index' method
        to determine what documents to delete from the index to make sure it's ready for test.

        Also checks for existing patch and stops it if it exists to avoid patching twice,
        which might lead to unexpected issues.

        Args:
            index_name (str): The name of the cloud index to prepare for testing.

        """
        if delete_index_documents_before_test:
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
        """Create an open source index with the given name and settings.

           Note:
            If the index creation fails due to a MarqoWebError, the error message will be
            printed, but it will not raise an exception. This behavior is designed to avoid
            test failures when the index already exists."""
        client = marqo.Client(**self.client_settings)
        if settings_dict is not None and kwargs is not None:
            raise ValueError("Only one of settings_dict and kwargs can be specified")
        try:
            if settings_dict is not None:
                client.create_index(index_name, settings_dict=settings_dict)
            elif kwargs is not None:
                client.create_index(index_name, **kwargs)
            else:
                client.create_index(index_name)
        except MarqoWebError as e:
            # we don't want to fail the test if the index already exists as it might be used in some scenarios
            print(f"Index creation failed with error: {e}")
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
        client = marqo.Client(**self.client_settings)
        idx = client.index(index_to_cleanup)
        print(f"Deleting documents from index {idx.index_name}")
        try:
            idx.refresh()
            # verifying that index is in the mapping
            client.config.instance_mapping.get_index_base_url(idx.index_name)

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
                        idx.delete_documents(docs_to_delete, auto_refresh=True)
                    q = ''.join(choice(ascii_letters) for _ in range(8))
                    attempt += 1
                    if attempt > max_attempts:
                        raise MarqoError(f"Max attempts reached. Failed to delete documents from index {idx.index_name}")
        except MarqoError as e:
            print(f"Error deleting documents from index {idx.index_name}: {e}")
