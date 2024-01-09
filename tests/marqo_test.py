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
    python3 tests/cloud_test_logic/delete_all_cloud_test_indexes.py

    # To delete all cloud test indexes specified by a certain test run identifier:
    export MQ_TEST_RUN_IDENTIFIER=danyil
    tests/cloud_test_logic/delete_all_cloud_test_indexes.py

  For detailed information on available cloud test parameters and their functionality,
  please refer to the docstring in 'tests/cloud_test_logic/run_cloud_test.py'.


## NOTES ABOUT THE TEST SUITE BEHAVIOUR ##

The index creation and deletion is handled differently for cloud and open-source tests.


1) Cloud Environment:
When `cluster.is_marqo_cloud` is set to True:
- All the indexes will be created at the start of the test run, in file
marqo.tests.populate_indices_for_cloud_tests.py
- Indices will not be deleted until the end of the workflow
- However, documents will be deleted every time you call prepare_cloud_index_for_test.
The function prepare_cloud_index_for_test will be triggered whenever an index is called for the cloud tests.
It performs cleanup for index documents and mocks the 'add_documents' method to call
add_documents_and_mark_for_cleanup_patch instead, which acts as a decorator for the original method.
This method stores a list of added documents for future cleanup of documents, and then performs the real request.

2) Open-Source Environment:
When `cluster.is_marqo_cloud` is set to False:
- All the indexes will be created in the setUpClass method of the test suite. We should only use the
created indexes in a test class.
- Indices will not be deleted after each test method, but will be cleared in the tearDownClass method of the test suite.
- Documents will be deleted before every test method runs in, via the test class' setUp() method.
"""

import logging
from collections import defaultdict
from functools import wraps
import json
import requests
import os
from random import choice
from string import ascii_letters

from pydantic import BaseModel
from typing import Any, Callable, Dict, List, Optional, Union
from unittest import mock, TestCase

from tests.cloud_test_logic.cloud_test_index import CloudTestIndex

import marqo
from marqo._httprequests import HTTP_OPERATIONS
from marqo.client import Client
from marqo.errors import InternalError, MarqoWebError, MarqoError
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
                self.client.index(index_name).add_documents(docs, non_tensor_fields=[], auto_refresh=True)
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

    # indexes in the list will be cleared in setUp and deleted in tearDownClass
    open_source_indexes_list: List[str] = []

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
        cls.client=marqo.Client(**cls.client_settings)
        cls.generic_test_index_name = "py_marqo_test_index"
        cls.unstructured_index_name = "unstructured_index"
        cls.structured_index_name = "structured_index"
        # TODO: include structured when boolean_field bug for structured is fixed
        cls.test_cases = [
            (CloudTestIndex.unstructured_image, cls.unstructured_index_name),
        ]

        # class property to indicate if test is being run on multi
        cls.IS_MULTI_INSTANCE = (True if os.environ.get("IS_MULTI_INSTANCE", False) in ["True", "TRUE", "true", True] else False)

    @classmethod
    def tearDownClass(cls) -> None:
        """Delete commonly used test indexes after all tests are run
        """
        if cls.client.config.is_marqo_cloud:
            for index in cls.client.get_indexes()['results']:
                if index["indexName"].endswith(cls.index_suffix):
                    cls.cleanup_documents_from_index(cls, index["indexName"])
        else:
            if cls.open_source_indexes_list:
                cls.delete_open_source_indexes(cls.open_source_indexes_list)

    def setUp(self) -> None:
        """We only clear indexes in open-source tests here as cloud tests indexes
        are cleared in prepare_cloud_index_for_test"""
        if not self.client.config.is_marqo_cloud and self.open_source_indexes_list:
            self.clear_open_source_indexes(self.open_source_indexes_list)

    def tearDown(self) -> None:
        if self.client.config.is_marqo_cloud:
            if hasattr(self, 'add_documents_and_mark_for_cleanup_patch'):
                self.add_documents_and_mark_for_cleanup_patch.stop()

    def warm_request(self, func, *args, **kwargs):
        '''
        Takes in a function object, func, and executes the function 5 times to warm search results.
        Any arguments passed to args and kwargs are passed as arguments to the function.
        This solves the occurence of tests failing due to eventual consistency implemented in marqo cloud.
        '''
        for i in range(5):
            func(*args, **kwargs)

    def get_test_index_name(
            self,
            cloud_test_index_to_use: Union[CloudTestIndex, None],
            open_source_test_index_name: Union[str, None],
            delete_index_documents_before_test: bool = True
    ):
        """
        Gets the name of an index that is used in a given unittest test case, based on whether the test is
        running in a cloud environment. The names of indexes on the cloud and in open source are slightly different.

        In the case of cloud testing, it provides the name of the cloud index to be used.
        Additionally, it applies a unique run identifier to the index name and performs
        cleanup operations on documents associated with the index if 'delete_index_documents_before_test' is True.

        In the case of open-source testing, it will return the open-source index name to be used. The documents
        in the index are cleaned by 'setUp' function of the test case.

        Note: Index creation for both cloud and open-source environments is NOT included in this function.
        In the case of cloud testing, the index created at the start of py-marqo test.
        In the case of open-source testing, the index is created in the 'setUpClass' method of the test case.

        Args:
            cloud_test_index_to_use (Union[CloudTestIndex, None]): The cloud test index to use in cloud environments.
                If None, an error is raised.
            open_source_test_index_name (Union[str, None]): The name of the open-source test index to create in
                open-source environments. If None, no open-source index is created.
            delete_index_documents_before_test (bool, optional): If True, existing documents in the index will
                be deleted before preparing it for testing. Default is True. Used only for cloud testing.

        Returns:
            The name of the index to be used, depending on the testing environment.

        Raises:
            ValueError: If 'cloud_test_index_to_use' is None in cloud environments.

        Notes:
            - 'delete_index_documents_before_test' is set to True by default in cloud testing to ensure a clean
              environment. However, in some scenarios, such as when using the with_documents decorator,
              it may be desirable to set it to False. This allows existing documents to persist when calling
              'create_test_index' multiple times in the same test.
        """
        client = marqo.Client(**self.client_settings)
        if client.config.is_marqo_cloud:
            if cloud_test_index_to_use is None:
                raise ValueError("cloud_test_index_to_use must be specified for cloud tests")
            index_name_to_return = f"{cloud_test_index_to_use.value}_{self.index_suffix}"
            self.prepare_cloud_index_for_test(index_name_to_return, delete_index_documents_before_test)
        else:
            index_name_to_return = open_source_test_index_name
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

    @classmethod
    def create_open_source_indexes(cls, index_settings_with_name: List[Dict]):
        """A function to call the internal Marqo API to create a batch of indexes.
         Use camelCase for the keys.

         Indexes created by this function will be added to cls.open_source_indexes_list, thus will be deleted
         in tearDownClass, cleared in setUp.
         """
        if cls.client.config.is_marqo_cloud:
            raise MarqoError("create_open_source_indexes is not supported in cloud environments")

        r = requests.post(f"{cls.authorized_url}/batch/indexes/create", data=json.dumps(index_settings_with_name))
        cls.open_source_indexes_list = [index['indexName'] for index in index_settings_with_name]
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise MarqoWebError(e)

    @classmethod
    def delete_open_source_indexes(cls, index_names: List[str]):
        if cls.client.config.is_marqo_cloud:
            raise MarqoError("delete_open_source_indexes is not supported in cloud environments")
        r = requests.post(f"{cls.authorized_url}/batch/indexes/delete", data=json.dumps(index_names))
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise MarqoWebError(e)

    @classmethod
    def clear_open_source_indexes(cls, index_names: List[str]):
        if cls.client.config.is_marqo_cloud:
            raise MarqoError("clear_open_source_indexes is not supported in cloud environments")
        for index_name in index_names:
            r = requests.delete(f"{cls.authorized_url}/indexes/{index_name}/documents/delete-all")
            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError as e:
                raise MarqoWebError(e)

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
            # verifying that index is in the mapping
            client.config.instance_mapping.get_index_base_url(idx.index_name)

            if self.index_to_documents_cleanup_mapping.get(index_to_cleanup):
                res = idx.delete_documents(list(self.index_to_documents_cleanup_mapping[index_to_cleanup]))
                if res['status'] == 'succeeded':
                    self.index_to_documents_cleanup_mapping[index_to_cleanup] = set()

            if idx.get_stats()["numberOfDocuments"] > 0:
                max_attempts = 100
                attempt = 0
                q = ""
                docs_to_delete = [i['_id'] for i in idx.search(q, limit=100)['hits']]
                while idx.get_stats()["numberOfDocuments"] > 0 or docs_to_delete:
                    docs_to_delete = [i['_id'] for i in idx.search(q, limit=100)['hits']]
                    print(docs_to_delete)
                    if docs_to_delete:
                        idx.delete_documents(docs_to_delete)
                    q = ''.join(choice(ascii_letters) for _ in range(8))
                    attempt += 1
                    if attempt > max_attempts:
                        raise MarqoError(f"Max attempts reached. Failed to delete documents from index {idx.index_name}")
        except MarqoError as e:
            print(f"Error deleting documents from index {idx.index_name}: {e}")
