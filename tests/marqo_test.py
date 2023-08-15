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
- However, documents will be deleted after each test.

The function `create_cloud_index` will be triggered whenever an index needs to be created for the cloud tests.
It checks the status of the index and creates a new index if needed. It also adds a suffix for unique test execution
and a hash generated from index settings ensure that only 1 index is created for a given
index settings_dict or index kwargs.
It also ensures that any tests that require an index with specific settings get routed to the appropriate index.

We will not actually create and delete real cloud indexes
during this test suite, because this slows down py-marqo <> Marqo cloud tests.
We should still test these methods with mocking. End-to-end
creation and deletion tests will be done elsewhere.
"""

import logging
import time
import uuid
from collections import defaultdict
from functools import wraps
import json
import os
from pydantic import BaseModel
from typing import Any, Callable, Dict, List, Optional, Union
from unittest import mock, TestCase
from tests.scripts.populate_indices_for_cloud_tests import create_settings_hash

import marqo
from marqo._httprequests import HTTP_OPERATIONS
from marqo.client import Client
from marqo.errors import InternalError, MarqoWebError, MarqoError
from marqo.cloud_helpers import cloud_wait_for_index_status


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
    def decorator(test_func):
        @wraps(test_func)
        def wrapper(self, *args, **kwargs):
            index_to_documents = index_to_documents_fn(self)
            new_args = list(args)
            for index_name, docs in index_to_documents.items():
                if len(docs) == 0:
                    continue
                self.create_test_index(index_name=index_name)
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
        if not self.client.config.is_marqo_cloud:
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

    def create_cloud_index(self, index_name, settings_dict=None, **kwargs):
        """
            Create a cloud index with the given name and settings.

            If settings_dict or any kwargs are provided, a unique index name will be generated by hashing
            the settings_dict or kwargs. If neither settings_dict nor index settings kwargs are provided,
            the index name will be formed by combining the given index_name and the index_suffix.

            The index name must fit within 32 characters on the cloud and must not start with '-'.

            This function checks the status of the index and waits for the index to be ready before returning.

            Args:
                index_name (str): The name of the index to create.
                settings_dict (dict, optional): Dictionary of settings to use when creating the index,
                    similar to settings_dict in create_index.
                **kwargs: Additional keyword arguments to pass to create_index,
                    such as model, treat_urls_and_pointers_as_image.

            Returns:
                str: The name of the created index.

            Caveats:
            - Please note that settings_dict overrides any kwargs during index creation.
        """
        client = marqo.Client(**self.client_settings)
        index_name = f"{index_name}" + (f"-{self.index_suffix}" if self.index_suffix else "")
        if settings_dict and kwargs:
            raise ValueError("Cannot provide both settings_dict and kwargs.")
        elif settings_dict or kwargs:
            index_name = f"{index_name}-{create_settings_hash(settings_dict, **kwargs)}"
        if settings_dict:
            settings_dict.update({
                "inference_type": "marqo.CPU.large", "storage_class": "marqo.basic"
            })

        if len(index_name) > 32:
            raise ValueError(f"Index name {index_name} is too long.")
        try:
            status = client.http.get(f"indexes/{index_name}/status")["index_status"]
            if status == "CREATING":
                cloud_wait_for_index_status(client.http, index_name, "READY")
            elif status != "READY":
                raise RuntimeError(f"Indexes must be precreated during cloud tests. Index {index_name} is not ready. "
                                   f"Please specify it's settings in scripts/populate_all_indices.py")
        except (MarqoWebError, TypeError) as e:
            raise RuntimeError(f"Indexes must be precreated during cloud tests. Index {index_name} is not ready. "
                               f"Please specify its settings in scripts/populate_all_indices.py")
        self.cleanup_documents_from_index(index_name)
        return index_name

    def create_test_index(self, index_name: str, settings_dict: dict = None, **kwargs):
        """Create a test index with the given name and settings and triggers specific logic if index is cloud index"""
        client = marqo.Client(**self.client_settings)
        if client.config.is_marqo_cloud:
            index_name = self.create_cloud_index(index_name, settings_dict, **kwargs)
        else:
            client.create_index(index_name, settings_dict=settings_dict, **kwargs)
        return index_name

    def cleanup_documents_from_index(self, index_to_cleanup: str):
        """"This is used for cloud tests only.
        Delete all documents from specified index.
        """
        idx = self.client.index(index_to_cleanup)
        max_attempts = 100
        print(f"Deleting documents from index {idx.index_name}")
        try:
            # veryfying that index is in the mapping
            idx.refresh()
            self.client.config.instance_mapping.get_index_base_url(idx.index_name)
            attempt = 0
            q = ""
            while idx.get_stats()["numberOfDocuments"] > 0:
                docs_to_delete = [i['_id'] for i in idx.search(q, limit=100)['hits']]
                if docs_to_delete:
                    idx.delete_documents(docs_to_delete, auto_refresh=True)
                if attempt % 10 == 0:
                    q += " "
                attempt += 1
                if attempt > max_attempts:
                    raise MarqoError(f"Max attempts reached. Failed to delete documents from index {idx.index_name}")
        except MarqoError as e:
            print(f"Error deleting documents from index {idx.index_name}: {e}")
