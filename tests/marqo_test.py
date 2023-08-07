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
and a hash generated from index settings to ensure each index is unique.
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

import marqo
from marqo.utils import construct_authorized_url
from marqo._httprequests import HTTP_OPERATIONS
from marqo.version import __marqo_version__ as py_marqo_support_version
from marqo.client import Client
from marqo.errors import InternalError, MarqoApiError, MarqoWebError
import zlib
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


def create_settings_hash(settings_dict, kwargs):
    dict_to_hash = {**settings_dict, **kwargs} if settings_dict else kwargs
    combined_str = ''.join(f"{key}{value}" for key, value in dict_to_hash.items())
    crc32_hash = zlib.crc32(combined_str.encode())
    short_hash = hex(crc32_hash & 0xffffffff)[2:][
                 :10]  # Take the first 10 characters of the hexadecimal representation
    print(f"Created index with settings hash: {short_hash} for settings: {dict_to_hash}")
    return short_hash


class MarqoTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        local_marqo_settings = {
            "url": os.environ.get("MARQO_URL", 'http://localhost:8882'),
        }
        api_key = os.environ.get("MARQO_API_KEY", None)
        if (api_key):
            local_marqo_settings["api_key"] = api_key
        cls.index_suffix = os.environ.get("MARQO_INDEX_SUFFIX", "")
        cls.client_settings = local_marqo_settings
        cls.authorized_url = cls.client_settings["url"]
        cls.generic_test_index_name = 'test-index'
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
        if self.client.config.is_marqo_cloud:
            self.cleanup_documents_from_all_indices()
        else:
            for index in self.client.get_indexes()['results']:
                if index.index_name.startswith(self.generic_test_index_name):
                    try:
                        index.delete()
                    except marqo.errors.MarqoApiError as e:
                        logging.debug(f'received error `{e}` from index deletion request.')

    def tearDown(self) -> None:
        if self.client.config.is_marqo_cloud:
            self.cleanup_documents_from_all_indices()
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

    def create_cloud_index(self, index_name, settings_dict=None, **kwargs):
        client = marqo.Client(**self.client_settings)
        index_name = f"{index_name}-{self.index_suffix}"
        if settings_dict or kwargs:
            index_name = f"{index_name}-{create_settings_hash(settings_dict, kwargs)}"
        if settings_dict:
            settings_dict.update({
                "inference_type": "marqo.CPU", "storage_class": "marqo.basic"
            })
        try:
            status = client.http.get(f"indexes/{index_name}/status")["index_status"]
            if status == "CREATING":
                cloud_wait_for_index_status(client.http, index_name, "READY")
            if status != "READY":
                self.client.create_index(index_name, settings_dict=settings_dict,
                                         inference_node_type="marqo.CPU", storage_node_type="marqo.basic", **kwargs)
        except (MarqoWebError, TypeError) as e:
            self.client.create_index(index_name, settings_dict=settings_dict,
                                     inference_node_type="marqo.CPU", storage_node_type="marqo.basic", **kwargs)
        return index_name

    def create_test_index(self, index_name: str, settings_dict: dict = None, **kwargs):
        """Create a test index with the given name and settings and triggers specific logic if index is cloud index"""
        client = marqo.Client(**self.client_settings)
        if client.config.is_marqo_cloud:
            index_name = self.create_cloud_index(index_name, settings_dict, **kwargs)
        else:
            client.create_index(index_name, settings_dict=settings_dict, **kwargs)
        return index_name

    def cleanup_documents_from_all_indices(self):
        client = marqo.Client(**self.client_settings)
        indexes = client.get_indexes()
        for index in indexes['results']:
            if index.index_name.startswith(self.generic_test_index_name) and \
                    self.index_suffix in index.index_name.split('-'):
                if client.http.get(f"/indexes/{index.index_name}/status")["index_status"] == "READY":
                    docs_to_delete = [i['_id'] for i in index.search("", limit=100)['hits']]
                    while docs_to_delete:
                        index.delete_documents(docs_to_delete, auto_refresh=True)
                        docs_to_delete = [i['_id'] for i in index.search("", limit=100)['hits']]