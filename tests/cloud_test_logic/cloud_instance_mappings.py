from functools import wraps
from typing import NamedTuple, Union, List
from unittest import mock

import requests


class GetIndexesIndexResponseObject(NamedTuple):
    """Represents a response object containing information about an index object in get indexes response.

    This class is designed to encapsulate data related to an index mapping, making it easier
    to pass this information as an argument to other functions.
    This class is used in the mock_instance_mappings decorator.

    Example Usage:
    This will create basic index object with name "test-index", status "READY", and endpoint "endpoint"
    >>> @mock_get_indexes_response([GetIndexesIndexResponseObject.get_default_index_object()])
    Response object would look:
    {"results": [{"index_name": "test-index", "index_status": "READY", "endpoint": "endpoint"}]}

    This will create index object with specified parameters
    >>> @mock_get_indexes_response([GetIndexesIndexResponseObject("other-index", "CREATING", "endpoint2")])
    Response object would look:
    {"results": [{"index_name": "other-index", "index_status": "CREATING", "endpoint": "endpoint2"}]}

"""
    index_name: str
    index_status: str
    endpoint: str

    @staticmethod
    def get_default_index_object():
        return GetIndexesIndexResponseObject(
            index_name="test-index",
            index_status="READY",
            endpoint="endpoint"
        )


def mock_get_indexes_response(indexes_list: Union[List[GetIndexesIndexResponseObject], None], to_return_mock: bool = False):
    """Function decorator to mock the get indexes endpoint.

    This decorator is used to mock the behavior of the requests.get which is used by
    MarqoCloudInstanceMappings object to retrieve and store specific information about
    cloud indexes: index_name, index status and index endpoint.
    It allows you to set up mock responses for requests to the "/indexes" URL.
    Requests are handled by side_effect function.
    if url ends with "/indexes" it returns a mock_get object, otherwise `requests.get` is called normally.
    A mock_get object is a MagicMock object with its return_value set to indexes_list.
    mock_get object can be argument of the decorated function, if to_return_mock is True.
    It can be used to modify the response of requests.get or to assert that it was called.

    Args:
        indexes_list (Union[List[GetIndexesIndexResponseObject], None]): A list of
            InstanceMappingIndexData objects to be used for mocking responses
            to requests to the "/indexes" URL. If None, mock object will not
            have return_value set for it.
        to_return_mock (bool): Indicates whether the decorated test function
            should return the mock_get object or not. If True, the test function
            will be called with the mock_get object as an argument; otherwise, it
            will be called without the mock object. It might be useful in cases
            where you want to use the mock object to assert that it was called
            or to modify it after it was called.

    Returns:
        function: The decorated test function.

    Example usage:
        >>> @mock_get_indexes_response([GetIndexesIndexResponseObject(), ...GetIndexesIndexResponseObject])
        >>> def test_example(mock_get):
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
