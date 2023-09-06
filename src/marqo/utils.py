import inspect
import json
import urllib.parse
from functools import wraps

from marqo import errors
from typing import Any, Optional, List

from marqo.marqo_logging import mq_logger


def construct_authorized_url(url_base: str, username: str, password: str) -> str:
    """
    Args:
        url_base: The url of the resource. For example, 'http://localhost:8882'
        username: User's username
        password: User's password

    Returns:
        A url string using the credentials for simple HTTP authentication.
    """
    http_sep = "://"
    if http_sep not in url_base:
        raise errors.MarqoError(f"Could not parse url: {url_base}")
    url_split = url_base.split(http_sep)
    if len(url_split) != 2:
        raise errors.MarqoError(f"Could not parse url: {url_base}")
    http_part, domain_part = url_split
    return f"{http_part}{http_sep}{username}:{password}@{domain_part}"


def translate_device_string_for_url(device: Optional[str]) -> Optional[str]:
    """Translates a device string for use as a URL param

    Args:
        device: a string representing a device on the server. Examples include
            "cpu", "cuda", "cuda:2"

    Returns:
        A string of the device, for use as query string parameter.
    """
    if device is None:
        return device

    lowered_device = device.lower()
    return lowered_device.replace(":", "")


def convert_list_to_query_params(query_param: str, x: List[Any]) -> str:
    """ Converts a list value for a query parameter to its query string.

    Args:
        query_param: query parameter
        x: List of values for the query parameter. 

    Returns:
        A rendered query string for the given parameter and parameter value.
    """
    return "&".join([f"{query_param}={urllib.parse.quote_plus(str(xx))}" for xx in x])


def convert_dict_to_url_params(d: dict) -> str:
    """Converts a dict into a url-encoded string that can be appended as a query_param
    Args:
        d: dict to be converted

    Returns:
         A URL-encoded string
    """
    as_str = json.dumps(d)
    url_encoded = urllib.parse.quote_plus(as_str)
    return url_encoded
