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


def deprecate_parameters(*deprecated_params):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            """ Checks if any of deprecated key arguments are passed to the function and issues a warning if so.
            Check is performed by comparing the values of the deprecated parameters to their default values."""
            # Get the default values of the function
            default_values = func.__defaults__

            # Get the parameter names
            signature = inspect.signature(func)
            default_parameter_names = [
                param.name for param in signature.parameters.values() if param.default is not inspect.Parameter.empty
            ]

            # Create a dictionary mapping parameter names to their default values
            defaults_dict = dict(zip(default_parameter_names, default_values))

            # Check if any of the deprecated parameters have values different from their defaults
            deprecated_params_with_non_default_values = [
                param for param in deprecated_params if kwargs.get(param) != defaults_dict.get(param)
            ]

            if deprecated_params_with_non_default_values:
                deprecated_params_string = ', '.join(deprecated_params_with_non_default_values)
                warning_msg = \
                    f"The parameter(s) {deprecated_params_string} are deprecated. " \
                    f"Please refer to the documentation " \
                    f"https://docs.marqo.ai/1.3.0/Using-Marqo-Cloud/indexes/#create-index " \
                    f"for updated parameters names. These parameters will be removed in Marqo 2.0.0."

                # Issue a deprecation warning
                mq_logger.warn(warning_msg)

            return func(*args, **kwargs)

        return wrapper

    return decorator


def is_key_arguments_default(current_frame, func, arguments_to_ignore=None):
    """ Checks if all key arguments of the function are default values.
    Ignores arguments specified in arguments_to_ignore list."""
    passed_and_default = zip(current_frame.f_locals.items(), inspect.signature(func).parameters.values())
    for passed_parameter, default_parameter in passed_and_default:
        if default_parameter.default is not inspect.Parameter.empty:
            if arguments_to_ignore and passed_parameter[0] in arguments_to_ignore:
                continue
            if passed_parameter[1] != default_parameter.default:
                return False
    return True


def use_one_of_two_arguments(func, value_name, value, deprecated_value, deprecated_value_name):
    """ Checks if both value and deprecated_value are passed to the function and returns only one of them,
    prioritising non-deprecated one if so. Raises error if both of them are specified"""
    default_parameters = inspect.signature(func).parameters
    value_is_specified = value != default_parameters[value_name].default
    deprecated_value_is_specified = deprecated_value != default_parameters[deprecated_value_name].default
    if value_is_specified and deprecated_value_is_specified:
        raise ValueError(f"{value_name} and {deprecated_value_name} can't be specified at the same time "
                         f"because they refer to the same value, please use only one at a time")
    return value if value_is_specified else deprecated_value
