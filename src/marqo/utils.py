from marqo import errors
from typing import Optional


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
