import copy
import json
from typing import get_args, Any, Callable, Dict, Literal, List, Optional, Tuple, Union
import requests
from json.decoder import JSONDecodeError
from marqo.config import Config
from marqo.errors import (
    MarqoWebError,
    BackendCommunicationError,
    BackendTimeoutError
)

HTTP_OPERATIONS = Literal["delete", "get", "post", "put"]
ALLOWED_OPERATIONS: Tuple[HTTP_OPERATIONS, ...] = get_args(HTTP_OPERATIONS)
session = requests.Session()

OPERATION_MAPPING = {
    'delete': session.delete,
    'get': session.get,
    'post': session.post,
    'put': session.put
}


class HttpRequests:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.headers = {'x-api-key': config.api_key} if config.api_key else {}

    def _operation(self, method: HTTP_OPERATIONS) -> Callable:
        if method not in ALLOWED_OPERATIONS:
            raise ValueError("{} not an allowed operation {}".format(method, ALLOWED_OPERATIONS))

        return OPERATION_MAPPING[method]

    def _construct_path(self, path: str, index_name="") -> str:
        """Augment the URL request path based if telemetry is required."""
        url = f"{self.config.get_url(index_name=index_name)}/{path}"
        if self.config.use_telemetry:
            delimeter= "?" if "?" not in f"{self.config.url}/{path}" else "&"
            return url + f"{delimeter}telemetry=True"
        return url

    def send_request(
        self,
        http_operation: HTTP_OPERATIONS,
        path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str], str]] = None,
        content_type: Optional[str] = None,
        index_name: str = ""
    ) -> Any:
        req_headers = copy.deepcopy(self.headers)

        if content_type is not None and content_type:
            req_headers['Content-Type'] = content_type

        if not isinstance(body, (bytes, str)) and body is not None:
            body = json.dumps(body)

        try:
            response = self._operation(http_operation)(
                url=self._construct_path(path, index_name),
                timeout=self.config.timeout,
                headers=req_headers,
                data=body,
                verify=True
            )
            return self.__validate(response)
        except requests.exceptions.Timeout as err:
            raise BackendTimeoutError(str(err)) from err
        except requests.exceptions.ConnectionError as err:
            raise BackendCommunicationError(str(err)) from err

    def get(
        self, path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str], str]] = None,
        index_name: str = ""
    ) -> Any:
        content_type = None
        if body is not None:
            content_type = 'application/json'
        return self.send_request('get', path=path, body=body, content_type=content_type,index_name=index_name)

    def post(
        self,
        path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str], str]] = None,
        content_type: Optional[str] = 'application/json',
        index_name: str = ""
    ) -> Any:
        return self.send_request('post', path, body, content_type, index_name=index_name)

    def put(
        self,
        path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str], str]] = None,
        content_type: Optional[str] = None,
        index_name: str = ""
    ) -> Any:
        if body is not None:
            content_type = 'application/json'
        return self.send_request('put', path, body, content_type, index_name=index_name)

    def delete(
        self,
        path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str]]] = None,
        index_name: str = ""
    ) -> Any:
        return self.send_request('delete', path, body, index_name=index_name)

    @staticmethod
    def __to_json(
        request: requests.Response
    ) -> Any:
        if request.content == b'':
            return request
        return request.json()

    @staticmethod
    def __validate(
        request: requests.Response
    ) -> Any:
        try:
            request.raise_for_status()
            return HttpRequests.__to_json(request)
        except requests.exceptions.HTTPError as err:
            convert_to_marqo_error_and_raise(response=request, err=err)


def convert_to_marqo_error_and_raise(response: requests.Response, err: requests.exceptions.HTTPError) -> None:
    """Raises a generic MarqoWebError for a given HTTPError"""
    try:
        response_msg = response.json()
        code = response_msg["code"]
        error_type = response_msg["type"]
    except (JSONDecodeError, KeyError) as e:
        response_msg = response.text
        code = "unhandled_error"
        error_type = "unhandled_error_type"

    raise MarqoWebError(message=response_msg, code=code, error_type=error_type,
        status_code=response.status_code) from err