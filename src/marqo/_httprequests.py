import copy
import json
from typing import Any, Callable, Dict, List, Optional, Union
import requests
from json.decoder import JSONDecodeError
from marqo.config import Config
from marqo.errors import (
    MarqoWebError,
    BackendCommunicationError,
    BackendTimeoutError
)

s = requests.Session()

ALLOWED_OPERATIONS = {s.delete, s.get, s.post, s.put}

OPERATION_MAPPING = {'delete': s.delete, 'get': s.get,
                     'post': s.post, 'put': s.put}


class HttpRequests:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.session = s

        if config.api_key:
            self.headers = {'x-api-key': config.api_key}
        else:
            self.headers = dict()

    def send_request(
        self,
        http_method: Callable,
        path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str], str]] = None,
        content_type: Optional[str] = None,
    ) -> Any:
        to_verify = False

        if http_method not in ALLOWED_OPERATIONS:
            raise ValueError("{} not an allowed operation {}".format(http_method, ALLOWED_OPERATIONS))

        req_headers = copy.deepcopy(self.headers)

        if content_type is not None and content_type:
            req_headers['Content-Type'] = content_type

        try:
            request_path = self.config.url + '/' + path
            if isinstance(body, bytes):
                response = http_method(
                    url=request_path,
                    timeout=self.config.timeout,
                    headers=req_headers,
                    data=body,
                    verify=to_verify
                )
            elif isinstance(body, str):
                response = http_method(
                    url=request_path,
                    timeout=self.config.timeout,
                    headers=req_headers,
                    data=body,
                    verify=to_verify
                )
            else:
                response = http_method(
                    url=request_path,
                    timeout=self.config.timeout,
                    headers=req_headers,
                    data=json.dumps(body) if body else None,
                    verify=to_verify
                )
            return self.__validate(response)

        except requests.exceptions.Timeout as err:
            raise BackendTimeoutError(str(err)) from err
        except requests.exceptions.ConnectionError as err:
            raise BackendCommunicationError(str(err)) from err

    def get(
        self, path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str], str]] = None,
    ) -> Any:
        content_type = None
        if body is not None:
            content_type = 'application/json'
        return self.send_request(s.get, path=path, body=body, content_type=content_type)

    def post(
        self,
        path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str], str]] = None,
        content_type: Optional[str] = 'application/json',
    ) -> Any:
        return self.send_request(s.post, path, body, content_type)

    def put(
        self,
        path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str], str]] = None,
        content_type: Optional[str] = None,
    ) -> Any:
        if body is not None:
            content_type = 'application/json'
        return self.send_request(s.put, path, body, content_type)

    def delete(
        self,
        path: str,
        body: Optional[Union[Dict[str, Any], List[Dict[str, Any]], List[str]]] = None,
    ) -> Any:
        return self.send_request(s.delete, path, body)

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