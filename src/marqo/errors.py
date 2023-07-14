import json
from requests import Response
from http import HTTPStatus


class MarqoError(Exception):
    """Generic class for Marqo error handling
    These will be caught and returned to the user as 5xx internal errors

    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return f'MarqoError. Error message: {self.message}'


class MarqoApiError(MarqoError):
    """Error sent by Marqo API"""

    def __init__(self, error: str, request: Response) -> None:
        self.status_code = request.status_code
        self.code = None
        self.link = None
        self.type = None

        if request.text:
            json_data = json.loads(request.text)
            self.message = json_data
            self.code = json_data.get('status')
            self.link = ''
            self.type = ''
            if 'error' in json_data and 'root_cause' in json_data["error"]\
                    and len(json_data.get('error').get('root_cause')) > 0:
                self.type = json_data.get('error').get('root_cause')[0].get('type')
        else:
            self.message = error
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.code and self.link:
            return f'MarqoApiError. Error code: {self.code}. Error message: {self.message} Error documentation: {self.link} Error type: {self.type}'

        return f'MarqoApiError. {self.message}'


# MARQO WEB ERROR

class MarqoWebError(Exception):

    status_code: int = None
    error_type: str = None
    message: str = None
    code: str = None
    link: str = ""

    def __init__(self, message: str, status_code: int = None,
                 error_type: str = None, code: str = None,
                 link: str = None) -> None:
        self.message = message

        if self.status_code is None:
            self.status_code = status_code

        if self.error_type is None:
            self.error_type = error_type

        if self.code is None:
            self.code = code

        if self.link is None:
            self.link = link
        super().__init__(self.message)

    def __str__(self) -> str:
        return f'MarqoWebError: {self.__class__.__name__} Error message: {self.message}\n' \
               f'status_code: {self.status_code}, type: {self.error_type}, ' \
               f'code: {self.code}, link: {self.link}'


# ---MARQO USER ERRORS---

# --


class __InvalidRequestError(MarqoWebError):
    """abstract error"""

    def __init__(self, message: str):
        self.message = message

    error_type = "invalid_request"


class IndexAlreadyExistsError(__InvalidRequestError):
    code = "index_already_exists"
    status_code = HTTPStatus.CONFLICT


class IndexNotFoundError(__InvalidRequestError):
    code = "index_not_found"
    status_code = HTTPStatus.NOT_FOUND


class InvalidIndexNameError(__InvalidRequestError):
    code = "invalid_index_name"
    status_code = HTTPStatus.BAD_REQUEST


class InvalidDocumentIdError(__InvalidRequestError):
    code = "invalid_document_id"
    status_code = HTTPStatus.BAD_REQUEST


class InvalidFieldNameError(__InvalidRequestError):
    code = "invalid_field_name"
    status_code = HTTPStatus.BAD_REQUEST


class InvalidArgError(__InvalidRequestError):
    code = "invalid_argument"
    status_code = HTTPStatus.BAD_REQUEST


class BadRequestError(__InvalidRequestError):
    code = "bad_request"
    status_code = HTTPStatus.BAD_REQUEST


class DocumentNotFoundError(__InvalidRequestError):
    code = "document_not_found"
    status_code = HTTPStatus.NOT_FOUND


class NonTensorIndexError(__InvalidRequestError):
    """Error trying to use a non-neural OpenSearch index like a neural one"""
    code = "document_not_found"
    status_code = HTTPStatus.NOT_FOUND

# ---MARQO INTERNAL ERROR---


class InternalError(MarqoWebError):
    error_type = "internal"
    code = "internal"
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR


class BackendCommunicationError(InternalError):
    """Error when connecting to Marqo"""
    code = "backend_communication_error"
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR

    def __init__(self, message: str,) -> None:
        self.message = f"Error communicating with Marqo: {message}"


class BackendTimeoutError(InternalError):
    """Error when Marqo operation takes longer than expected"""
    code = "backend_timeout_error"
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR

    def __init__(self, message: str,) -> None:
        self.message = f"Timeout error communicating with Marqo: {message}"


# NON HTTP ERRORS:

class MarqoCloudIndexNotReadyError(MarqoError):
    """Error when Marqo index is not ready"""
    code = "index_not_ready_cloud"
    status_code = HTTPStatus.NOT_FOUND

    def __init__(self, index_name: str,) -> None:
        self.message = f"The Python client could not resolve the endpoint for the index name {index_name}. " \
                       f"This could be due to the index is still in the process of being created," \
                       f" or that the client's cache has not yet been updated.\n" \
                       f"- Please try again in a couple of minutes, or you can query the index status" \
                       f" with mq.index({index_name}).get_status function to see when index is ready.\n" \
                       f"- If the problem persists, please contact marqo support at support@marqo.ai"


class MarqoCloudIndexNotFoundError(MarqoError):
    """Error when Marqo index is not ready"""
    code = "index_not_found_cloud"
    status_code = HTTPStatus.NOT_FOUND

    def __init__(self, index_name: str,) -> None:
        self.message = f"The index name {index_name} does not exist in the Marqo cloud or client's cache" \
                       f" has not yet been updated. Please check the index name and try again.\n" \
                       f"- If the problem persists, please contact marqo support at support@marqo.ai"
