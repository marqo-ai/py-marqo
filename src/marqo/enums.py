"""enums to be used by consumers of the Marqo client"""
from enum import Enum


class SearchMethods(str, Enum):
    LEXICAL = "LEXICAL"
    TENSOR = "TENSOR"


class Devices:
    cpu = "cpu"
    cuda = "cuda"


class IndexStatus(str, Enum):
    READY = "READY"
    DELETED = "DELETED"
    MODIFYING = "MODIFYING"
    CREATING = "CREATING"
    DELETING = "DELETING"
    FAILED = "FAILED"


class InterpolationMethod(str, Enum):
    LERP = "lerp"
    NLERP = "nlerp"
    SLERP = "slerp"


class EmbedContentType(str, Enum):
    Query = "query"
    Document = "document"
