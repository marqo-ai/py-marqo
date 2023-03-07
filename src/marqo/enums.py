"""enums to be used by consumers of the Marqo client"""
from enum import Enum

class SearchMethods(str, Enum):
    LEXICAL = "LEXICAL"
    TENSOR = "TENSOR"


class Devices:
    cpu = "cpu"
    cuda = "cuda"

