from enum import Enum
from typing import List, Optional, Dict, Any

from marqo.models.strict_base_model import StrictBaseModel


class IndexType(Enum):
    Structured = 'structured'
    Unstructured = 'unstructured'


class FieldType(Enum):
    Text = 'text'
    Bool = 'bool'
    Int = 'int'
    Float = 'float'
    ArrayText = 'array<text>'
    ArrayInt = 'array<int>'
    ArrayFloat = 'array<float>'
    ImagePointer = 'image_pointer'
    MultimodalCombination = 'multimodal_combination'


class VectorNumericType(Enum):
    Float = 'float'
    Bfloat16 = 'bfloat16'


class FieldFeature(Enum):
    LexicalSearch = 'lexical_search'
    ScoreModifier = 'score_modifier'
    Filter = 'filter'


class DistanceMetric(Enum):
    Euclidean = 'euclidean'
    Angular = 'angular'
    DotProduct = 'dotproduct'
    PrenormalizedAnguar = 'prenormalized-angular'
    Geodegrees = 'geodegrees'
    Hamming = 'hamming'


class TextSplitMethod(Enum):
    Character = 'character'
    Word = 'word'
    Sentence = 'sentence'
    Passage = 'passage'


class PatchMethod(Enum):
    Simple = 'simple'
    Frcnn = 'frcnn'


class Field(StrictBaseModel):
    name: str
    type: FieldType
    features: List[FieldFeature] = []
    dependent_fields: Optional[Dict[str, float]]
    lexical_field_name: Optional[str]
    filter_field_name: Optional[str]


class TensorField(StrictBaseModel):
    name: str
    chunk_field_name: Optional[str]
    embeddings_field_name: Optional[str]


class HnswConfig(StrictBaseModel):
    ef_construction: int
    m: int


class TextPreProcessing(StrictBaseModel):
    split_length: int
    split_overlap: int
    split_method: TextSplitMethod


class ImagePreProcessing(StrictBaseModel):
    patch_method: Optional[PatchMethod]


class Model(StrictBaseModel):
    name: str
    properties: Optional[Dict[str, Any]]
    custom: bool = False
