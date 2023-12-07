from enum import Enum
from typing import List, Optional, Dict, Any

from marqo.models.strict_base_model import StrictBaseModel


class IndexType(str, Enum):
    Structured = 'structured'
    Unstructured = 'unstructured'


class FieldType(str, Enum):
    Text = 'text'
    Bool = 'bool'
    Int = 'int'
    Float = 'float'
    ArrayText = 'array<text>'
    ArrayInt = 'array<int>'
    ArrayFloat = 'array<float>'
    ImagePointer = 'image_pointer'
    MultimodalCombination = 'multimodal_combination'


class VectorNumericType(str, Enum):
    Float = 'float'
    Bfloat16 = 'bfloat16'


class FieldFeature(str, Enum):
    LexicalSearch = 'lexical_search'
    ScoreModifier = 'score_modifier'
    Filter = 'filter'


class DistanceMetric(str, Enum):
    Euclidean = 'euclidean'
    Angular = 'angular'
    DotProduct = 'dotproduct'
    PrenormalizedAnguar = 'prenormalized-angular'
    Geodegrees = 'geodegrees'
    Hamming = 'hamming'


class TextSplitMethod(str, Enum):
    Character = 'character'
    Word = 'word'
    Sentence = 'sentence'
    Passage = 'passage'


class PatchMethod(str, Enum):
    Simple = 'simple'
    Frcnn = 'frcnn'


class HnswConfig(StrictBaseModel):
    ef_construction: Optional[int] = None
    m: Optional[int] = None


class TextPreProcessing(StrictBaseModel):
    splitLength: Optional[int] = None
    splitOverlap: Optional[int] = None
    splitMethod: Optional[TextSplitMethod] = None


class ImagePreProcessing(StrictBaseModel):
    patchMethod: Optional[PatchMethod]


class Model(StrictBaseModel):
    name: Optional[str] = None
    properties: Optional[Dict[str, Any]]
    custom: Optional[bool] = None


class FieldRequest(StrictBaseModel):
    name: str
    type: FieldType
    features: List[FieldFeature] = []
    dependent_fields: Optional[Dict[str, float]]
