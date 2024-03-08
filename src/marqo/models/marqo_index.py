from enum import Enum
from typing import List, Optional, Dict, Any

from pydantic import Field
from marqo.models.marqo_models import StrictBaseModel


class IndexType(str, Enum):
    Structured = 'structured'
    Unstructured = 'unstructured'


class FieldType(str, Enum):
    Text = 'text'
    Bool = 'bool'
    Int = 'int'
    Long = 'long'
    Float = 'float'
    Double = 'double'
    ArrayText = 'array<text>'
    ArrayInt = 'array<int>'
    ArrayLong = 'array<long>'
    ArrayFloat = 'array<float>'
    ArrayDouble = 'array<double>'
    ImagePointer = 'image_pointer'
    MultimodalCombination = 'multimodal_combination'
    CustomVector = "custom_vector"


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
    efConstruction: Optional[int] = Field(None, alias="ef_construction")
    m: Optional[int] = None


class TextPreProcessing(StrictBaseModel):
    splitLength: Optional[int] = Field(None, alias="split_length")
    splitOverlap: Optional[int] = Field(None, alias="split_overlap")
    splitMethod: Optional[TextSplitMethod] = Field(None, alias="split_method")


class ImagePreProcessing(StrictBaseModel):
    patchMethod: Optional[PatchMethod] = Field(None, alias="patch_method")


class Model(StrictBaseModel):
    name: Optional[str] = None
    properties: Optional[Dict[str, Any]]
    custom: Optional[bool] = None


class FieldRequest(StrictBaseModel):
    name: str
    type: FieldType
    features: List[FieldFeature] = []
    dependentFields: Optional[Dict[str, float]] = Field(None, alias="dependent_fields")


class AnnParameters(StrictBaseModel):
    spaceType: Optional[DistanceMetric] = Field(None, alias="space_type")
    parameters: Optional[HnswConfig] = None
