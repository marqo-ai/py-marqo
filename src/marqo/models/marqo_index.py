from enum import Enum
from typing import List, Optional, Dict, Any

from pydantic import Field
from marqo.models.strict_base_model import StrictBaseModel, StrictAllowPopulationBaseModel


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


class HnswConfig(StrictAllowPopulationBaseModel):
    efConstruction: Optional[int] = Field(None, alias="ef_construction")
    m: Optional[int] = None


class TextPreProcessing(StrictAllowPopulationBaseModel):
    splitLength: Optional[int] = Field(None, alias="split_length")
    splitOverlap: Optional[int] = Field(None, alias="split_overlap")
    splitMethod: Optional[TextSplitMethod] = Field(None, alias="split_method")


class ImagePreProcessing(StrictAllowPopulationBaseModel):
    patchMethod: Optional[PatchMethod] = Field(None, alias="patch_method")


class Model(StrictAllowPopulationBaseModel):
    name: Optional[str] = None
    properties: Optional[Dict[str, Any]]
    custom: Optional[bool] = None


class FieldRequest(StrictAllowPopulationBaseModel):
    name: str
    type: FieldType
    features: List[FieldFeature] = []
    dependentFields: Optional[Dict[str, float]] = Field(None, alias="dependent_fields")
