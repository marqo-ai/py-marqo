from typing import Dict, List, Optional, Union
from marqo.models.marqo_models import StrictBaseModel
from abc import ABC
from enum import Enum

from pydantic import validator, BaseModel, root_validator


class SearchBody(StrictBaseModel):
    q: Optional[Union[str, Dict[str, float]]] = None
    searchableAttributes: Union[None, List[str]] = None
    searchMethod: Union[None, str] = "TENSOR"
    limit: int = 10
    offset: int = 0
    showHighlights: bool = True
    reRanker: str = None
    filter: str = None
    attributesToRetrieve: Union[None, List[str]] = None
    boost: Optional[Dict] = None
    image_download_headers: Optional[Dict] = None
    context: Optional[Dict] = None
    scoreModifiers: Optional[Dict] = None
    modelAuth: Optional[Dict] = None


class BulkSearchBody(SearchBody):
    index: str


class BulkSearchQuery(StrictBaseModel):
    queries: List[BulkSearchBody]


class RetrievalMethod(str, Enum):
    Disjunction = 'disjunction'
    Tensor = 'tensor'
    Lexical = 'lexical'


class RankingMethod(str, Enum):
    RRF = 'rrf'
    NormalizeLinear = 'normalize_linear'
    Tensor = 'tensor'
    Lexical = 'lexical'


class HybridParameters:
    retrieval_method: Optional[RetrievalMethod] = RetrievalMethod.Disjunction
    ranking_method: Optional[RankingMethod] = RankingMethod.RRF
    alpha: Optional[float] = None
    rrf_k: Optional[int] = None
    searchable_attributes_lexical: Optional[List[str]] = None
    searchable_attributes_tensor: Optional[List[str]] = None
    verbose: bool = False

    # Input for API, but form will change before being passed to core Hybrid Query.
    score_modifiers_lexical: Optional[dict] = None
    score_modifiers_tensor: Optional[dict] = None

