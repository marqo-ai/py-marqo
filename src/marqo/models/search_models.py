from typing import Dict, List, Optional, Union
from marqo.models.marqo_models import StrictBaseModel


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

