from typing import Dict, List, Optional, Union
from pydantic import BaseModel

from marqo.enums import SearchMethods

class SearchBody(BaseModel):
    q: Union[str, Dict[str, float]]
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

class BulkSearchBody(SearchBody):
    index: str

class BulkSearchQuery(BaseModel):
    queries: List[BulkSearchBody]