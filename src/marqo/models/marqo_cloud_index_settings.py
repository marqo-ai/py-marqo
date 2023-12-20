from typing import Optional

from pydantic import Field, validator

from marqo.models.create_index_settings import IndexSettings
from marqo.models.marqo_index import *

class CloudIndexSettings(IndexSettings):
    numberOfInferences: Optional[int] = Field(None, alias="number_of_inferences")
    waitForReadiness: Optional[bool] = Field(None, alias="wait_for_readiness")

    @validator('type', pre=True, always=True)
    def set_default_type(self, v):
        return IndexType.Unstructured if v is None else v

    @validator('treatUrlsAndPointersAsImages', pre=True, always=True)
    def set_default_treatUrlsAndPointersAsImages(self, v):
        return False if v is None else v

    @validator("shortStringLengthThreshold", pre=True, always=True)
    def set_default_shortStringLengthThreshold(self, v):
        return 20 if v is None else v

    @validator("normalizeEmbeddings", pre=True, always=True)
    def set_default_normalizeEmbeddings(self, v):
        return True if v is None else v

    @validator("textPreprocessing", pre=True, always=True)
    def set_default_textPreprocessing(self, v):
        return TextPreProcessing(
            splitLength=2,
            splitOverlap=0,
            splitMethod=TextSplitMethod.Sentence
        ) if v is None else v

    @validator("imagePreprocessing", pre=True, always=True)
    def set_default_imagePreprocessing(self, v):
        return ImagePreProcessing(
            patchMethod=None
        ) if v is None else v

    @validator("vectorNumericType", pre=True, always=True)
    def set_default_vectorNumericType(self, v):
        return VectorNumericType.Float if v is None else v

    @validator("annParameters", pre=True, always=True)
    def set_default_annParameters(self, v):
        return AnnParameters(
            spaceType=DistanceMetric.PrenormalizedAnguar,
            parameters=HnswConfig(efConstruction=128, m=8)
        ) if v is None else v





