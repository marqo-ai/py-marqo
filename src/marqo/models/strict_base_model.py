from pydantic import BaseModel


class StrictBaseModel(BaseModel):
    class Config:
        extra: str = "forbid"


class StrictAllowPopulationBaseModel(BaseModel):
    class Config:
        extra: str = "forbid"
        allow_population_by_field_name = True
