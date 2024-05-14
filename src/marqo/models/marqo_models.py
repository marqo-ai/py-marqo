from pydantic import BaseModel


class MarqoBaseModel(BaseModel):
    class Config:
        populate_by_name = True  # accept both real name and alias (if present)
        validate_assignment = True


class StrictBaseModel(MarqoBaseModel):
    class Config(MarqoBaseModel.Config):
        extra = "forbid"


class ImmutableBaseModel(MarqoBaseModel):
    class Config(MarqoBaseModel.Config):
        frozen = True


class ImmutableStrictBaseModel(StrictBaseModel, ImmutableBaseModel):
    class Config(StrictBaseModel.Config, ImmutableBaseModel.Config):
        pass