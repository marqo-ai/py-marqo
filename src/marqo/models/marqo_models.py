
from pydantic import ConfigDict, BaseModel


class MarqoBaseModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, validate_assignment=True)


class StrictBaseModel(MarqoBaseModel):
    # TODO[pydantic]: The `Config` class inherits from another class, please create the `model_config` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.
    """class Config(MarqoBaseModel.Config):
        extra = "forbid"
        use_enum_values = True"""

    model_config = ConfigDict(
        **MarqoBaseModel.model_config,
        extra="forbid",
        use_enum_values=True
    )


class ImmutableBaseModel(MarqoBaseModel):
    # TODO[pydantic]: The `Config` class inherits from another class, please create the `model_config` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.
    """class Config(MarqoBaseModel.Config):
        frozen = True"""
    model_config = ConfigDict(
        **MarqoBaseModel.model_config,
        frozen=True
    )


class ImmutableStrictBaseModel(StrictBaseModel, ImmutableBaseModel):
    # TODO[pydantic]: The `Config` class inherits from another class, please create the `model_config` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-config for more information.
    """class Config(StrictBaseModel.Config, ImmutableBaseModel.Config):
        pass"""
    
    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        extra="forbid",
        use_enum_values=True,
        frozen=True
    )