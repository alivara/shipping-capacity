import enum
from functools import lru_cache
from typing import List

from pydantic import BaseModel
from pydantic_settings import BaseSettings as PydanticBaseSettings
from pydantic_settings import SettingsConfigDict


class Environment(str, enum.Enum):
    TEST = "test"
    LOCAL = "local"
    PRODUCTION = "production"


class BaseSettings(PydanticBaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class BaseAppSettings(BaseSettings):
    VERSION: str = "0.1.0"
    APP_NAME: str = "Shipping Capacity"
    ADMIN_EMAIL: str = "alivarasteh100@gmail.com"

    DEBUG: bool = False
    ENV: Environment = Environment.LOCAL
    TRUSTED_HOSTS: List[str] = ["localhost", ""]
    ALLOWED_ORIGINS: List[str] = ["localhost", ""]

    model_config = SettingsConfigDict(env_prefix="APP_")


class DatabaseSettings(BaseSettings):
    POSTGRESQL_USER: str = ""
    POSTGRESQL_PASSWORD: str = ""
    POSTGRESQL_HOST: str = ""
    POSTGRESQL_PORT: str = ""
    POSTGRESQL_DB: str = ""

    model_config = SettingsConfigDict(env_prefix="DATABASE_")


class Settings(BaseModel):
    BASE: BaseAppSettings = BaseAppSettings()
    DATABASE: DatabaseSettings = DatabaseSettings()


@lru_cache()
def get_settings():
    return Settings()
