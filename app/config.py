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

    CSV_FILE_PATH: str = "data/sailing_level_raw.csv"

    model_config = SettingsConfigDict(env_prefix="DATABASE_")

    @property
    def POSTGRESQL_URL_ASYNC(self):
        return (
            "postgresql+asyncpg://"
            f"{self.POSTGRESQL_USER}:"
            f"{self.POSTGRESQL_PASSWORD}@{self.POSTGRESQL_HOST}:"
            f"{self.POSTGRESQL_PORT}/{self.POSTGRESQL_DB}"
        )

    @property
    def POSTGRESQL_URL(self):
        return (
            "postgresql://"
            f"{self.POSTGRESQL_USER}:{self.POSTGRESQL_PASSWORD}@"
            f"{self.POSTGRESQL_HOST}:{self.POSTGRESQL_PORT}/{self.POSTGRESQL_DB}"
        )


class Settings(BaseModel):
    BASE: BaseAppSettings = BaseAppSettings()
    DATABASE: DatabaseSettings = DatabaseSettings()


@lru_cache()
def get_settings():
    return Settings()
