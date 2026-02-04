"""
config/source.py
"""

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv(override=True)


def require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class SourceConfig:
    METADATA_PATH: Path
    DATA_PATH: Path
    SEPARATOR: str = ";"

    @staticmethod
    def from_env() -> "SourceConfig":
        root = project_root() / "data"
        return SourceConfig(
            METADATA_PATH=root / require_env("METADATA_FILE_NAME"),
            DATA_PATH=root / require_env("DATA_FILE_NAME"),
            SEPARATOR=os.getenv("CSV_SEPARATOR", ";"),
        )


@dataclass(frozen=True)
class DatabaseConfig:
    USERNAME: str
    PASSWORD: str
    DATABASE: str
    SCHEMA: str
    HOST: str = "localhost"
    PORT: int = 5432

    @staticmethod
    def from_env() -> "DatabaseConfig":
        return DatabaseConfig(
            USERNAME=require_env("USERNAME"),
            PASSWORD=require_env("PASSWORD"),
            DATABASE=require_env("DATABASE"),
            SCHEMA=require_env("SCHEMA"),
            HOST=os.getenv("HOST", "localhost"),
            PORT=int(os.getenv("PORT", "5432")),
        )
    
    @property
    def connection_string(self) -> str:
        return f"postgresql+psycopg://{self.USERNAME}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"