"""
Configuracion centralizada para la arquitectura hexagonal.
Envuelve config.py existente y expone Settings como dataclass tipada.
"""

import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import List, Optional


@dataclass(frozen=True)
class DatabaseSettings:
    database_url: str = field(default_factory=lambda: os.getenv("DATABASE_URL") or os.getenv("DB_URL", ""))
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))
    database: str = field(default_factory=lambda: os.getenv("DB_NAME", "reconciliacion_bibliografica"))
    user: str = field(default_factory=lambda: os.getenv("DB_USER", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", ""))

    @property
    def url(self) -> str:
        if self.database_url:
            if self.database_url.startswith("postgres://"):
                return "postgresql://" + self.database_url[len("postgres://"):]
            return self.database_url
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class ScopusSettings:
    api_key: str = field(default_factory=lambda: os.getenv("SCOPUS_API_KEY", ""))
    affiliation_ids: List[str] = field(
        default_factory=lambda: [
            x.strip()
            for x in os.getenv("SCOPUS_AFFILIATION_IDS", "").split(",")
            if x.strip()
        ]
    )


@dataclass(frozen=True)
class OpenAlexSettings:
    ror_id: str = field(default_factory=lambda: os.getenv("OPENALEX_ROR_ID", ""))
    email: str = field(default_factory=lambda: os.getenv("OPENALEX_EMAIL", ""))


@dataclass(frozen=True)
class WosSettings:
    api_key: str = field(default_factory=lambda: os.getenv("WOS_API_KEY", ""))


@dataclass(frozen=True)
class PipelineSettings:
    default_max_results: int = field(
        default_factory=lambda: int(os.getenv("PIPELINE_MAX_RESULTS", "500"))
    )
    fuzzy_title_threshold: float = field(
        default_factory=lambda: float(os.getenv("PIPELINE_FUZZY_THRESHOLD", "90.0"))
    )


@dataclass(frozen=True)
class AppSettings:
    env: str = field(default_factory=lambda: os.getenv("APP_ENV", "development"))
    allowed_origins: List[str] = field(
        default_factory=lambda: [
            o.strip()
            for o in os.getenv("ALLOWED_ORIGINS", "").split(",")
            if o.strip()
        ]
    )
    contact_email: str = field(
        default_factory=lambda: os.getenv("CONTACT_EMAIL", "biblioteca@universidad.edu")
    )

    @property
    def is_production(self) -> bool:
        return self.env.lower() == "production"

    @property
    def cors_origins(self) -> List[str]:
        return self.allowed_origins if self.is_production else ["*"]


@dataclass(frozen=True)
class Settings:
    app: AppSettings = field(default_factory=AppSettings)
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    scopus: ScopusSettings = field(default_factory=ScopusSettings)
    openalex: OpenAlexSettings = field(default_factory=OpenAlexSettings)
    wos: WosSettings = field(default_factory=WosSettings)
    pipeline: PipelineSettings = field(default_factory=PipelineSettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton de configuracion. Cachea en memoria tras el primer acceso."""
    return Settings()
