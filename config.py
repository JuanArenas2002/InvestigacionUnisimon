"""
Configuración central de compatibilidad para el repositorio.

Este módulo expone la API histórica esperada por el código legado:
`db_config`, `scopus_config`, `openalex_config`, `wos_config`, `cvlac_config`,
`datos_abiertos_config`, `reconciliation_config`, `criteria_config`, `institution`,
los enums `SourceName`, `MatchType`, `RecordStatus`, y alias de clases como
`AppConfig` y `DatabaseConfig`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_list(name: str, default: List[str] | None = None) -> List[str]:
    value = os.getenv(name, "")
    items = [part.strip() for part in value.split(",") if part.strip()]
    return items if items else list(default or [])


class SourceName(str, Enum):
    SCOPUS = "scopus"
    OPENALEX = "openalex"
    WOS = "wos"
    CVLAC = "cvlac"
    DATOS_ABIERTOS = "datos_abiertos"


class MatchType(str, Enum):
    DOI_EXACT = "doi_exact"
    FUZZY = "fuzzy"
    FUZZY_HIGH = "fuzzy_high"
    FUZZY_COMBINED = "fuzzy_combined"
    MANUAL_REVIEW = "manual_review"
    NO_MATCH = "no_match"


class RecordStatus(str, Enum):
    PENDING = "pending"
    MATCHED = "matched"
    REVIEW = "review"
    REJECTED = "rejected"
    NEW_CANONICAL = "new_canonical"


@dataclass(frozen=True)
class AppConfig:
    env: str = field(default_factory=lambda: os.getenv("APP_ENV", "development"))
    allowed_origins: List[str] = field(default_factory=lambda: _env_list("ALLOWED_ORIGINS"))
    contact_email: str = field(default_factory=lambda: os.getenv("CONTACT_EMAIL", "biblioteca@universidad.edu"))

    @property
    def is_production(self) -> bool:
        return self.env.lower() == "production"

    @property
    def cors_origins(self) -> List[str]:
        return self.allowed_origins if self.is_production else ["*"]


@dataclass(frozen=True)
class DatabaseConfig:
    database_url: str = field(default_factory=lambda: os.getenv("DATABASE_URL") or os.getenv("DB_URL", ""))
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))
    database: str = field(default_factory=lambda: os.getenv("DB_NAME", "reconciliacion_bibliografica"))
    user: str = field(default_factory=lambda: os.getenv("DB_USER", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", ""))
    echo_sql: bool = field(default_factory=lambda: _env_bool("DB_ECHO_SQL", False))

    @property
    def url(self) -> str:
        if self.database_url:
            # Compatibilidad con URLs heredadas tipo postgres://
            if self.database_url.startswith("postgres://"):
                return "postgresql://" + self.database_url[len("postgres://"):]
            return self.database_url
        auth = f"{self.user}:{self.password}@" if self.password else f"{self.user}@"
        return f"postgresql://{auth}{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class ReconciliationConfig:
    doi_exact_match: bool = field(default_factory=lambda: _env_bool("RECON_DOI_EXACT_MATCH", True))
    fuzzy_enabled: bool = field(default_factory=lambda: _env_bool("RECON_FUZZY_ENABLED", True))
    title_high_confidence: float = field(default_factory=lambda: float(os.getenv("RECON_TITLE_HIGH_CONFIDENCE", "85.0")))
    year_must_match: bool = field(default_factory=lambda: _env_bool("RECON_YEAR_MUST_MATCH", True))
    year_tolerance: int = field(default_factory=lambda: int(os.getenv("RECON_YEAR_TOLERANCE", "1")))
    combined_threshold: float = field(default_factory=lambda: float(os.getenv("RECON_COMBINED_THRESHOLD", "85.0")))
    manual_review_threshold: float = field(default_factory=lambda: float(os.getenv("RECON_MANUAL_REVIEW_THRESHOLD", "70.0")))
    author_match_threshold: float = field(default_factory=lambda: float(os.getenv("RECON_AUTHOR_MATCH_THRESHOLD", "40.0")))


@dataclass(frozen=True)
class CriteriaConfig:
    min_title_length: int = field(default_factory=lambda: int(os.getenv("CRITERIA_MIN_TITLE_LENGTH", "15")))
    blacklist_keywords: List[str] = field(
        default_factory=lambda: _env_list(
            "CRITERIA_BLACKLIST_KEYWORDS",
            ["erratum", "correction", "retraction", "editorial", "table of contents"],
        )
    )


@dataclass(frozen=True)
class InstitutionConfig:
    name: str = field(default_factory=lambda: os.getenv("INSTITUTION_NAME", "Universidad"))
    ror_id: str = field(default_factory=lambda: os.getenv("ROR_ID", ""))
    contact_email: str = field(default_factory=lambda: os.getenv("CONTACT_EMAIL", "biblioteca@universidad.edu"))
    scopus_affiliation_id: str = field(default_factory=lambda: os.getenv("SCOPUS_AFFILIATION_ID", ""))


@dataclass(frozen=True)
class ScopusConfig:
    base_url: str = field(default_factory=lambda: os.getenv("SCOPUS_BASE_URL", "https://api.elsevier.com/content"))
    api_key: str = field(default_factory=lambda: os.getenv("SCOPUS_API_KEY", ""))
    inst_token: str = field(default_factory=lambda: os.getenv("SCOPUS_INST_TOKEN", ""))
    affiliation_ids: List[str] = field(default_factory=lambda: _env_list("SCOPUS_AFFILIATION_IDS"))
    max_retries: int = field(default_factory=lambda: int(os.getenv("SCOPUS_MAX_RETRIES", "3")))
    timeout: int = field(default_factory=lambda: int(os.getenv("SCOPUS_TIMEOUT", "30")))


@dataclass(frozen=True)
class OpenAlexConfig:
    base_url: str = field(default_factory=lambda: os.getenv("OPENALEX_BASE_URL", "https://api.openalex.org"))
    api_key: str = field(default_factory=lambda: os.getenv("OPENALEX_API_KEY", ""))
    email: str = field(default_factory=lambda: os.getenv("OPENALEX_EMAIL", ""))
    max_retries: int = field(default_factory=lambda: int(os.getenv("OPENALEX_MAX_RETRIES", "3")))
    timeout: int = field(default_factory=lambda: int(os.getenv("OPENALEX_TIMEOUT", "30")))
    ror_id: str = field(default_factory=lambda: os.getenv("OPENALEX_ROR_ID", ""))


@dataclass(frozen=True)
class WosConfig:
    base_url: str = field(default_factory=lambda: os.getenv("WOS_BASE_URL", "https://api.clarivate.com/apis/wos-starter"))
    api_key: str = field(default_factory=lambda: os.getenv("WOS_API_KEY", ""))
    max_per_page: int = field(default_factory=lambda: int(os.getenv("WOS_MAX_PER_PAGE", "100")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("WOS_MAX_RETRIES", "3")))
    timeout: int = field(default_factory=lambda: int(os.getenv("WOS_TIMEOUT", "30")))


@dataclass(frozen=True)
class CvlacConfig:
    base_url: str = field(default_factory=lambda: os.getenv("CVLAC_BASE_URL", "https://scienti.minciencias.gov.co/cvlac"))
    max_retries: int = field(default_factory=lambda: int(os.getenv("CVLAC_MAX_RETRIES", "3")))
    timeout: int = field(default_factory=lambda: int(os.getenv("CVLAC_TIMEOUT", "30")))


@dataclass(frozen=True)
class DatosAbiertosConfig:
    base_url: str = field(default_factory=lambda: os.getenv("DATOS_ABIERTOS_BASE_URL", "https://www.datos.gov.co/resource"))
    app_token: str = field(default_factory=lambda: os.getenv("DATOS_ABIERTOS_TOKEN", ""))
    max_retries: int = field(default_factory=lambda: int(os.getenv("DATOS_ABIERTOS_MAX_RETRIES", "3")))
    timeout: int = field(default_factory=lambda: int(os.getenv("DATOS_ABIERTOS_TIMEOUT", "30")))


app_config = AppConfig()
db_config = DatabaseConfig()
reconciliation_config = ReconciliationConfig()
criteria_config = CriteriaConfig()
institution = InstitutionConfig()
scopus_config = ScopusConfig()
openalex_config = OpenAlexConfig()
wos_config = WosConfig()
cvlac_config = CvlacConfig()
datos_abiertos_config = DatosAbiertosConfig()

DATABASE_URL = db_config.url
DATA_DIR = os.getenv("DATA_DIR", str(Path(__file__).resolve().parent / "reports" / "test_output"))


__all__ = [
    "AppConfig",
    "CriteriaConfig",
    "CvlacConfig",
    "DATABASE_URL",
    "DATA_DIR",
    "DatosAbiertosConfig",
    "DatabaseConfig",
    "InstitutionConfig",
    "MatchType",
    "OpenAlexConfig",
    "RecordStatus",
    "ReconciliationConfig",
    "ScopusConfig",
    "SourceName",
    "WosConfig",
    "app_config",
    "criteria_config",
    "cvlac_config",
    "db_config",
    "datos_abiertos_config",
    "institution",
    "openalex_config",
    "reconciliation_config",
    "scopus_config",
    "wos_config",
]