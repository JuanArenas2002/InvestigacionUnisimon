"""
Configuración centralizada del proyecto de Reconciliación Bibliográfica.
Todas las credenciales, URLs y parámetros del sistema en un solo lugar.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Optional


# =============================================================
# RUTAS DEL PROYECTO
# =============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "OpenAlexJson"
EXPORTS_DIR = BASE_DIR / "exports"


# =============================================================
# BASE DE DATOS
# =============================================================

@dataclass
class DatabaseConfig:
    """Configuración de PostgreSQL"""
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    database: str = os.getenv("DB_NAME", "reconciliacion_bibliografica")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "123456")
    echo_sql: bool = False  # True para depuración SQL

    @property
    def url(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


# =============================================================
# INSTITUCIÓN
# =============================================================

@dataclass
class InstitutionConfig:
    """Datos de la institución a analizar"""
    ror_id: str = os.getenv("ROR_ID", "https://ror.org/02njbw696")
    name: str = os.getenv("INSTITUTION_NAME", "Universidad")
    contact_email: str = os.getenv("CONTACT_EMAIL", "biblioteca@universidad.edu")
    scopus_affiliation_id: str = os.getenv("SCOPUS_AFFILIATION_ID", "")


# =============================================================
# APIs EXTERNAS
# =============================================================

@dataclass
class OpenAlexConfig:
    """Configuración de OpenAlex API"""
    base_url: str = "https://api.openalex.org/works"
    api_key: str = os.getenv("OA_KEY", "")
    max_per_page: int = 200
    timeout: int = 30
    rate_limit_delay: float = 0.1
    max_retries: int = 3


@dataclass
class ScopusConfig:
    """Configuración de Scopus API (Elsevier)"""
    base_url: str = "https://api.elsevier.com/content"
    api_key: str = os.getenv("SCOPUS_API_KEY", "")
    inst_token: str = os.getenv("SCOPUS_INST_TOKEN", "")
    timeout: int = 30
    max_retries: int = 3
    max_per_page: int = 25  # Scopus limita a 25 por página


@dataclass
class WosConfig:
    """Configuración de Web of Science API (Clarivate)"""
    base_url: str = "https://api.clarivate.com/apis/wos-starter/v1"
    api_key: str = os.getenv("WOS_API_KEY", "")
    timeout: int = 30
    max_retries: int = 3
    max_per_page: int = 50


@dataclass
class CvlacConfig:
    """Configuración para CVLAC / GrupLAC (Minciencias)"""
    base_url: str = "https://scienti.minciencias.gov.co/cvlac"
    gruplac_url: str = "https://scienti.minciencias.gov.co/gruplac"
    timeout: int = 60
    max_retries: int = 3
    delay_between_requests: float = 2.0  # Más conservador para scraping


@dataclass
class DatosAbiertosConfig:
    """Configuración para API de Datos Abiertos Colombia"""
    base_url: str = "https://www.datos.gov.co/resource"
    app_token: str = os.getenv("DATOS_ABIERTOS_TOKEN", "")
    timeout: int = 30
    max_per_page: int = 1000  # SODA API permite hasta 50k


# =============================================================
# RECONCILIACIÓN
# =============================================================

# =============================================================
# CRITERIOS DE INCLUSIÓN/EXCLUSIÓN
# =============================================================

@dataclass
class CriteriaConfig:
    """
    Criterios de validación de registros bibliográficos.
    Define qué registros se aceptan, rechazan o marcan para revisión.
    
    Ver: docs/CRITERIA.md
    """

    # --- Campos obligatorios ---
    min_title_length: int = 5
    max_title_length: int = 500
    min_year: int = 1900
    max_year: int = 2099

    # --- Completitud de metadatos (en limpieza) ---
    min_completeness_accepted: float = 0.70      # 70% para aceptar
    min_completeness_review: float = 0.50        # 50% para revisar manualmente
    # < 50% se rechaza

    # --- Fuzzy matching (en reconciliación) ---
    fuzzy_auto_accept: float = 0.95              # >= 95% = aceptar automático
    fuzzy_manual_review: float = 0.85            # 85-95% = revisar manualmente

    # --- Tolerancia temporal ---
    year_tolerance: int = 2                      # +/- años permitidos

    # --- Palabras prohibidas (contenido no científico) ---
    blacklist_keywords: list = field(default_factory=lambda: [
        "404", "error", "not found", "confidencial",
        "borrador", "draft", "untitled", "sin titulo"
    ])

    # --- Fuentes válidas ---
    valid_sources: list = field(default_factory=lambda: [
        "openalex", "scopus", "wos", "cvlac", "datos_abiertos"
    ])

    # --- Tipos de publicación válidos ---
    valid_publication_types: list = field(default_factory=lambda: [
        "journal-article", "review-article", "conference-paper",
        "book-chapter", "book", "report", "dataset", "preprint",
        "monograph", "technical-report", "working-paper"
    ])


# =============================================================
# RECONCILIACIÓN
# =============================================================

@dataclass
class ReconciliationConfig:
    """Parámetros del motor de reconciliación"""

    # --- Paso 1: Match exacto por DOI ---
    doi_exact_match: bool = True

    # --- Paso 2: Fuzzy matching ---
    fuzzy_enabled: bool = True

    # Umbrales de similitud (0-100)
    title_threshold: float = 88.0       # Mínimo para considerar match de título
    title_high_confidence: float = 95.0  # Match casi seguro solo por título
    author_threshold: float = 80.0       # Mínimo para match de autores
    combined_threshold: float = 85.0     # Umbral combinado ponderado

    # Pesos para score combinado
    weight_title: float = 0.55
    weight_year: float = 0.20
    weight_authors: float = 0.25
    title_weight: float = 0.55  # Alias para compatibility
    year_weight: float = 0.20
    author_weight: float = 0.25

    # Año: ¿debe coincidir exactamente?
    year_must_match: bool = True
    year_tolerance: int = 0  # +/- años de tolerancia (0 = exacto)

    # Review manual
    manual_review_threshold: float = 70.0  # Por debajo de combined_threshold
    #   pero encima de este, marcar para revisión manual


# =============================================================
# ENUMS / CONSTANTES
# =============================================================

class SourceName:
    """Nombres canónicos de las fuentes"""
    OPENALEX = "openalex"
    SCOPUS = "scopus"
    WOS = "wos"
    CVLAC = "cvlac"
    DATOS_ABIERTOS = "datos_abiertos"

    ALL = [OPENALEX, SCOPUS, WOS, CVLAC, DATOS_ABIERTOS]


class MatchType:
    """Tipos de coincidencia en reconciliación"""
    DOI_EXACT = "doi_exact"
    FUZZY_HIGH = "fuzzy_high_confidence"
    FUZZY_COMBINED = "fuzzy_combined"
    MANUAL_REVIEW = "manual_review"
    NO_MATCH = "no_match"


class RecordStatus:
    """Estados de un registro externo"""
    PENDING = "pending"
    MATCHED = "matched"
    NEW_CANONICAL = "new_canonical"
    REVIEW = "manual_review"
    REJECTED = "rejected"


# =============================================================
# INSTANCIA GLOBAL DE CONFIGURACIÓN
# =============================================================

db_config = DatabaseConfig()
institution = InstitutionConfig()
openalex_config = OpenAlexConfig()
scopus_config = ScopusConfig()
wos_config = WosConfig()
cvlac_config = CvlacConfig()
datos_abiertos_config = DatosAbiertosConfig()
criteria_config = CriteriaConfig()
reconciliation_config = ReconciliationConfig()
