"""
Clase base abstracta para todos los extractores bibliográficos.

Cada fuente (OpenAlex, Scopus, WoS, CVLAC, Datos Abiertos) debe
implementar esta interfaz para que el pipeline y el reconciliador
trabajen de forma homogénea.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Dict, Optional

from unidecode import unidecode
import re

logger = logging.getLogger(__name__)


# =============================================================
# FORMATO ESTÁNDAR DE SALIDA
# =============================================================

@dataclass
class StandardRecord:
    """
    Registro estandarizado que todos los extractores deben producir.
    Este es el contrato común que alimenta las tablas por fuente
    (openalex_records, scopus_records, etc.) y el motor de reconciliación.
    """

    # --- Fuente ---
    source_name: str            # openalex, scopus, wos, cvlac, datos_abiertos
    source_id: Optional[str] = None  # ID interno de esa fuente

    # --- Identificadores ---
    doi: Optional[str] = None
    pmid: Optional[str] = None
    pmcid: Optional[str] = None

    # --- Metadatos ---
    title: Optional[str] = None
    publication_year: Optional[int] = None
    publication_date: Optional[str] = None
    publication_type: Optional[str] = None
    language: Optional[str] = None

    # --- Fuente / revista ---
    source_journal: Optional[str] = None
    issn: Optional[str] = None

    # --- Open Access ---
    is_open_access: Optional[bool] = None
    oa_status: Optional[str] = None

    # --- Autores ---
    authors: List[Dict] = field(default_factory=list)
    # Cada autor: {"name": str, "orcid": str|None, "is_institutional": bool}
    institutional_authors: List[Dict] = field(default_factory=list)

    # --- Métricas ---
    citation_count: int = 0

    # --- URLs ---
    url: Optional[str] = None

    # --- Data cruda original ---
    raw_data: Optional[dict] = field(default_factory=dict)

    # --- Campos calculados para reconciliación ---
    normalized_title: Optional[str] = None
    authors_text: Optional[str] = None
    normalized_authors: Optional[str] = None

    # --- Timestamp ---
    extracted_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def compute_normalized_fields(self):
        """Calcula campos normalizados para reconciliación"""
        if self.title:
            self.normalized_title = normalize_text(self.title)

        # Concatenar nombres de autores para fuzzy matching
        author_names = []
        for a in self.authors:
            name = a.get("name", "")
            if name:
                author_names.append(name)

        if author_names:
            self.authors_text = "; ".join(author_names)
            self.normalized_authors = normalize_text(self.authors_text)

        # Normalizar DOI
        if self.doi:
            self.doi = normalize_doi(self.doi)

        return self

    def to_dict(self) -> dict:
        return asdict(self)


# =============================================================
# NORMALIZADORES COMPARTIDOS
# =============================================================

def normalize_author_name(name: str) -> str:
    """
    Limpia un nombre de autor para almacenamiento legible:
    - Reemplaza guiones Unicode (\u2010, \u2011, \u2012, \u2013, \u2014, \u00AD) y
      guiones ASCII por espacios.
    - Colapsa espacios multiples.
    - Conserva tildes y mayusculas (no es para comparacion, es para display).
    """
    if not name:
        return ""
    name = str(name).strip()
    # Reemplazar todos los tipos de guion/hyphen por espacio
    name = re.sub(r'[\u2010\u2011\u2012\u2013\u2014\u2015\u00AD\u002D\uFE58\uFE63\uFF0D]+', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def normalize_text(text: str) -> str:
    """
    Normaliza texto para comparación:
    - minúsculas
    - sin tildes/diacríticos
    - solo alfanuméricos y espacios
    - espacios múltiples colapsados
    """
    if not text:
        return ""
    text = str(text).lower().strip()
    text = unidecode(text)
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def normalize_doi(doi: str) -> str:
    """
    Normaliza un DOI a formato canónico: 10.xxxx/yyyy (sin URL prefix).
    """
    if not doi:
        return ""
    doi = str(doi).strip().lower()
    # Eliminar prefijos URL comunes
    for prefix in ["https://doi.org/", "http://doi.org/", "doi:", "doi.org/"]:
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
    return doi.strip()


def normalize_year(year) -> Optional[int]:
    """Normaliza año a entero"""
    if year is None:
        return None
    year_str = re.sub(r'\D', '', str(year))
    if year_str and len(year_str) == 4:
        return int(year_str)
    return None


# =============================================================
# CLASE BASE ABSTRACTA
# =============================================================

class BaseExtractor(ABC):
    """
    Interfaz que todos los extractores deben implementar.

    Flujo de uso:
        extractor = MiExtractor(config)
        records = extractor.extract(year_from=2020, year_to=2025)
        # records es List[StandardRecord] — formato homogéneo
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Nombre canónico de la fuente (e.g., 'openalex', 'scopus')"""
        ...

    @abstractmethod
    def extract(
        self,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: Optional[int] = None,
        **kwargs,
    ) -> List[StandardRecord]:
        """
        Extrae publicaciones de la fuente.

        Args:
            year_from: Año inicial (inclusive)
            year_to: Año final (inclusive)
            max_results: Límite de resultados (None = todos)

        Returns:
            Lista de StandardRecord normalizados
        """
        ...

    @abstractmethod
    def _parse_record(self, raw: dict) -> StandardRecord:
        """
        Convierte un registro crudo de la fuente a StandardRecord.
        Cada fuente tiene su propio formato → esta función lo traduce.
        """
        ...

    def _post_process(self, records: List[StandardRecord]) -> List[StandardRecord]:
        """
        Post-procesamiento común: calcular campos normalizados.
        Se invoca automáticamente después de extract().
        """
        for record in records:
            record.compute_normalized_fields()
        logger.info(
            f"[{self.source_name}] {len(records)} registros extraídos y normalizados."
        )
        return records
