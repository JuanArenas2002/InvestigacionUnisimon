"""
Schemas Pydantic para el Serial Title API de Scopus.
"""

import re
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator


class JournalCoverageResponse(BaseModel):
    """Cobertura y vigencia de una revista en Scopus."""
    issn: str = Field(..., description="ISSN consultado")
    title: Optional[str] = Field(None, description="Nombre de la revista")
    source_id: Optional[str] = Field(None, description="ID interno de Scopus")
    publisher: Optional[str] = Field(None, description="Editorial")
    status: Optional[str] = Field(
        None, description="Estado: Active, Discontinued o Unknown"
    )
    is_discontinued: bool = Field(
        False, description="True si la revista ya no está activa en Scopus"
    )
    coverage_from: Optional[int] = Field(
        None, description="Primer año de cobertura en Scopus"
    )
    coverage_to: Optional[int] = Field(
        None, description="Último año de cobertura en Scopus"
    )
    subject_areas: Optional[List[str]] = Field(
        None, description="Áreas temáticas Scopus"
    )
    error: Optional[str] = Field(
        None, description="Mensaje de error si no se encontró la revista"
    )


class BulkCoverageRequest(BaseModel):
    """
    Solicitud masiva de cobertura por lista de ISSNs.

    El campo `issns` acepta cualquiera de estos formatos:
      - Array de strings:  ["2595-3982", "0028-0836"]
      - Un solo string con ISSNs separados por comas, saltos de línea o espacios:
        "2595-3982\\n0028-0836\\n01650327"
    Los ISSNs duplicados se eliminan automáticamente.
    """
    issns: List[str] = Field(
        ...,
        description=(
            "Lista de ISSNs. Puede ser un array de strings o un solo string "
            "con ISSNs separados por comas, saltos de línea o espacios."
        ),
        min_length=1,
        max_length=200,
    )
    max_workers: int = Field(
        5,
        ge=1,
        le=10,
        description="Número de hilos paralelos (1-10). Default: 5.",
    )

    @field_validator("issns", mode="before")
    @classmethod
    def normalize_issns(cls, v):
        """
        Acepta tanto una lista de strings como un único string con ISSNs
        separados por comas, saltos de línea o espacios. Elimina duplicados
        y entradas vacías, y preserva el orden de aparición.
        """
        # Si viene como string único, convertir a lista
        if isinstance(v, str):
            v = [v]

        # Expandir cada elemento que pueda contener separadores
        expanded = []
        for item in v:
            if isinstance(item, str):
                # Dividir por coma, salto de línea, retorno de carro o espacio
                parts = re.split(r"[\s,]+", item.strip())
                expanded.extend(parts)
            else:
                expanded.append(item)

        # Limpiar, deduplicar preservando orden
        seen = set()
        clean = []
        for issn in expanded:
            if not isinstance(issn, str):
                continue
            issn = issn.strip()
            if not issn:
                continue
            if issn not in seen:
                seen.add(issn)
                clean.append(issn)

        if not clean:
            raise ValueError("No se encontraron ISSNs válidos en la solicitud.")

        return clean


class BulkCoverageResponse(BaseModel):
    """Respuesta masiva de cobertura de revistas."""
    total: int = Field(..., description="Total de ISSNs consultados")
    found: int = Field(..., description="Revistas encontradas en Scopus")
    not_found: int = Field(..., description="ISSNs no encontrados")
    errors: int = Field(..., description="ISSNs con error de API")
    results: List[JournalCoverageResponse] = Field(
        ..., description="Resultados por ISSN"
    )
