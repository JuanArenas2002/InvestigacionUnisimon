from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from project.config.container import build_pipeline, build_source_registry

router = APIRouter(prefix="/ingest", tags=["Ingest"])


class IngestRequest(BaseModel):
    sources: Optional[List[str]] = Field(default=None, description="Fuentes a procesar")
    year_from: Optional[int] = Field(default=None, ge=1900, le=2100)
    year_to: Optional[int] = Field(default=None, ge=1900, le=2100)
    max_results: Optional[int] = Field(default=100, ge=1)
    source_kwargs: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict,
        description=(
            "Parámetros específicos por fuente. "
            "Ej: {'cvlac': {'cvlac_codes': ['123']}, 'google_scholar': {'scholar_ids': ['abc']}}"
        ),
    )
    dry_run: bool = False


@router.post("", summary="Ejecuta pipeline ETL de ingesta")
def ingest(request: IngestRequest):
    registry = build_source_registry()
    selected_sources = request.sources or registry.source_names

    invalid_sources = sorted(set(selected_sources) - set(registry.source_names))
    if invalid_sources:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Fuentes no registradas",
                "invalid": invalid_sources,
                "available": registry.source_names,
            },
        )

    pipeline = build_pipeline(selected_sources)

    result = pipeline.run(
        year_from=request.year_from,
        year_to=request.year_to,
        max_results=request.max_results,
        source_kwargs=request.source_kwargs,
        persist=not request.dry_run,
    )

    return {
        "status": "ok",
        "selected_sources": selected_sources,
        "stages": {
            "collect": result.collected,
            "deduplicate": result.deduplicated,
            "normalize": result.normalized,
            "match": result.matched,
            "enrich": result.enriched,
        },
        "persistence": {
            "authors_saved": result.authors_saved,
            "source_saved": result.source_saved,
            "canonical_upserted": result.canonical_upserted,
            "dry_run": request.dry_run,
        },
        "by_source": result.by_source,
        "errors": result.errors,
    }
