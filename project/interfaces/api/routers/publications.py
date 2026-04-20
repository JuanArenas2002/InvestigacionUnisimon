from fastapi import APIRouter, Query

from project.config.container import build_repository

router = APIRouter(prefix="/publications", tags=["Publications"])


@router.get("", summary="Lista publicaciones canonicas")
def get_publications(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    repository = build_repository()
    records = repository.list_publications(limit=limit, offset=offset)
    return {
        "count": len(records),
        "limit": limit,
        "offset": offset,
        "items": records,
    }
