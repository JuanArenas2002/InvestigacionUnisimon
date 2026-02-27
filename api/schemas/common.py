"""
Schemas comunes: paginación, respuestas genéricas, utilidades.
"""

from typing import Generic, TypeVar, Optional, List
from pydantic import BaseModel, Field

T = TypeVar("T")


class PaginationParams(BaseModel):
    page: int = Field(1, ge=1, description="Número de página")
    page_size: int = Field(50, ge=1, le=500, description="Registros por página")

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size


class PaginatedResponse(BaseModel, Generic[T]):
    """Respuesta paginada genérica"""
    items: List[T]
    total: int
    page: int
    page_size: int
    pages: int

    @classmethod
    def create(cls, items: List[T], total: int, page: int, page_size: int):
        import math
        return cls(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            pages=math.ceil(total / page_size) if page_size > 0 else 0,
        )


class MessageResponse(BaseModel):
    """Respuesta simple con mensaje"""
    ok: bool = True
    message: str = ""
    detail: Optional[dict] = None
