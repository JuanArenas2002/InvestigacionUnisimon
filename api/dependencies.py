"""
Dependencias compartidas para inyección en FastAPI.
"""

from typing import Generator
from sqlalchemy.orm import Session
from db.session import get_session_factory


def get_db() -> Generator[Session, None, None]:
    """
    Dependency de FastAPI para obtener una sesión de BD.
    Se usa con Depends(get_db).
    La sesión se cierra automáticamente al terminar la request.
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        session.close()
