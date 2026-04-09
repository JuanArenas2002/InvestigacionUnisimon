"""
Autenticación de investigadores.
Endpoint de login basado en cédula.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from jose import jwt, JWTError

from db.models import Author, ResearcherCredential
from api.dependencies import get_db
from api.schemas.auth import (
    LoginRequest,
    TokenResponse,
    ChangePasswordRequest,
    ErrorResponse,
    CreateCredentialRequest,
    ResearcherCredentialResponse,
)

logger = logging.getLogger(__name__)

# Configuración JWT
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "supersecretkey-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

router = APIRouter(prefix="/auth", tags=["Autenticación de Investigadores"])


# =============================================================
# HELPERS
# =============================================================

def create_access_token(
    subject: str,
    expires_delta: Optional[timedelta] = None,
) -> tuple[str, datetime]:
    """
    Crea un JWT token de acceso.
    
    Returns:
        (token, expiration_datetime)
    """
    if expires_delta is None:
        expires_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode = {"sub": subject, "exp": expire}
    
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt, expire


def verify_token(token: str) -> str:
    """
    Verifica la validez de un JWT token.
    
    Returns:
        cedula del investigador si es válido
        
    Raises:
        HTTPException si el token es inválido
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        cedula = payload.get("sub")
        if cedula is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token inválido",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return cedula
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado o inválido",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_token_from_header(authorization: Optional[str] = None) -> str:
    """
    Extrae el token JWT del header Authorization.
    
    Formato esperado: "Bearer <token>"
    """
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header faltante",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Formato de Authorization inválido. Use: Bearer <token>",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return parts[1]


async def get_current_researcher(
    token: str = Depends(get_token_from_header),
    db: Session = Depends(get_db),
) -> Author:
    """
    Dependency que valida el token y retorna el investigador autenticado.
    
    Uso en otros endpoints:
    ```python
    @router.get("/me")
    def get_profile(researcher: Author = Depends(get_current_researcher)):
        return researcher
    ```
    """
    cedula = verify_token(token)
    author = db.query(Author).filter(Author.cedula == cedula).first()
    if not author:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Investigador no encontrado",
        )
    return author


# =============================================================
# ENDPOINTS
# =============================================================

@router.post(
    "/login",
    response_model=TokenResponse,
    responses={
        401: {"model": ErrorResponse, "description": "Credenciales inválidas"},
        404: {"model": ErrorResponse, "description": "Investigador no encontrado"},
        410: {"model": ErrorResponse, "description": "Credencial expirada"},
    },
    summary="Login de investigador",
    description="Autentica un investigador usando su cédula y contraseña.",
)
def login(request: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    """
    **Endpoint de login para investigadores.**
    
    **Factores:**
    - `cedula`: Cédula de ciudadanía del investigador
    - `password`: Contraseña
    
    **Retorna:**
    - JWT access token válido por 60 minutos (configurable)
    - Información del investigador
    
    **Errores:**
    - 404: Investigador/credencial no encontrado
    - 401: Credenciales inválidas o expiradas
    - 410: Credencial expirada
    """
    # 1. Buscar investigador por cédula
    author = db.query(Author).filter(Author.cedula == request.cedula).first()
    if not author:
        logger.warning(f"Intento de login con cédula no registrada: {request.cedula}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Investigador no encontrado",
        )
    
    # 2. Buscar credencial activa
    credential = (
        db.query(ResearcherCredential)
        .filter(
            ResearcherCredential.author_id == author.id,
            ResearcherCredential.is_active == True,
        )
        .first()
    )
    if not credential:
        logger.warning(f"Intento de login sin credencial activa para: {request.cedula}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay una credencial activa para este investigador",
        )
    
    # 3. Verificar expiración
    if credential.is_expired():
        logger.warning(f"Intento de login con credencial expirada: {request.cedula}")
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="La credencial ha expirado",
        )
    
    # 4. Verificar contraseña
    if not credential.verify_password(request.password):
        logger.warning(f"Intento de login con contraseña incorrecta: {request.cedula}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Cédula o contraseña incorrecta",
        )
    
    # 5. Generar token JWT
    token, expires_at = create_access_token(subject=request.cedula)
    
    # 6. Registrar último acceso
    credential.last_login = datetime.now(timezone.utc)
    db.commit()
    logger.info(f"Login exitoso para investigador: {request.cedula} (ID: {author.id})")
    
    # 7. Retornar respuesta
    expires_in = int((expires_at - datetime.now(timezone.utc)).total_seconds())
    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=expires_in,
        researcher_id=author.id,
        researcher_name=author.name,
        cedula=author.cedula,
    )


@router.post(
    "/change-password",
    summary="Cambiar contraseña",
    description="Permite que un investigador autenticado cambie su contraseña.",
    responses={
        401: {"model": ErrorResponse, "description": "No autorizado"},
        400: {"model": ErrorResponse, "description": "Contraseña actual incorrecta"},
    }
)
def change_password(
    request: ChangePasswordRequest,
    token: str = Depends(get_token_from_header),
    db: Session = Depends(get_db),
):
    """
    Cambia la contraseña del investigador autenticado.
    
    **Requiere:** Token JWT válido en header Authorization
    """
    # Verificar token
    cedula = verify_token(token)
    
    # Buscar investigador
    author = db.query(Author).filter(Author.cedula == cedula).first()
    if not author:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No autorizado")
    
    # Buscar credencial activa
    credential = (
        db.query(ResearcherCredential)
        .filter(
            ResearcherCredential.author_id == author.id,
            ResearcherCredential.is_active == True,
        )
        .first()
    )
    if not credential:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No autorizado")
    
    # Verificar contraseña anterior
    if not credential.verify_password(request.old_password):
        logger.warning(f"Intento de cambio de contraseña con clave anterior incorrecta: {cedula}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Contraseña actual incorrecta",
        )
    
    # Actualizar contraseña
    credential.password_hash = ResearcherCredential.hash_password(request.new_password)
    db.commit()
    logger.info(f"Contraseña actualizada para investigador: {cedula}")
    
    return {"message": "Contraseña actualizada exitosamente"}


@router.post(
    "/create-credential",
    response_model=ResearcherCredentialResponse,
    summary="Crear nueva credencial",
    description="Crea una nueva credencial para un investigador (requiere permiso de admin).",
)
def create_credential(
    request: CreateCredentialRequest,
    db: Session = Depends(get_db),
):
    """
    Crea una nueva credencial para un investigador.
    
    **Notas:**
    - Si deactivate_previous=True, desactiva la credencial anterior
    - Solo puede haber una credencial activa por investigador
    """
    # Buscar autor
    author = db.query(Author).filter(Author.id == request.author_id).first()
    if not author:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Investigador con ID {request.author_id} no encontrado",
        )
    
    # Desactivar credencial anterior si es necesario
    if request.deactivate_previous:
        active_credential = (
            db.query(ResearcherCredential)
            .filter(
                ResearcherCredential.author_id == request.author_id,
                ResearcherCredential.is_active == True,
            )
            .first()
        )
        if active_credential:
            active_credential.is_active = False
            logger.info(f"Credencial anterior desactivada para autor ID {request.author_id}")
    
    # Crear nueva credencial
    new_credential = ResearcherCredential(
        author_id=request.author_id,
        password_hash=ResearcherCredential.hash_password(request.password),
        is_active=True,
        activated_at=datetime.now(timezone.utc),
    )
    db.add(new_credential)
    db.commit()
    db.refresh(new_credential)
    
    logger.info(f"Nueva credencial creada para autor ID {request.author_id}")
    
    return ResearcherCredentialResponse.model_validate(new_credential)
