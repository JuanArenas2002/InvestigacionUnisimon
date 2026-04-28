"""
Autenticación de investigadores.
Endpoint de login basado en cédula.
"""

import os
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header, Request, status
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session
from jose import jwt, JWTError

from api.security.token_blocklist import blocklist

from db.models import Author, ResearcherCredential
from api.dependencies import get_db
from api.schemas.auth import (
    LoginRequest,
    TokenResponse,
    ErrorResponse,
    CreateCredentialRequest,
    ResearcherCredentialResponse,
)

logger = logging.getLogger(__name__)

# Configuración JWT
_raw_secret = os.getenv("JWT_SECRET_KEY", "")
if not _raw_secret:
    raise RuntimeError(
        "JWT_SECRET_KEY no está configurada. "
        "Define esta variable de entorno antes de iniciar la API."
    )
SECRET_KEY = _raw_secret
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

router = APIRouter(prefix="/auth", tags=["Autenticación de Investigadores"])

# 5 intentos por minuto por IP
_limiter = Limiter(key_func=get_remote_address)


# =============================================================
# HELPERS
# =============================================================

_INVALID_TOKEN = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Token inválido o expirado",
    headers={"WWW-Authenticate": "Bearer"},
)
_INVALID_CREDENTIALS = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Cédula o contraseña incorrecta",
)


def create_access_token(
    subject: str,
    expires_delta: Optional[timedelta] = None,
) -> tuple[str, datetime, str]:
    """Crea un JWT con jti único. Retorna (token, expiration, jti)."""
    if expires_delta is None:
        expires_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    expire = datetime.now(timezone.utc) + expires_delta
    jti = str(uuid.uuid4())
    to_encode = {"sub": subject, "exp": expire, "jti": jti}

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt, expire, jti


def verify_token(token: str) -> tuple[str, str]:
    """
    Verifica el JWT y comprueba la blocklist.
    Retorna (cedula, jti) o lanza HTTPException 401.
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise _INVALID_TOKEN

    cedula = payload.get("sub")
    jti = payload.get("jti")
    if not cedula or not jti:
        raise _INVALID_TOKEN

    if blocklist.is_revoked(jti):
        raise _INVALID_TOKEN

    return cedula, jti


def get_token_from_header(authorization: Optional[str] = Header(None)) -> str:
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
    """Dependency: valida token (incluye blocklist) y retorna el investigador."""
    cedula, _ = verify_token(token)
    author = db.query(Author).filter(Author.cedula == cedula).first()
    if not author:
        raise _INVALID_TOKEN
    return author


def get_token_jti(token: str = Depends(get_token_from_header)) -> tuple[str, str, datetime]:
    """Dependency: retorna (token_raw, jti, expires_at) para uso en logout."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        raise _INVALID_TOKEN
    jti = payload.get("jti")
    exp = payload.get("exp")
    if not jti or not exp:
        raise _INVALID_TOKEN
    expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
    return token, jti, expires_at


# =============================================================
# ENDPOINTS
# =============================================================

def _mask_cedula(cedula: str) -> str:
    """Enmascara la cédula para logs: '1234567890' -> '******7890'."""
    return f"{'*' * max(0, len(cedula) - 4)}{cedula[-4:]}" if cedula else "****"


@router.post(
    "/login",
    response_model=TokenResponse,
    responses={
        401: {"model": ErrorResponse, "description": "Credenciales inválidas"},
        410: {"model": ErrorResponse, "description": "Credencial expirada — contactar admin"},
        429: {"description": "Demasiados intentos. Espera 1 minuto."},
    },
    summary="Login de investigador",
    description="Autentica un investigador usando su cédula y contraseña. Límite: 5 intentos/minuto por IP.",
)
@_limiter.limit("5/minute")
def login(request: Request, body: LoginRequest, db: Session = Depends(get_db)) -> TokenResponse:
    masked = _mask_cedula(body.cedula)

    # 1. Buscar investigador — mismo error que contraseña incorrecta (anti-enumeración)
    author = db.query(Author).filter(Author.cedula == body.cedula).first()
    if not author:
        logger.warning("Login fallido — cédula no registrada: %s", masked)
        raise _INVALID_CREDENTIALS

    # 2. Buscar credencial activa
    credential = (
        db.query(ResearcherCredential)
        .filter(ResearcherCredential.author_id == author.id, ResearcherCredential.is_active == True)
        .first()
    )
    if not credential:
        logger.warning("Login fallido — sin credencial activa: %s", masked)
        raise _INVALID_CREDENTIALS

    # 3. Verificar expiración (este sí puede distinguirse: el usuario necesita contactar admin)
    if credential.is_expired():
        logger.warning("Login fallido — credencial expirada: %s", masked)
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="La credencial ha expirado. Contacta al administrador.")

    # 4. Verificar contraseña
    if not credential.verify_password(body.password):
        logger.warning("Login fallido — contraseña incorrecta: %s", masked)
        raise _INVALID_CREDENTIALS

    # 5. Generar token JWT con jti
    token, expires_at, _ = create_access_token(subject=body.cedula)

    # 6. Registrar último acceso
    credential.last_login = datetime.now(timezone.utc)
    db.commit()
    logger.info("Login exitoso — investigador ID %s", author.id)

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
