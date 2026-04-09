"""
Esquemas Pydantic para autenticación y credenciales de investigadores.
"""

from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


# =============================================================
# REQUESTS
# =============================================================

class LoginRequest(BaseModel):
    """
    Solicitud de login de investigador.
    
    Factores:
    - cedula: Cédula de ciudadanía del investigador
    - password: Contraseña
    """
    cedula: str = Field(
        ...,
        min_length=5,
        max_length=50,
        description="Cédula de ciudadanía del investigador"
    )
    password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Contraseña del investigador"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "cedula": "1234567890",
                "password": "MiPassword123!"
            }
        }


class ChangePasswordRequest(BaseModel):
    """Solicitud para cambiar contraseña."""
    old_password: str = Field(..., description="Contraseña actual")
    new_password: str = Field(
        ...,
        min_length=8,
        description="Nueva contraseña (mínimo 8 caracteres)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "old_password": "MiPassword123!",
                "new_password": "NuevaPassword456!"
            }
        }


class CreateCredentialRequest(BaseModel):
    """Solicitud para crear una nueva credencial (por administrador o el mismo investigador)."""
    author_id: int = Field(..., description="ID del autor/investigador")
    password: str = Field(
        ...,
        min_length=8,
        description="Nueva contraseña"
    )
    deactivate_previous: bool = Field(
        True,
        description="Si es True, desactiva la credencial anterior"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "author_id": 42,
                "password": "NuevaPassword789!",
                "deactivate_previous": True
            }
        }


# =============================================================
# RESPONSES
# =============================================================

class TokenResponse(BaseModel):
    """Respuesta después de login exitoso."""
    access_token: str = Field(..., description="JWT token de acceso")
    token_type: str = Field(default="bearer", description="Tipo de token")
    expires_in: int = Field(..., description="Segundos hasta expiración")
    
    # Datos del investigador
    researcher_id: int = Field(..., description="ID del investigador (author_id)")
    researcher_name: str = Field(..., description="Nombre del investigador")
    cedula: str = Field(..., description="Cédula del investigador")

    class Config:
        json_schema_extra = {
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer",
                "expires_in": 3600,
                "researcher_id": 42,
                "researcher_name": "Juan Pérez",
                "cedula": "1234567890"
            }
        }


class ResearcherCredentialResponse(BaseModel):
    """Información de una credencial de investigador."""
    id: int
    author_id: int
    is_active: bool
    created_at: datetime
    activated_at: Optional[datetime]
    last_login: Optional[datetime]
    expires_at: Optional[datetime]

    class Config:
        from_attributes = True


class ResearcherCredentialDetailResponse(ResearcherCredentialResponse):
    """Detalles completos de una credencial con info del autor."""
    researcher_name: str
    cedula: str
    
    class Config:
        from_attributes = True


class LoginSuccessResponse(BaseModel):
    """Respuesta detallada de login exitoso."""
    message: str = "Login exitoso"
    token: TokenResponse


class ErrorResponse(BaseModel):
    """Respuesta de error estándar."""
    detail: str
    error_code: str = Field(..., description="Código de error único")

    class Config:
        json_schema_extra = {
            "example": {
                "detail": "Credenciales inválidas",
                "error_code": "INVALID_CREDENTIALS"
            }
        }
