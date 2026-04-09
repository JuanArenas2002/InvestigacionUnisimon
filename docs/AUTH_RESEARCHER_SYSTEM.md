# 🔐 Autenticación de Investigadores

**Implementado:** 9 de abril de 2026

## Descripción

Sistema de autenticación para investigadores basado en:
- **Cédula** como factor de login (unique identifier)
- **Contraseña** hasheada con bcrypt
- **JWT tokens** para autorización de endpoints
- **Multi-credenciales**: Un investigador puede tener múltiples claves, pero solo una activa

---

## 📊 Estructura de Base de Datos

### Tabla: `researcher_credentials`

```sql
CREATE TABLE researcher_credentials (
    id SERIAL PRIMARY KEY,
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    password_hash VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    
    -- Auditoría
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TIMESTAMP WITH TIME ZONE,
    last_login TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Restricción: solo una activa por investigador
    CONSTRAINT uq_one_active_per_author UNIQUE (author_id, is_active) WHERE is_active = true
);
```

### Atributos

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id` | INTEGER | Primary Key |
| `author_id` | INTEGER | Referencia al tabla `authors` |
| `password_hash` | VARCHAR(255) | Hash bcrypt de la contraseña |
| `is_active` | BOOLEAN | Solo una credencial activa por investigador |
| `created_at` | TIMESTAMP | Cuándo se creó |
| `activated_at` | TIMESTAMP | Cuándo se activó |
| `last_login` | TIMESTAMP | Último acceso exitoso |
| `expires_at` | TIMESTAMP | Expiración opcional |
| `updated_at` | TIMESTAMP | Última actualización |

---

## 🔌 Endpoints de API

### 1. **POST /api/auth/login** — Login

**Autenticación de investigador**

#### Request
```json
{
  "cedula": "1234567890",
  "password": "Password123!"
}
```

#### Response (200 OK)
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer",
  "expires_in": 3600,
  "researcher_id": 42,
  "researcher_name": "Juan Pérez García",
  "cedula": "1234567890"
}
```

#### Posibles Errores

| Status | Error | Descripción |
|--------|-------|-------------|
| 404 | Investigador no encontrado | Cédula no existe en BD |
| 404 | No hay credencial activa | Investigador sin clave activa |
| 410 | Credencial expirada | Fecha expires_at ha pasado |
| 401 | Cédula o contraseña incorrecta | Datos inválidos |

---

### 2. **POST /api/auth/change-password** — Cambiar Contraseña

**Permite cambiar la contraseña (requiere token válido)**

#### Headers
```
Authorization: Bearer <access_token>
```

#### Request
```json
{
  "old_password": "Password123!",
  "new_password": "NewPassword456!"
}
```

#### Response (200 OK)
```json
{
  "message": "Contraseña actualizada exitosamente"
}
```

#### Posibles Errores

| Status | Error | Descripción |
|--------|-------|-------------|
| 401 | No autorizado | Token ausente o inválido |
| 400 | Contraseña actual incorrecta | old_password no coincide |

---

### 3. **POST /api/auth/create-credential** — Crear Nueva Credencial

**Crea una nueva clave para un investigador (típicamente admin)**

#### Request
```json
{
  "author_id": 42,
  "password": "NuevaPassword789!",
  "deactivate_previous": true
}
```

#### Response (200 OK)
```json
{
  "id": 15,
  "author_id": 42,
  "is_active": true,
  "created_at": "2026-04-09T15:30:00+00:00",
  "activated_at": "2026-04-09T15:30:00+00:00",
  "last_login": null,
  "expires_at": null
}
```

#### Posibles Errores

| Status | Error | Descripción |
|--------|-------|-------------|
| 404 | Investigador no encontrado | author_id no existe |

---

## 🧪 Testing / Pruebas

### 1. **Crear investigador de prueba**

```bash
python scripts/create_test_researcher.py
```

Output:
```
✓ Investigador creado: Juan Pérez García (Cédula: 1234567890)
✓ Credencial creada (ID: 1)
============================================================
DATOS DE PRUEBA - GUARDAR EN LUGAR SEGURO
============================================================
Cédula:      1234567890
Contraseña:  Password123!
Endpoint:    POST /api/auth/login
============================================================
```

### 2. **Login en Swagger UI**

1. Ir a: http://localhost:8000/docs
2. Buscar endpoint `POST /api/auth/login`
3. Click en "Try it out"
4. Ingresar:
   ```json
   {
     "cedula": "1234567890",
     "password": "Password123!"
   }
   ```
5. Click "Execute"
6. Copiar el `access_token`

### 3. **Login con cURL**

```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "cedula": "1234567890",
    "password": "Password123!"
  }'
```

### 4. **Usar token en otros endpoints**

Una vez tienes el token, úsalo en el header `Authorization`:

```bash
curl -X GET http://localhost:8000/api/authors/me \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

---

## 🔑 JWT Token Structure

Tu `access_token` es un JWT con esta estructura:

```json
{
  "sub": "1234567890",  // cedula del investigador
  "exp": 1712707800     // timestamp de expiración
}
```

**Tiempo de vida**: 60 minutos (configurable via `ACCESS_TOKEN_EXPIRE_MINUTES` env var)

---

## 🛡️ Seguridad

### Contraseñas

- ✅ Hasheadas con **bcrypt** (salt rounds: 12)
- ✅ Nunca se almacenan en texto plano
- ✅ Validadas en cada login

### Tokens JWT

- ✅ Firmados con `JWT_SECRET_KEY` (env var requerida)
- ✅ Expiración temporal
- ✅ Se requiere en header `Authorization: Bearer <token>`

### Auditoría

- ✅ Cada login registra `last_login`
- ✅ Cada cambio de clave registra `updated_at`
- ✅ `created_at` inmutable

### Restricciones

- ✅ Solo **una credencial activa** por investigador
- ✅ Puede haber expiración (`expires_at`)
- ✅ Investigador puede cambiar su propia clave

---

## 🚀 Integración con Otros Endpoints

Ahora puedes proteger endpoints usando la dependency `get_current_researcher`:

```python
from api.routers.auth import get_current_researcher
from db.models import Author

@router.get("/me")
def get_profile(researcher: Author = Depends(get_current_researcher)):
    """Retorna el perfil del investigador autenticado."""
    return researcher
```

---

## 📝 Variables de Entorno Requeridas

```bash
# .env (o variables del sistema)

JWT_SECRET_KEY=tu-clave-secreta-muy-larga
ACCESS_TOKEN_EXPIRE_MINUTES=60  # opcional, default: 60

DB_HOST=localhost
DB_PORT=5432
DB_NAME=reconciliacion_bibliografica
DB_USER=postgres
DB_PASSWORD=tu-password-bd
```

---

## 🔄 Flujo de Login Completo

```
1. Usuario ingresa cedula + password
        ↓
2. API busca Author por cedula
   ├─ No encontrado? → 404
   └─ Encontrado
        ↓
3. Buscar ResearcherCredential activa
   ├─ No hay? → 404
   └─ Hay
        ↓
4. Verificar expiración
   ├─ Expirada? → 410
   └─ Válida
        ↓
5. Verificar contraseña (bcrypt)
   ├─ Incorrecta? → 401
   └─ Correcta
        ↓
6. Generar JWT token
        ↓
7. Registrar last_login
        ↓
8. Retornar token + info investigador ✅
```

---

## ⚠️ Notas Importantes

1. **Migración SQL**: Ejecutar `db/migration_v11_researcher_credentials.sql`
   ```bash
   psql -U postgres < db/migration_v11_researcher_credentials.sql
   ```

2. **Cambiar JWT_SECRET_KEY en producción**: No dejes la clave default
   
3. **HTTPS obligatorio en producción**: Tokens en header HTTP deben transmitirse encriptados

4. **Rate limiting**: Considera agregar rate limiting a `/api/auth/login` para evitar brute-force

5. **Auditoría**: `last_login` es útil para detectar accesos sospechosos

---

## 📚 Referencias

- [Pydantic v2 Docs](https://docs.pydantic.dev/latest/)
- [Python-jose JWT](https://github.com/mpdavis/python-jose)
- [bcrypt](https://pypi.org/project/bcrypt/)
- [FastAPI Security](https://fastapi.tiangolo.com/tutorial/security/)

---

**Última actualización:** 9 de abril de 2026
