
-- Migration v11: Agregar tabla de credenciales de investigadores
-- Descripción:
--   - Tabla researcher_credentials para almacenar claves de investigadores
--   - Un investigador puede tener múltiples credenciales, pero solo una activa
--   - Factor de login: cédula del investigador (cedula en tabla authors)
--   - Auditoría temporal: created_at, activated_at, last_login, expires_at

BEGIN;

-- Crear tabla de credenciales de investigadores
CREATE TABLE IF NOT EXISTS researcher_credentials (
    id SERIAL PRIMARY KEY,
    author_id INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    password_hash VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,  
    -- Auditoría temporal
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    activated_at TIMESTAMP WITH TIME ZONE,
    last_login TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Crear índice único condicional: solo una credencial activa por investigador
CREATE UNIQUE INDEX IF NOT EXISTS uq_one_active_per_author 
    ON researcher_credentials(author_id, is_active) 
    WHERE is_active = true;

-- Índices para búsquedas rápidas
CREATE INDEX IF NOT EXISTS ix_researcher_credentials_author_id 
    ON researcher_credentials(author_id);

CREATE INDEX IF NOT EXISTS ix_researcher_credentials_is_active 
    ON researcher_credentials(is_active);

CREATE INDEX IF NOT EXISTS ix_researcher_credentials_author_active 
    ON researcher_credentials(author_id, is_active) 
    WHERE is_active = true;

-- Trigger para actualizar updated_at
CREATE OR REPLACE FUNCTION update_researcher_credentials_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_researcher_credentials_updated_at ON researcher_credentials;

CREATE TRIGGER trigger_researcher_credentials_updated_at
BEFORE UPDATE ON researcher_credentials
FOR EACH ROW
EXECUTE FUNCTION update_researcher_credentials_timestamp();

COMMIT;
