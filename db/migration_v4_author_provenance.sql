-- ============================================================
-- Migración v4: field_provenance para autores
-- Agrega columna JSONB para rastrear qué fuente aportó cada
-- dato del autor (name, orcid, scopus_id, openalex_id, etc.)
-- ============================================================

-- 1. Agregar columna
ALTER TABLE authors
ADD COLUMN IF NOT EXISTS field_provenance JSONB;

COMMENT ON COLUMN authors.field_provenance IS
  '{campo: fuente} ej: {"orcid": "openalex", "scopus_id": "scopus", "name": "cvlac"}';

-- 2. Índice GIN para consultas sobre el JSON
CREATE INDEX IF NOT EXISTS ix_authors_field_provenance
ON authors USING GIN (field_provenance);
