-- ============================================================
-- MIGRACIÓN v11: Mejoras al sistema de autores
-- ============================================================
-- Prerequisito: migración v10 ya aplicada.
--
-- Cambios:
--   1. pg_trgm — extensión para búsqueda fuzzy por similitud de nombres
--   2. authors — índice GIN en external_ids, columna verification_status,
--                columna possible_duplicate_of
--   3. author_audit_log  — historial de cambios por autor
--   4. author_conflicts  — conflictos cuando fuentes difieren en un campo
--   5. author_institutions — rangos de fecha (start_year, end_year, is_current)
-- ============================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────
-- 0. Extensión pg_trgm (similarity())
-- ─────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ─────────────────────────────────────────────────────────────
-- 1. authors: índice GIN en external_ids
--    Acelera consultas del tipo:  external_ids['scopus'] = '...'
-- ─────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS ix_authors_external_ids_gin
    ON authors USING GIN (external_ids);

-- Índice GIN en normalized_name para similarity() eficiente
CREATE INDEX IF NOT EXISTS ix_authors_normalized_name_trgm
    ON authors USING GIN (normalized_name gin_trgm_ops);

-- ─────────────────────────────────────────────────────────────
-- 2. authors: columnas nuevas
-- ─────────────────────────────────────────────────────────────
ALTER TABLE authors
    ADD COLUMN IF NOT EXISTS verification_status VARCHAR(30)
        DEFAULT 'auto_detected'
        CHECK (verification_status IN (
            'auto_detected',   -- creado automáticamente por el pipeline
            'verified',        -- confirmado por un humano
            'needs_review',    -- marcado para revisión
            'flagged'          -- posible error / duplicado sospechoso
        )),
    ADD COLUMN IF NOT EXISTS possible_duplicate_of INTEGER
        REFERENCES authors(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS ix_authors_verification_status
    ON authors(verification_status);

CREATE INDEX IF NOT EXISTS ix_authors_possible_dup
    ON authors(possible_duplicate_of)
    WHERE possible_duplicate_of IS NOT NULL;

-- ─────────────────────────────────────────────────────────────
-- 3. author_audit_log: historial de cambios
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS author_audit_log (
    id          SERIAL PRIMARY KEY,
    author_id   INTEGER REFERENCES authors(id) ON DELETE CASCADE,
    change_type VARCHAR(30) NOT NULL
                CHECK (change_type IN (
                    'created', 'updated', 'merged_into',
                    'merged_from', 'verified', 'deleted'
                )),
    -- Campos antes/después del cambio (null en 'created')
    before_data JSONB,
    after_data  JSONB,
    -- Campos específicos que cambiaron  {campo: {before, after}}
    field_changes JSONB,
    -- Fuente que provocó el cambio (nombre del extractor o 'manual')
    source      VARCHAR(100),
    -- Usuario o proceso (opcional)
    changed_by  VARCHAR(200),
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_author_audit_author_id
    ON author_audit_log(author_id);
CREATE INDEX IF NOT EXISTS ix_author_audit_created
    ON author_audit_log(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_author_audit_change_type
    ON author_audit_log(change_type);

-- ─────────────────────────────────────────────────────────────
-- 4. author_conflicts: conflictos entre fuentes
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS author_conflicts (
    id              SERIAL PRIMARY KEY,
    author_id       INTEGER REFERENCES authors(id) ON DELETE CASCADE,
    field_name      VARCHAR(100) NOT NULL,
    existing_value  TEXT,
    new_value       TEXT,
    existing_source VARCHAR(100),
    new_source      VARCHAR(100),
    resolved        BOOLEAN DEFAULT FALSE,
    -- 'kept_existing' | 'used_new' | 'manual' | 'ignored'
    resolution      VARCHAR(50),
    resolved_at     TIMESTAMPTZ,
    resolved_by     VARCHAR(200),
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_author_conflicts_author
    ON author_conflicts(author_id);
CREATE INDEX IF NOT EXISTS ix_author_conflicts_unresolved
    ON author_conflicts(resolved, created_at DESC)
    WHERE resolved = FALSE;

-- ─────────────────────────────────────────────────────────────
-- 5. author_institutions: rangos de fecha
-- ─────────────────────────────────────────────────────────────
ALTER TABLE author_institutions
    ADD COLUMN IF NOT EXISTS start_year  INTEGER,
    ADD COLUMN IF NOT EXISTS end_year    INTEGER,
    ADD COLUMN IF NOT EXISTS is_current  BOOLEAN DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS ix_author_inst_current
    ON author_institutions(is_current)
    WHERE is_current = TRUE;

COMMIT;

-- ─────────────────────────────────────────────────────────────
-- Verificación
-- ─────────────────────────────────────────────────────────────
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'authors'
  AND column_name IN ('verification_status', 'possible_duplicate_of')
ORDER BY column_name;

SELECT table_name
FROM information_schema.tables
WHERE table_name IN ('author_audit_log', 'author_conflicts');
