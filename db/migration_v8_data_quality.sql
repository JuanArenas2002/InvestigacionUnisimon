-- =============================================================
-- Migración v8: Calidad de datos en publicaciones canónicas
-- Ejecutar UNA sola vez sobre la base de datos existente.
-- Todos los ALTER son idempotentes (IF NOT EXISTS).
-- =============================================================

-- ──────────────────────────────────────────────────────────────
-- canonical_publications: detección de conflictos entre fuentes
-- ──────────────────────────────────────────────────────────────

ALTER TABLE canonical_publications
    ADD COLUMN IF NOT EXISTS field_conflicts   JSONB DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS citations_by_source JSONB DEFAULT '{}'::jsonb;

COMMENT ON COLUMN canonical_publications.field_conflicts IS
    'Conflictos entre fuentes por campo. Ej: {"is_open_access": {"openalex": "true", "scopus": "false"}, "doi": {"openalex": "10.1/a", "scopus": "10.2/b"}}';

COMMENT ON COLUMN canonical_publications.citations_by_source IS
    'Conteo de citas por fuente. Ej: {"openalex": 45, "scopus": 52, "wos": 48}. citation_count = max de este dict.';

-- Índice para buscar publicaciones con conflictos pendientes
CREATE INDEX IF NOT EXISTS ix_canon_has_conflicts
    ON canonical_publications ((field_conflicts IS NOT NULL AND field_conflicts != '{}'::jsonb));