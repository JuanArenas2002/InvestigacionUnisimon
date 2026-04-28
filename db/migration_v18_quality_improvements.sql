-- ============================================================
-- MIGRACIÓN v18: Mejoras de calidad de datos
-- ============================================================
-- Prerequisito: migración v17 ya aplicada.
--
-- Cambios:
--   1. canonical_publications — índice UNIQUE parcial en doi,
--      índice en normalized_title (trgm), publication_year
--   2. publication_authors     — columna role
--   3. possible_duplicate_pairs — columnas confidence_level,
--      reviewed_by, reviewed_at
--   4. Índices en dedup_hash y match_score en todas las source tables
--   5. author_aliases          — tabla nueva
-- ============================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────
-- 1. canonical_publications: índices faltantes
-- ─────────────────────────────────────────────────────────────

-- DOI: UNIQUE parcial (NULLs permitidos, duplicados de DOI no)
CREATE UNIQUE INDEX IF NOT EXISTS uq_canonical_doi
    ON canonical_publications(doi)
    WHERE doi IS NOT NULL AND doi != '';

-- normalized_title: búsqueda fuzzy (requiere pg_trgm de v11)
CREATE INDEX IF NOT EXISTS ix_canonical_normalized_title_trgm
    ON canonical_publications USING GIN (normalized_title gin_trgm_ops);

-- publication_year: filtros y agrupaciones frecuentes
CREATE INDEX IF NOT EXISTS ix_canonical_pub_year
    ON canonical_publications(publication_year)
    WHERE publication_year IS NOT NULL;

-- ─────────────────────────────────────────────────────────────
-- 2. publication_authors: columna role
-- ─────────────────────────────────────────────────────────────
ALTER TABLE publication_authors
    ADD COLUMN IF NOT EXISTS role VARCHAR(50)
        CHECK (role IN (
            'first_author',
            'corresponding_author',
            'co_author',
            'last_author',
            'other'
        ));

CREATE INDEX IF NOT EXISTS ix_pub_authors_role
    ON publication_authors(role)
    WHERE role IS NOT NULL;

-- ─────────────────────────────────────────────────────────────
-- 3. possible_duplicate_pairs: campos de revisión
-- ─────────────────────────────────────────────────────────────
ALTER TABLE possible_duplicate_pairs
    ADD COLUMN IF NOT EXISTS confidence_level VARCHAR(20)
        CHECK (confidence_level IN ('high', 'medium', 'low')),
    ADD COLUMN IF NOT EXISTS reviewed_by  VARCHAR(200),
    ADD COLUMN IF NOT EXISTS reviewed_at  TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS ix_dup_pairs_confidence
    ON possible_duplicate_pairs(confidence_level)
    WHERE confidence_level IS NOT NULL;

-- ─────────────────────────────────────────────────────────────
-- 4. Índices en dedup_hash y match_score — source tables
-- ─────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS ix_cvlac_dedup_hash
    ON cvlac_records(dedup_hash) WHERE dedup_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_cvlac_match_score
    ON cvlac_records(match_score) WHERE match_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_scopus_dedup_hash
    ON scopus_records(dedup_hash) WHERE dedup_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_scopus_match_score
    ON scopus_records(match_score) WHERE match_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_wos_dedup_hash
    ON wos_records(dedup_hash) WHERE dedup_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_wos_match_score
    ON wos_records(match_score) WHERE match_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_openalex_dedup_hash
    ON openalex_records(dedup_hash) WHERE dedup_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_openalex_match_score
    ON openalex_records(match_score) WHERE match_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_datos_dedup_hash
    ON datos_abiertos_records(dedup_hash) WHERE dedup_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_datos_match_score
    ON datos_abiertos_records(match_score) WHERE match_score IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_gruplac_dedup_hash
    ON gruplac_records(dedup_hash) WHERE dedup_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_gruplac_match_score
    ON gruplac_records(match_score) WHERE match_score IS NOT NULL;

-- ─────────────────────────────────────────────────────────────
-- 5. author_aliases: variantes de nombre por fuente
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS author_aliases (
    id              SERIAL PRIMARY KEY,
    author_id       INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    alias_name      VARCHAR(300) NOT NULL,
    normalized_alias VARCHAR(300),
    source          VARCHAR(100),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_author_alias UNIQUE (author_id, alias_name)
);

CREATE INDEX IF NOT EXISTS ix_author_aliases_author_id
    ON author_aliases(author_id);

CREATE INDEX IF NOT EXISTS ix_author_aliases_normalized
    ON author_aliases USING GIN (normalized_alias gin_trgm_ops);

COMMIT;

-- ─────────────────────────────────────────────────────────────
-- Verificación
-- ─────────────────────────────────────────────────────────────
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename IN (
    'canonical_publications',
    'publication_authors',
    'possible_duplicate_pairs',
    'author_aliases'
)
ORDER BY tablename, indexname;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'publication_authors'
  AND column_name = 'role';

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'possible_duplicate_pairs'
  AND column_name IN ('confidence_level', 'reviewed_by', 'reviewed_at')
ORDER BY column_name;

SELECT COUNT(*) AS aliases_count FROM author_aliases;
