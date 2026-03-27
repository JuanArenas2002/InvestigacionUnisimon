-- Active: 1774386404707@@127.0.0.1@5432@reconciliacion_bibliografica
-- ============================================================
-- MIGRACIÓN v10: Cobertura completa de campos por fuente
-- ============================================================
-- Prerequisito: migración v9 ya aplicada.
--
-- Cambios:
--   1. canonical_publications  — campos enriquecidos (abstract,
--      keywords, first_author, corresponding_author, coauthorships,
--      knowledge_area, cine_code, page_range, publisher,
--      journal_coverage, source_url)
--   2. datos_abiertos_records  — pesos, ventana, id_minciencias,
--      clase, subtipo
--   3. gruplac_records         — tabla nueva (fuente GrupLAC)
-- ============================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────
-- 1. canonical_publications: campos enriquecidos
-- ─────────────────────────────────────────────────────────────

ALTER TABLE canonical_publications
    ADD COLUMN IF NOT EXISTS abstract             TEXT,
    ADD COLUMN IF NOT EXISTS keywords             TEXT,
    ADD COLUMN IF NOT EXISTS source_url           VARCHAR(1000),
    ADD COLUMN IF NOT EXISTS page_range           VARCHAR(100),
    ADD COLUMN IF NOT EXISTS publisher            VARCHAR(300),
    ADD COLUMN IF NOT EXISTS journal_coverage     VARCHAR(100),
    ADD COLUMN IF NOT EXISTS knowledge_area       VARCHAR(300),
    ADD COLUMN IF NOT EXISTS cine_code            VARCHAR(50),
    ADD COLUMN IF NOT EXISTS first_author         VARCHAR(300),
    ADD COLUMN IF NOT EXISTS corresponding_author VARCHAR(300),
    ADD COLUMN IF NOT EXISTS coauthorships_count  INTEGER;

-- ─────────────────────────────────────────────────────────────
-- 2. datos_abiertos_records: pesos y campos faltantes
-- ─────────────────────────────────────────────────────────────

ALTER TABLE datos_abiertos_records
    ADD COLUMN IF NOT EXISTS id_minciencias  VARCHAR(100),
    ADD COLUMN IF NOT EXISTS clase           VARCHAR(100),
    ADD COLUMN IF NOT EXISTS subtipo         VARCHAR(100),
    ADD COLUMN IF NOT EXISTS peso_absoluto   FLOAT,
    ADD COLUMN IF NOT EXISTS peso_relativo   FLOAT,
    ADD COLUMN IF NOT EXISTS peso_escalafon  FLOAT,
    ADD COLUMN IF NOT EXISTS ventana         VARCHAR(20);

CREATE INDEX IF NOT EXISTS ix_datos_id_minciencias
    ON datos_abiertos_records(id_minciencias)
    WHERE id_minciencias IS NOT NULL;

-- ─────────────────────────────────────────────────────────────
-- 2b. scopus_records: publisher y journal_coverage
-- ─────────────────────────────────────────────────────────────

ALTER TABLE scopus_records
    ADD COLUMN IF NOT EXISTS publisher        VARCHAR(300),
    ADD COLUMN IF NOT EXISTS journal_coverage VARCHAR(100);

-- ─────────────────────────────────────────────────────────────
-- 2c. openalex_records: publisher
-- ─────────────────────────────────────────────────────────────

ALTER TABLE openalex_records
    ADD COLUMN IF NOT EXISTS publisher VARCHAR(300);

-- ─────────────────────────────────────────────────────────────
-- 3. gruplac_records: nueva tabla de fuente
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gruplac_records (
    id SERIAL PRIMARY KEY,
    -- Columnas específicas de GrupLAC
    gruplac_product_id    VARCHAR(255) UNIQUE,
    gruplac_group_id      VARCHAR(100),
    group_name            VARCHAR(500),
    group_leader          VARCHAR(300),
    group_classification  VARCHAR(10),
    group_institution     VARCHAR(500),
    author_link_status    VARCHAR(100),
    author_role           VARCHAR(100),
    product_type          VARCHAR(100),
    abstract              TEXT,
    -- Columnas comunes del mixin (obligatorias)
    dedup_hash            VARCHAR(64) UNIQUE,
    doi                   VARCHAR(255),
    title                 TEXT,
    normalized_title      TEXT,
    publication_year      INTEGER,
    publication_date      VARCHAR(20),
    publication_type      VARCHAR(100),
    source_journal        VARCHAR(500),
    issn                  VARCHAR(20),
    language              VARCHAR(10),
    is_open_access        BOOLEAN,
    oa_status             VARCHAR(50),
    citation_count        INTEGER DEFAULT 0,
    authors_text          TEXT,
    normalized_authors    TEXT,
    url                   VARCHAR(1000),
    raw_data              JSONB,
    status                VARCHAR(30) DEFAULT 'pending'
                          CHECK (status IN ('pending','matched','new_canonical','manual_review','rejected')),
    match_type            VARCHAR(50),
    match_score           FLOAT,
    reconciled_at         TIMESTAMPTZ,
    canonical_publication_id INTEGER REFERENCES canonical_publications(id) ON DELETE SET NULL,
    created_at            TIMESTAMPTZ DEFAULT now(),
    updated_at            TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_gruplac_year_title
    ON gruplac_records(publication_year, normalized_title);
CREATE INDEX IF NOT EXISTS ix_gruplac_canonical
    ON gruplac_records(canonical_publication_id);
CREATE INDEX IF NOT EXISTS ix_gruplac_group_id
    ON gruplac_records(gruplac_group_id);
CREATE INDEX IF NOT EXISTS ix_gruplac_status
    ON gruplac_records(status);

COMMIT;

-- ─────────────────────────────────────────────────────────────
-- Verificación
-- ─────────────────────────────────────────────────────────────
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_name = 'canonical_publications'
  AND column_name IN (
      'abstract','keywords','source_url','page_range','publisher',
      'journal_coverage','knowledge_area','cine_code',
      'first_author','corresponding_author','coauthorships_count'
  )
ORDER BY column_name;

SELECT COUNT(*) AS total_gruplac FROM gruplac_records;
