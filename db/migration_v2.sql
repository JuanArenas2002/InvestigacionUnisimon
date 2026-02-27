-- =============================================================
-- Migración: Mejoras de estructura v2
-- Fecha: 2026-02-23
-- Descripción:
--   1. Nuevas tablas: journals, institutions, author_institutions
--   2. updated_at en authors y external_records
--   3. journal_id FK en canonical_publications
--   4. JSON → JSONB en external_records.raw_data y reconciliation_log.match_details
--   5. Índices GIN full-text en títulos y autores
--   6. Índices parciales en openalex_id, scopus_id
--   7. Check constraints de estado (status, action)
--
-- NOTA: Toda sentencia es idempotente (IF NOT EXISTS / DO $$).
--       Se puede ejecutar múltiples veces sin riesgo.
-- =============================================================

BEGIN;

-- =============================================================
-- 1. NUEVAS TABLAS
-- =============================================================

CREATE TABLE IF NOT EXISTS journals (
    id          SERIAL PRIMARY KEY,
    issn        VARCHAR(20) UNIQUE,
    name        VARCHAR(500) NOT NULL,
    publisher   VARCHAR(300),
    country     VARCHAR(100),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS institutions (
    id          SERIAL PRIMARY KEY,
    ror_id      VARCHAR(100) UNIQUE,
    name        VARCHAR(500) NOT NULL,
    country     VARCHAR(100),
    type        VARCHAR(50),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS author_institutions (
    id              SERIAL PRIMARY KEY,
    author_id       INTEGER NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    institution_id  INTEGER NOT NULL REFERENCES institutions(id) ON DELETE CASCADE,
    CONSTRAINT uq_author_institution UNIQUE (author_id, institution_id)
);


-- =============================================================
-- 2. COLUMNAS FALTANTES
-- =============================================================

-- updated_at en authors
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'authors' AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE authors ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();
    END IF;
END $$;

-- updated_at en external_records
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'external_records' AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE external_records ADD COLUMN updated_at TIMESTAMPTZ DEFAULT now();
    END IF;
END $$;

-- journal_id FK en canonical_publications
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'canonical_publications' AND column_name = 'journal_id'
    ) THEN
        ALTER TABLE canonical_publications ADD COLUMN journal_id INTEGER
            REFERENCES journals(id) ON DELETE SET NULL;
    END IF;
END $$;


-- =============================================================
-- 3. JSON → JSONB
-- =============================================================

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'external_records'
          AND column_name = 'raw_data'
          AND data_type = 'json'
    ) THEN
        ALTER TABLE external_records
            ALTER COLUMN raw_data TYPE JSONB USING raw_data::JSONB;
    END IF;
END $$;

DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'reconciliation_log'
          AND column_name = 'match_details'
          AND data_type = 'json'
    ) THEN
        ALTER TABLE reconciliation_log
            ALTER COLUMN match_details TYPE JSONB USING match_details::JSONB;
    END IF;
END $$;


-- =============================================================
-- 4. ÍNDICES
-- =============================================================

-- Índices únicos
CREATE UNIQUE INDEX IF NOT EXISTS ix_ext_dedup_hash
    ON external_records (dedup_hash);

CREATE UNIQUE INDEX IF NOT EXISTS ix_canon_doi_unique
    ON canonical_publications (doi) WHERE doi IS NOT NULL;

-- Índices parciales para IDs externos de autores
CREATE INDEX IF NOT EXISTS ix_authors_openalex
    ON authors (openalex_id) WHERE openalex_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_authors_scopus
    ON authors (scopus_id) WHERE scopus_id IS NOT NULL;

-- Índices en publication_authors (para JOINs rápidos)
CREATE INDEX IF NOT EXISTS ix_pub_authors_author
    ON publication_authors (author_id);

CREATE INDEX IF NOT EXISTS ix_pub_authors_pub
    ON publication_authors (publication_id);

-- Índices en reconciliation_log
CREATE INDEX IF NOT EXISTS ix_recon_log_ext
    ON reconciliation_log (external_record_id);

CREATE INDEX IF NOT EXISTS ix_recon_log_canon
    ON reconciliation_log (canonical_publication_id);

-- Índice en journal_id
CREATE INDEX IF NOT EXISTS ix_canon_journal
    ON canonical_publications (journal_id) WHERE journal_id IS NOT NULL;

-- Índices GIN full-text (búsqueda rápida en títulos y autores)
CREATE INDEX IF NOT EXISTS ix_canon_title_fts
    ON canonical_publications
    USING GIN (to_tsvector('spanish', coalesce(title, '')));

CREATE INDEX IF NOT EXISTS ix_ext_title_fts
    ON external_records
    USING GIN (to_tsvector('spanish', coalesce(title, '')));

CREATE INDEX IF NOT EXISTS ix_authors_name_fts
    ON authors
    USING GIN (to_tsvector('spanish', coalesce(name, '')));


-- =============================================================
-- 5. CHECK CONSTRAINTS
-- =============================================================

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ck_ext_status'
    ) THEN
        ALTER TABLE external_records
        ADD CONSTRAINT ck_ext_status
        CHECK (status IN ('pending','matched','new_canonical','manual_review','rejected'));
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'ck_log_action'
    ) THEN
        ALTER TABLE reconciliation_log
        ADD CONSTRAINT ck_log_action
        CHECK (action IN ('linked_existing','created_new','flagged_review','rejected','manual_resolved'));
    END IF;
END $$;


COMMIT;
