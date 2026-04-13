-- Migration: Google Scholar records table
-- Version: db_v15_google_scholar (fixed)
-- Date: 2026-04-10

BEGIN;

CREATE TABLE IF NOT EXISTS google_scholar_records (
    id SERIAL PRIMARY KEY,

    -- Identificadores
    google_scholar_id VARCHAR(50) NOT NULL UNIQUE,
    scholar_profile_id VARCHAR(50),

    -- Metadatos principales
    title VARCHAR(1000) NOT NULL,
    authors_json JSONB DEFAULT '[]'::jsonb,
    publication_year INTEGER,
    publication_date DATE,

    -- Publicación
    publication_type VARCHAR(100),
    source_journal VARCHAR(500),

    -- Identificadores bibliográficos
    issn VARCHAR(20),
    doi VARCHAR(100) UNIQUE,

    -- Métricas
    citation_count INTEGER DEFAULT 0,
    citations_by_year JSONB DEFAULT '{}'::jsonb,

    -- URLs
    url TEXT,

    -- Control
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    raw_data JSONB DEFAULT '{}'::jsonb,
    extracted_at TIMESTAMP,

    -- Auditoría
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Relación
    canonical_publication_id INTEGER 
        REFERENCES canonical_publications(id) 
        ON DELETE SET NULL
);

-- Índices (optimizados, sin redundancia)
CREATE INDEX idx_google_scholar_status 
    ON google_scholar_records(status);

CREATE INDEX idx_google_scholar_year 
    ON google_scholar_records(publication_year);

CREATE INDEX idx_google_scholar_canonical 
    ON google_scholar_records(canonical_publication_id);

CREATE INDEX idx_google_scholar_doi 
    ON google_scholar_records(doi);

-- Trigger para updated_at automático
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_google_scholar_updated_at
BEFORE UPDATE ON google_scholar_records
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

COMMIT;