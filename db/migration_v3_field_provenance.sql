-- ============================================================
-- Migración v3: Agregar campo field_provenance a canonical_publications
-- Registra qué fuente (openalex, scopus, wos, cvlac, datos_abiertos)
-- aportó cada campo al registro canónico.
-- ============================================================

ALTER TABLE canonical_publications
ADD COLUMN IF NOT EXISTS field_provenance JSONB DEFAULT '{}';

COMMENT ON COLUMN canonical_publications.field_provenance IS
    'Dict campo→fuente. Ej: {"doi":"openalex","source_journal":"scopus","citation_count":"wos"}';

-- ============================================================
-- Backfill: Para publicaciones existentes que solo tienen 1 fuente,
-- se puede inferir la procedencia automáticamente.
-- ============================================================

UPDATE canonical_publications cp
SET field_provenance = (
    SELECT jsonb_build_object(
        'title', er.source_name,
        'doi', CASE WHEN cp.doi IS NOT NULL THEN er.source_name ELSE NULL END,
        'publication_year', CASE WHEN cp.publication_year IS NOT NULL THEN er.source_name ELSE NULL END,
        'source_journal', CASE WHEN cp.source_journal IS NOT NULL THEN er.source_name ELSE NULL END,
        'publication_type', CASE WHEN cp.publication_type IS NOT NULL THEN er.source_name ELSE NULL END,
        'is_open_access', CASE WHEN cp.is_open_access IS NOT NULL THEN er.source_name ELSE NULL END,
        'issn', CASE WHEN cp.issn IS NOT NULL THEN er.source_name ELSE NULL END,
        'citation_count', CASE WHEN cp.citation_count > 0 THEN er.source_name ELSE NULL END,
        'publication_date', CASE WHEN cp.publication_date IS NOT NULL THEN er.source_name ELSE NULL END
    )
    -- Eliminar claves con valor null del JSONB
    - (
        SELECT COALESCE(array_agg(key), '{}')
        FROM jsonb_each(jsonb_build_object(
            'title', er.source_name,
            'doi', CASE WHEN cp.doi IS NOT NULL THEN er.source_name ELSE NULL END,
            'publication_year', CASE WHEN cp.publication_year IS NOT NULL THEN er.source_name ELSE NULL END,
            'source_journal', CASE WHEN cp.source_journal IS NOT NULL THEN er.source_name ELSE NULL END,
            'publication_type', CASE WHEN cp.publication_type IS NOT NULL THEN er.source_name ELSE NULL END,
            'is_open_access', CASE WHEN cp.is_open_access IS NOT NULL THEN er.source_name ELSE NULL END,
            'issn', CASE WHEN cp.issn IS NOT NULL THEN er.source_name ELSE NULL END,
            'citation_count', CASE WHEN cp.citation_count > 0 THEN er.source_name ELSE NULL END,
            'publication_date', CASE WHEN cp.publication_date IS NOT NULL THEN er.source_name ELSE NULL END
        )) WHERE value = 'null'::jsonb
    )
    FROM external_records er
    WHERE er.canonical_publication_id = cp.id
      AND er.status IN ('matched', 'new_canonical')
    ORDER BY er.created_at ASC
    LIMIT 1
)
WHERE cp.field_provenance IS NULL OR cp.field_provenance = '{}';
