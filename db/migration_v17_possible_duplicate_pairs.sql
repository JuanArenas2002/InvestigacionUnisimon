-- Migration v17: tabla de pares de publicaciones posiblemente duplicadas
-- Persiste pares detectados durante reconciliación para consulta posterior.

CREATE TABLE IF NOT EXISTS possible_duplicate_pairs (
    id                  SERIAL PRIMARY KEY,
    canonical_id_1      INTEGER NOT NULL REFERENCES canonical_publications(id) ON DELETE CASCADE,
    canonical_id_2      INTEGER NOT NULL REFERENCES canonical_publications(id) ON DELETE CASCADE,
    similarity_score    FLOAT   NOT NULL,
    match_method        VARCHAR(50) NOT NULL DEFAULT 'title',
    detected_at         TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status              VARCHAR(20) NOT NULL DEFAULT 'pending',
    -- 'pending' | 'merged' | 'dismissed'
    CONSTRAINT uq_dup_pair UNIQUE (canonical_id_1, canonical_id_2),
    CONSTRAINT chk_dup_order CHECK (canonical_id_1 < canonical_id_2)
);

CREATE INDEX IF NOT EXISTS ix_dup_pairs_id1 ON possible_duplicate_pairs (canonical_id_1);
CREATE INDEX IF NOT EXISTS ix_dup_pairs_id2 ON possible_duplicate_pairs (canonical_id_2);
CREATE INDEX IF NOT EXISTS ix_dup_pairs_status ON possible_duplicate_pairs (status) WHERE status = 'pending';

-- Verificar
SELECT COUNT(*) AS pairs_count FROM possible_duplicate_pairs;
