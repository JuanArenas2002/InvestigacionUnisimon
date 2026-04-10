-- Active: 1774386404707@@127.0.0.1@5432@reconciliacion_bibliografica
-- ============================================================
-- MIGRACIÓN v14: Índices para google_scholar y cvlac en external_ids
-- ============================================================
-- Prerequisito: migración v13 ya aplicada.
--
-- Cambios:
--   1. Agrega índice para external_ids->>'google_scholar'
--   2. Agrega índice para external_ids->>'cvlac'
--      (cvlac faltaba — solo existían openalex y scopus desde v9)
--
-- Esto permite búsquedas O(log n) por ID de Scholar o CvLAC
-- sin escanear toda la tabla.
-- ============================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────
-- 1. Índice para google_scholar
-- ─────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS ix_authors_google_scholar_id
    ON authors ((external_ids->>'google_scholar'))
    WHERE external_ids ? 'google_scholar';

-- ─────────────────────────────────────────────────────────────
-- 2. Índice para cvlac (faltaba desde v9)
-- ─────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS ix_authors_cvlac_id
    ON authors ((external_ids->>'cvlac'))
    WHERE external_ids ? 'cvlac';

COMMIT;

-- ─────────────────────────────────────────────────────────────
-- Verificación post-migración
-- ─────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                                    AS total_autores,
    COUNT(*) FILTER (WHERE external_ids ? 'openalex')          AS con_openalex,
    COUNT(*) FILTER (WHERE external_ids ? 'scopus')            AS con_scopus,
    COUNT(*) FILTER (WHERE external_ids ? 'cvlac')             AS con_cvlac,
    COUNT(*) FILTER (WHERE external_ids ? 'google_scholar')    AS con_google_scholar,
    COUNT(*) FILTER (WHERE external_ids ? 'wos')               AS con_wos
FROM authors;
