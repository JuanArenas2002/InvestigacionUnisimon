-- Active: 1774386404707@@127.0.0.1@5432@reconciliacion_bibliografica
-- ============================================================
-- MIGRACIÓN v9: Arquitectura de fuentes como plugins
-- ============================================================
-- Ejecutar UNA SOLA VEZ contra la base de datos de producción.
-- Prerequisito: migración v8 ya aplicada.
--
-- Cambios:
--   1. Agrega columna  authors.external_ids  JSONB
--   2. Migra datos de las columnas individuales  openalex_id,
--      scopus_id, wos_id, cvlac_id  al nuevo JSONB
--   3. Crea índices GIN para búsquedas eficientes por clave
--   4. Elimina las columnas individuales de ID (ya no necesarias)
--
-- Después de esta migración, agregar una nueva fuente solo
-- requiere crear su archivo sources/nueva_fuente.py — la tabla
-- de autores NUNCA necesitará modificarse.
-- ============================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────
-- 1. Agregar columna external_ids
-- ─────────────────────────────────────────────────────────────
ALTER TABLE authors
    ADD COLUMN IF NOT EXISTS external_ids JSONB DEFAULT '{}'::jsonb;

-- ─────────────────────────────────────────────────────────────
-- 2. Migrar datos de columnas individuales → JSONB
--    Solo migra valores no nulos y no vacíos.
-- ─────────────────────────────────────────────────────────────
UPDATE authors
SET external_ids = (
    SELECT jsonb_strip_nulls(jsonb_build_object(
        'openalex', NULLIF(TRIM(openalex_id), ''),
        'scopus',   NULLIF(TRIM(scopus_id),   ''),
        'wos',      NULLIF(TRIM(wos_id),       ''),
        'cvlac',    NULLIF(TRIM(cvlac_id),     '')
    ))
)
WHERE
    openalex_id IS NOT NULL OR
    scopus_id   IS NOT NULL OR
    wos_id      IS NOT NULL OR
    cvlac_id    IS NOT NULL;

-- ─────────────────────────────────────────────────────────────
-- 3. Índice GIN para búsquedas rápidas (? operator, @> operator)
-- ─────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS ix_authors_external_ids_gin
    ON authors USING gin(external_ids);

-- Índice para búsqueda exacta por clave específica (author_id = 'X')
-- Útil para   WHERE external_ids->>'openalex' = 'A12345'
CREATE INDEX IF NOT EXISTS ix_authors_openalex_id
    ON authors ((external_ids->>'openalex'))
    WHERE external_ids ? 'openalex';

CREATE INDEX IF NOT EXISTS ix_authors_scopus_id
    ON authors ((external_ids->>'scopus'))
    WHERE external_ids ? 'scopus';

-- ─────────────────────────────────────────────────────────────
-- 4. Verificación antes de eliminar columnas
-- ─────────────────────────────────────────────────────────────
DO $$
DECLARE
    migrated   bigint;
    with_oa    bigint;
    with_sc    bigint;
    total_src  bigint;
BEGIN
    SELECT COUNT(*) INTO migrated
    FROM authors
    WHERE external_ids != '{}'::jsonb;

    SELECT COUNT(*) INTO with_oa FROM authors WHERE openalex_id IS NOT NULL AND openalex_id != '';
    SELECT COUNT(*) INTO with_sc FROM authors WHERE scopus_id   IS NOT NULL AND scopus_id   != '';

    total_src := with_oa + with_sc;

    IF total_src > 0 AND migrated = 0 THEN
        RAISE EXCEPTION
            'Migración falló: % autores con IDs de fuente pero external_ids vacío',
            total_src;
    END IF;

    RAISE NOTICE 'Migración OK: % autores con external_ids migrado (% con openalex, % con scopus)',
        migrated, with_oa, with_sc;
END $$;

-- ─────────────────────────────────────────────────────────────
-- 5. Eliminar columnas individuales (ya migradas a JSONB)
-- ─────────────────────────────────────────────────────────────
-- CASCADE elimina automáticamente los índices dependientes de estas columnas.
ALTER TABLE authors
    DROP COLUMN IF EXISTS openalex_id CASCADE,
    DROP COLUMN IF EXISTS scopus_id   CASCADE,
    DROP COLUMN IF EXISTS wos_id      CASCADE,
    DROP COLUMN IF EXISTS cvlac_id    CASCADE;

COMMIT;

-- ─────────────────────────────────────────────────────────────
-- Verificación post-migración
-- ─────────────────────────────────────────────────────────────
SELECT
    COUNT(*)                                               AS total_autores,
    COUNT(*) FILTER (WHERE external_ids ? 'openalex')     AS con_openalex,
    COUNT(*) FILTER (WHERE external_ids ? 'scopus')       AS con_scopus,
    COUNT(*) FILTER (WHERE external_ids ? 'wos')          AS con_wos,
    COUNT(*) FILTER (WHERE external_ids ? 'cvlac')        AS con_cvlac,
    COUNT(*) FILTER (WHERE external_ids = '{}'::jsonb
                       OR  external_ids IS NULL)          AS sin_ids_externos
FROM authors;


SELECT *
FROM authors
WHERE field_provenance ->> 'name' = 'cvlac';