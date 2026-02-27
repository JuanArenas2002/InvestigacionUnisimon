-- =============================================================
-- Vaciar TODA la base de datos y restaurar contadores de PK a 1
-- Base de datos: reconciliacion_bibliografica
--
-- ORDEN: respeta las FK (hijos primero, padres después).
-- Ejecutar en pgAdmin o psql.
-- =============================================================

BEGIN;

-- 1. Tablas hijas (dependen de otras)
TRUNCATE TABLE reconciliation_log        RESTART IDENTITY CASCADE;
TRUNCATE TABLE publication_authors       RESTART IDENTITY CASCADE;
TRUNCATE TABLE author_institutions       RESTART IDENTITY CASCADE;
TRUNCATE TABLE openalex_records          RESTART IDENTITY CASCADE;
TRUNCATE TABLE scopus_records            RESTART IDENTITY CASCADE;
TRUNCATE TABLE wos_records               RESTART IDENTITY CASCADE;
TRUNCATE TABLE cvlac_records             RESTART IDENTITY CASCADE;
TRUNCATE TABLE datos_abiertos_records    RESTART IDENTITY CASCADE;

-- 2. Tablas padre
TRUNCATE TABLE canonical_publications    RESTART IDENTITY CASCADE;
TRUNCATE TABLE authors                   RESTART IDENTITY CASCADE;
TRUNCATE TABLE journals                  RESTART IDENTITY CASCADE;
TRUNCATE TABLE institutions              RESTART IDENTITY CASCADE;

COMMIT;

-- Verificación rápida (debe mostrar 0 en todas)
SELECT 'canonical_publications' AS tabla, COUNT(*) FROM canonical_publications
UNION ALL SELECT 'openalex_records',      COUNT(*) FROM openalex_records
UNION ALL SELECT 'scopus_records',        COUNT(*) FROM scopus_records
UNION ALL SELECT 'wos_records',           COUNT(*) FROM wos_records
UNION ALL SELECT 'cvlac_records',         COUNT(*) FROM cvlac_records
UNION ALL SELECT 'datos_abiertos_records',COUNT(*) FROM datos_abiertos_records
UNION ALL SELECT 'authors',               COUNT(*) FROM authors
UNION ALL SELECT 'publication_authors',   COUNT(*) FROM publication_authors
UNION ALL SELECT 'reconciliation_log',    COUNT(*) FROM reconciliation_log
UNION ALL SELECT 'journals',             COUNT(*) FROM journals
UNION ALL SELECT 'institutions',         COUNT(*) FROM institutions
UNION ALL SELECT 'author_institutions',  COUNT(*) FROM author_institutions;
