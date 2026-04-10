-- Active: 1774386404707@@127.0.0.1@5432@reconciliacion_bibliografica
-- ============================================================
-- SEED: Vincular IDs externos de investigadores conocidos
-- ============================================================
-- Uso: ejecutar este script cuando se conoce el mapeo completo
-- de IDs de un investigador (cvlac, scopus, openalex, google_scholar).
--
-- Estrategia:
--   1. Localizar el autor por el ID más confiable disponible
--      (preferencia: cvlac > openalex > scopus)
--   2. Fusionar todos los IDs al external_ids JSONB existente
--      con el operador || (no sobreescribe claves ya presentes
--      a menos que uses jsonb_set explícito)
--
-- IMPORTANTE: el operador || sobreescribe si la clave ya existe.
-- Si no quieres pisar un valor previo, usa la versión
-- "solo si no existe" que aparece comentada más abajo.
-- ============================================================

BEGIN;

-- ─────────────────────────────────────────────────────────────
-- Ejemplo 1: actualizar/insertar TODOS los IDs de un investigador
-- Localiza por cvlac y fusiona los demás
-- ─────────────────────────────────────────────────────────────
UPDATE authors
SET
    external_ids = external_ids || jsonb_build_object(
        'cvlac',          '0000614289',
        'scopus',         '57193767797',
        'openalex',       'A5067960810',
        'google_scholar', 'V94aovUAAAAJ'
    ),
    updated_at = NOW()
WHERE external_ids->>'cvlac' = '0000614289';

-- Si el autor aún no existe con ese cvlac, buscar por otro ID:
-- WHERE external_ids->>'openalex' = 'A5067960810'
-- WHERE external_ids->>'scopus'   = '57193767797'

-- ─────────────────────────────────────────────────────────────
-- Ejemplo 2: agregar google_scholar SOLO si no tiene uno aún
-- (versión conservadora — no pisa datos existentes)
-- ─────────────────────────────────────────────────────────────
-- UPDATE authors
-- SET
--     external_ids = external_ids || '{"google_scholar": "V94aovUAAAAJ"}'::jsonb,
--     updated_at = NOW()
-- WHERE external_ids->>'cvlac' = '0000614289'
--   AND NOT (external_ids ? 'google_scholar');

-- ─────────────────────────────────────────────────────────────
-- Verificación: ver el resultado
-- ─────────────────────────────────────────────────────────────
SELECT
    id,
    name,
    external_ids,
    updated_at
FROM authors
WHERE external_ids->>'cvlac' = '0000614289'
   OR external_ids->>'google_scholar' = 'V94aovUAAAAJ';

COMMIT;
