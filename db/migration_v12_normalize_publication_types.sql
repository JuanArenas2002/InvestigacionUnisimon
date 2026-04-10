-- Migración: Normalizar todos los tipos de publicación a mayúsculas
-- Fecha: 2026-04-09
-- Objetivo: Evitar duplicados como 'article' vs 'ARTICLE'

-- Paso 1: Verificar tipos únicos antes de cambios
SELECT 'ANTES DE MIGRACIÓN' as fase, publication_type, COUNT(*) as count
FROM canonical_publications
WHERE publication_type IS NOT NULL
GROUP BY publication_type
ORDER BY publication_type;

-- Paso 2: Aplicar conversión a mayúsculas
UPDATE canonical_publications
SET publication_type = UPPER(TRIM(publication_type))
WHERE publication_type IS NOT NULL
  AND publication_type != UPPER(TRIM(publication_type));

-- Paso 3: Verificar tipos únicos después de cambios
SELECT 'DESPUÉS DE MIGRACIÓN' as fase, publication_type, COUNT(*) as count
FROM canonical_publications
WHERE publication_type IS NOT NULL
GROUP BY publication_type
ORDER BY publication_type;

-- Paso 4: Mostrar resumen de cambios
SELECT 'RESUMEN FINAL' as info,
       COUNT(DISTINCT publication_type) as tipos_unicos,
       COUNT(*) as total_publicaciones
FROM canonical_publications
WHERE publication_type IS NOT NULL;
