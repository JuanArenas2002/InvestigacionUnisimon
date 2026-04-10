-- Migración: Crear trigger para normalizar nombres de autores a mayúsculas
-- Fecha: 2026-04-09
-- Objetivo: Evitar duplicados de autores por diferencia de mayúsculas/minúsculas

-- Paso 1: Crear función trigger para normalizar nombres de autores
CREATE OR REPLACE FUNCTION normalize_author_name_trigger()
RETURNS TRIGGER AS $$
BEGIN
    -- Convertir el nombre a mayúsculas si no es NULL
    IF NEW.name IS NOT NULL THEN
        NEW.name := UPPER(TRIM(NEW.name));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Paso 2: Crear trigger que se ejecute ANTES de INSERT o UPDATE
DROP TRIGGER IF EXISTS trg_normalize_author_name ON authors;

CREATE TRIGGER trg_normalize_author_name
    BEFORE INSERT OR UPDATE ON authors
    FOR EACH ROW
    EXECUTE FUNCTION normalize_author_name_trigger();

-- Paso 3: Normalizar todos los nombres existentes a mayúsculas
UPDATE authors
SET name = UPPER(TRIM(name))
WHERE name IS NOT NULL
  AND name != UPPER(TRIM(name));

-- Paso 4: Verificar resultados
SELECT 'RESUMEN DE NORMALIZACIÓN' as info,
       COUNT(*) as total_autores,
       COUNT(DISTINCT UPPER(name)) as nombres_unicos_normalizados
FROM authors
WHERE name IS NOT NULL;

-- Paso 5: Mostrar algunos ejemplos normalizados (primeros 10)
SELECT 'EJEMPLOS DE AUTORES NORMALIZADOS' as info,
       name,
       COUNT(*) as cantidad_registros
FROM authors
GROUP BY name
ORDER BY cantidad_registros DESC
LIMIT 10;
