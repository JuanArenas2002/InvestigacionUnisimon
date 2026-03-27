
-- Crear tabla de estados de publicación
CREATE TABLE IF NOT EXISTS publicacion_estados (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(20) UNIQUE NOT NULL,
    descripcion TEXT
);

-- Insertar estados por defecto
INSERT INTO publicacion_estados (nombre, descripcion) VALUES
    ('Avalado', 'Publicación validada y aceptada'),
    ('Revisión', 'Publicación en proceso de revisión'),
    ('Rechazado', 'Publicación rechazada')
ON CONFLICT (nombre) DO NOTHING;

-- Agregar columna y relación foránea
ALTER TABLE canonical_publications
ADD COLUMN IF NOT EXISTS estado_publicacion_id INTEGER DEFAULT 1 REFERENCES publicacion_estados(id);

COMMENT ON COLUMN canonical_publications.estado_publicacion_id IS
    'Estado de la publicación: referencia a publicacion_estados (Avalado, Revisión, Rechazado)';

COMMENT ON COLUMN canonical_publications.estado_publicacion IS
    'Estado de la publicación: Avalado, Revisión, Rechazado';
