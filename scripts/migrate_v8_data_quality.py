"""
Migración v8: Calidad de datos en publicaciones canónicas.

Agrega dos columnas a canonical_publications:
  - field_conflicts   : Conflictos detectados entre fuentes para un campo
  - citations_by_source: Citas reportadas por cada fuente por separado

Ejecutar UNA sola vez:
    python scripts/migrate_v8_data_quality.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.session import get_engine
from sqlalchemy import text

engine = get_engine()

MIGRATIONS = [
    (
        "field_conflicts",
        "ALTER TABLE canonical_publications "
        "ADD COLUMN IF NOT EXISTS field_conflicts JSONB DEFAULT '{}'::jsonb",
        "Conflictos entre fuentes por campo",
    ),
    (
        "citations_by_source",
        "ALTER TABLE canonical_publications "
        "ADD COLUMN IF NOT EXISTS citations_by_source JSONB DEFAULT '{}'::jsonb",
        "Citas reportadas por cada fuente",
    ),
    (
        "ix_canon_has_conflicts",
        "CREATE INDEX IF NOT EXISTS ix_canon_has_conflicts "
        "ON canonical_publications ((field_conflicts IS NOT NULL AND field_conflicts != '{}'::jsonb))",
        "Índice para publicaciones con conflictos",
    ),
]

with engine.connect() as conn:
    for name, sql, description in MIGRATIONS:
        try:
            conn.execute(text(sql))
            conn.commit()
            print(f"OK: {description} ({name})")
        except Exception as e:
            conn.rollback()
            print(f"ERROR en {name}: {e}")
            sys.exit(1)

    # Verificar columnas
    result = conn.execute(text(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name='canonical_publications' "
        "AND column_name IN ('field_conflicts', 'citations_by_source') "
        "ORDER BY column_name"
    ))
    print("\nColumnas verificadas en canonical_publications:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")

print("\nMigración v8 completada.")
