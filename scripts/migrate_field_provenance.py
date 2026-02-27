"""Script para ejecutar migración de field_provenance."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.session import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    conn.execute(text(
        "ALTER TABLE canonical_publications "
        "ADD COLUMN IF NOT EXISTS field_provenance JSONB DEFAULT '{}'"
    ))
    conn.commit()
    print("OK: columna field_provenance agregada")

    result = conn.execute(text(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name='canonical_publications' AND column_name='field_provenance'"
    ))
    for row in result:
        print(f"  Columna: {row[0]}, Tipo: {row[1]}")
