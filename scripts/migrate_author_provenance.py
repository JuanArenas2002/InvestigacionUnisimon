"""
Migración v4: Agregar field_provenance a la tabla authors.
Ejecutar: python scripts/migrate_author_provenance.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from db.session import get_session

SQL = """
ALTER TABLE authors
ADD COLUMN IF NOT EXISTS field_provenance JSONB;

COMMENT ON COLUMN authors.field_provenance IS
  '{campo: fuente} ej: {"orcid": "openalex", "scopus_id": "scopus", "name": "cvlac"}';

CREATE INDEX IF NOT EXISTS ix_authors_field_provenance
ON authors USING GIN (field_provenance);
"""

def main():
    db = get_session()
    try:
        db.execute(text(SQL))
        db.commit()
        print("OK - columna field_provenance agregada a authors")
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
