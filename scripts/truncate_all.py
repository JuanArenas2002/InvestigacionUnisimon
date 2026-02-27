"""
Script para vaciar TODAS las tablas de la base de datos.

Uso:
    python scripts/truncate_all.py
    python scripts/truncate_all.py --yes   (sin confirmación)
"""

import sys
import os

# Agregar raíz del proyecto al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from db.session import get_engine


TABLES = [
    "reconciliation_log",
    "publication_authors",
    "author_institutions",
    "openalex_records",
    "scopus_records",
    "wos_records",
    "cvlac_records",
    "datos_abiertos_records",
    "canonical_publications",
    "authors",
    "journals",
    "institutions",
]


def truncate_all(confirm: bool = True):
    """Vacía todas las tablas y reinicia PKs."""
    if confirm:
        print("⚠️  Esto ELIMINARÁ todos los registros de la base de datos.")
        print(f"   Tablas afectadas: {', '.join(TABLES)}")
        resp = input("\n¿Continuar? (s/N): ").strip().lower()
        if resp not in ("s", "si", "sí", "y", "yes"):
            print("Cancelado.")
            return

    engine = get_engine()
    with engine.connect() as conn:
        for table in TABLES:
            conn.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE"))
        conn.commit()

        print("\n✅ Tablas vaciadas. Verificando conteos:\n")
        for table in TABLES:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"   {table}: {count}")

    print("\n✅ Todas las tablas vaciadas y PKs reiniciados.")


if __name__ == "__main__":
    skip_confirm = "--yes" in sys.argv or "-y" in sys.argv
    truncate_all(confirm=not skip_confirm)
