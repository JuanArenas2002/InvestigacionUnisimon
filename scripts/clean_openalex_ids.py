"""
Limpia los IDs de OpenAlex en external_ids de autores.

Convierte valores como:
  "https://openalex.org/A5058933826"  →  "A5058933826"

Solo afecta la clave "openalex" dentro del JSONB external_ids.
Las demás claves (scopus, cvlac, google_scholar, wos) no se tocan.

Uso:
    python scripts/clean_openalex_ids.py            # ejecuta los cambios
    python scripts/clean_openalex_ids.py --dry-run  # solo muestra qué cambiaría
"""

import argparse
import sys
from pathlib import Path

# Permite importar módulos del proyecto desde cualquier directorio
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from db.session import get_session
from db.models import Author

PREFIX = "https://openalex.org/"


def clean_openalex_id(raw: str) -> str:
    """Quita el prefijo de URL si existe, retorna solo el ID interno."""
    if raw and raw.startswith(PREFIX):
        return raw[len(PREFIX):]
    return raw


def run(dry_run: bool = False):
    session = get_session()
    try:
        # Traer solo autores con openalex en external_ids que contengan la URL
        authors = (
            session.query(Author)
            .filter(
                Author.external_ids["openalex"].astext.like(f"{PREFIX}%")
            )
            .all()
        )

        total = len(authors)
        if total == 0:
            print("No se encontraron IDs de OpenAlex con prefijo URL. Nada que limpiar.")
            return

        print(f"Autores a actualizar: {total}")
        print(f"Modo: {'DRY-RUN (sin cambios)' if dry_run else 'EJECUCIÓN REAL'}\n")

        for author in authors:
            raw = (author.external_ids or {}).get("openalex", "")
            cleaned = clean_openalex_id(raw)
            print(f"  ID {author.id:>6} | {author.name[:50]:<50} | {raw} → {cleaned}")

            if not dry_run:
                author.external_ids = {**author.external_ids, "openalex": cleaned}
                # Forzar que SQLAlchemy detecte el cambio en el JSONB
                from sqlalchemy.orm.attributes import flag_modified
                flag_modified(author, "external_ids")

        if not dry_run:
            session.commit()
            print(f"\n✓ {total} autores actualizados correctamente.")
        else:
            print(f"\n[DRY-RUN] {total} autores se actualizarían. Ejecuta sin --dry-run para aplicar.")

    except Exception as e:
        session.rollback()
        print(f"\nERROR: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Limpia prefijo URL en IDs de OpenAlex")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra qué cambiaría sin modificar la base de datos",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)
