"""
Seeder: carga autores institucionales desde el Excel de redes académicas.

Lee el archivo "Redes académicas - Investigadores.xlsx" y hace upsert
de cada investigador en la tabla `authors`, poblando:
  - cedula        (columna CC)
  - name          (columna Nombre)
  - external_ids  → cvlac, google_scholar, scopus
  - orcid         (columna ORCID)

Criterio de match (en orden de prioridad):
  1. Por cédula  (si el autor ya existe con esa cédula → actualiza)
  2. Por ORCID   (si el autor ya tiene ese ORCID → actualiza)
  3. Por nombre normalizado  (si hay match exacto → actualiza)
  4. Ninguno → crea nuevo autor

Uso:
    python -m db.seeders.seed_authors_from_excel
    python -m db.seeders.seed_authors_from_excel --excel "ruta/al/archivo.xlsx"
    python -m db.seeders.seed_authors_from_excel --dry-run
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs

from db.models import Author
from db.session import get_session
from shared.normalizers import normalize_author_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Ruta por defecto del Excel
DEFAULT_EXCEL = Path(__file__).resolve().parents[2] / "Redes académicas - Investigadores.xlsx"


# =============================================================
# PARSERS DE URL
# =============================================================

def _extract_cvlac_id(url: str) -> Optional[str]:
    """Extrae cod_rh de una URL de CVLAC."""
    if not url:
        return None
    try:
        qs = parse_qs(urlparse(url).query)
        cod = qs.get("cod_rh", [None])[0]
        return cod.strip() if cod else None
    except Exception:
        return None


def _extract_scholar_id(url: str) -> Optional[str]:
    """Extrae el parámetro `user` de una URL de Google Scholar."""
    if not url:
        return None
    try:
        qs = parse_qs(urlparse(url).query)
        uid = qs.get("user", [None])[0]
        return uid.strip() if uid else None
    except Exception:
        return None


def _extract_scopus_id(url: str) -> Optional[str]:
    """Extrae authorId de una URL de Scopus."""
    if not url:
        return None
    try:
        qs = parse_qs(urlparse(url).query)
        aid = qs.get("authorId", [None])[0]
        return aid.strip() if aid else None
    except Exception:
        return None


def _extract_orcid(url: str) -> Optional[str]:
    """Extrae el ORCID de una URL https://orcid.org/xxxx-xxxx-xxxx-xxxx."""
    if not url:
        return None
    url = str(url).strip()
    match = re.search(r'(\d{4}-\d{4}-\d{4}-\d{3}[\dX])', url)
    return match.group(1) if match else None


def _normalize_name(name: str) -> str:
    """Normaliza nombre para comparación: minúsculas sin tildes."""
    if not name:
        return ""
    from unidecode import unidecode
    return re.sub(r'\s+', ' ', unidecode(name).lower().strip())


# =============================================================
# LECTURA DEL EXCEL
# =============================================================

def read_excel(path: Path) -> list[dict]:
    """
    Lee el Excel y devuelve una lista de dicts con los campos normalizados.

    Columnas esperadas (en orden):
      0: CC (cédula)
      1: Nombre
      2: CvLAC (URL)
      3: Google Scholar (URL)
      4: ResearchGate (URL) — ignorado
      5: Academia.edu (URL) — ignorado
      6: Research ID / WOS (URL o ID) — ignorado (ya cubierto por WosAdapter)
      7: Autor ID Scopus (URL)
      8: ORCID (URL)
    """
    try:
        import openpyxl
    except ImportError:
        logger.error("openpyxl no está instalado: pip install openpyxl")
        sys.exit(1)

    wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        return []

    authors = []
    for row in rows[1:]:  # saltar encabezado
        if not row or not row[1]:
            continue

        cedula   = str(row[0]).strip() if row[0] else None
        name     = str(row[1]).strip() if row[1] else None
        cvlac_id = _extract_cvlac_id(str(row[2]) if row[2] else None)
        scholar_id = _extract_scholar_id(str(row[3]) if row[3] else None)
        scopus_id = _extract_scopus_id(str(row[7]) if row[7] else None)
        orcid    = _extract_orcid(str(row[8]) if row[8] else None)

        if not name:
            continue

        authors.append({
            "cedula":     cedula,
            "name":       name,
            "orcid":      orcid,
            "cvlac_id":   cvlac_id,
            "scholar_id": scholar_id,
            "scopus_id":  scopus_id,
        })

    logger.info(f"Excel leído: {len(authors)} investigadores encontrados.")
    return authors


# =============================================================
# UPSERT EN LA DB
# =============================================================

def _find_existing(session, row: dict):
    """
    Busca un autor existente en la BD por cédula → ORCID → nombre normalizado.
    Retorna el objeto Author o None.
    """
    from db.models import Author

    # 1. Por cédula
    if row["cedula"]:
        author = session.query(Author).filter(Author.cedula == row["cedula"]).first()
        if author:
            return author

    # 2. Por ORCID
    if row["orcid"]:
        author = session.query(Author).filter(Author.orcid == row["orcid"]).first()
        if author:
            return author

    # 3. Por nombre normalizado
    normalized = _normalize_name(row["name"])
    author = session.query(Author).filter(
        Author.normalized_name == normalized
    ).first()
    return author


def _build_external_ids(existing: dict, row: dict) -> dict:
    """Fusiona los external_ids existentes con los nuevos del Excel."""
    ext = dict(existing or {})
    if row["cvlac_id"]:
        ext["cvlac"] = row["cvlac_id"]
    if row["scholar_id"]:
        ext["google_scholar"] = row["scholar_id"]
    if row["scopus_id"]:
        ext["scopus"] = row["scopus_id"]
    return ext


def seed(excel_path: Path, dry_run: bool = False) -> dict:
    """
    Ejecuta el upsert de autores desde el Excel.

    Returns:
        Dict con conteos: created, updated, skipped.
    """
    rows = read_excel(excel_path)
    if not rows:
        logger.warning("No se encontraron filas en el Excel.")
        return {"created": 0, "updated": 0, "skipped": 0}

    from db.session import get_session
    from db.models import Author

    session = get_session()
    created = updated = skipped = 0

    try:
        for row in rows:
            existing = _find_existing(session, row)

            if existing:
                # Actualizar campos
                changed = False

                if not existing.cedula and row["cedula"]:
                    existing.cedula = row["cedula"]
                    changed = True

                if not existing.orcid and row["orcid"]:
                    existing.orcid = row["orcid"]
                    changed = True

                new_ext = _build_external_ids(existing.external_ids, row)
                if new_ext != (existing.external_ids or {}):
                    existing.external_ids = new_ext
                    changed = True

                if changed:
                    if not dry_run:
                        session.add(existing)
                    updated += 1
                    logger.debug(f"Actualizado: {existing.name}")
                else:
                    skipped += 1
            else:
                # Crear nuevo autor
                normalized = _normalize_name(row["name"])
                ext_ids = _build_external_ids({}, row)
                author = Author(
                    name=normalize_author_name(row["name"]),
                    normalized_name=normalized,
                    cedula=row["cedula"],
                    orcid=row["orcid"],
                    external_ids=ext_ids if ext_ids else {},
                    is_institutional=True,
                    verification_status="verified",
                )
                if not dry_run:
                    session.add(author)
                created += 1
                logger.debug(f"Creado: {row['name']}")

        if not dry_run:
            session.commit()
            logger.info(
                f"Seeder completado — Creados: {created}, "
                f"Actualizados: {updated}, Sin cambios: {skipped}"
            )
        else:
            logger.info(
                f"[DRY-RUN] Creados: {created}, "
                f"Actualizados: {updated}, Sin cambios: {skipped}"
            )

    except Exception as e:
        session.rollback()
        logger.error(f"Error durante el seeder: {e}")
        raise
    finally:
        session.close()

    return {"created": created, "updated": updated, "skipped": skipped}


# =============================================================
# PUNTO DE ENTRADA
# =============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Carga autores institucionales desde el Excel de redes académicas."
    )
    parser.add_argument(
        "--excel",
        type=Path,
        default=DEFAULT_EXCEL,
        help=f"Ruta al archivo Excel (default: {DEFAULT_EXCEL})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simula el proceso sin escribir en la base de datos.",
    )
    args = parser.parse_args()

    if not args.excel.exists():
        logger.error(f"Archivo no encontrado: {args.excel}")
        sys.exit(1)

    result = seed(args.excel, dry_run=args.dry_run)
    print(
        f"\nResultado: {result['created']} creados, "
        f"{result['updated']} actualizados, "
        f"{result['skipped']} sin cambios."
    )
