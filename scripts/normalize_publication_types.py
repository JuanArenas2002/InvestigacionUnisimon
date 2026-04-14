"""
Script para normalizar TODOS los tipos de publicación y nombres de autores a mayúsculas.

Este script:
1. Normaliza publication_type en canonical_publications a mayúsculas
2. Normaliza publication_type en TODAS las tablas de fuente (scopus, openalex, wos, cvlac, etc.)
3. Normaliza author names a mayúsculas
4. Registra todos los cambios realizados

Se ejecuta como:
    python scripts/normalize_publication_types.py
"""

from db.session import get_session
from db.models import CanonicalPublication, Author, SOURCE_MODELS
from shared.normalizers import normalize_publication_type, normalize_author_name
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def normalize_canonical_publication_types():
    """Normaliza publication_type en canonical_publications."""
    session = get_session()
    try:
        publications = session.query(CanonicalPublication).filter(
            CanonicalPublication.publication_type.isnot(None)
        ).all()

        changes = 0
        logger.info(f"\n📄 CanonicalPublication: {len(publications)} tienen publication_type")

        for pub in publications:
            original = pub.publication_type
            normalized = normalize_publication_type(original)

            if original != normalized:
                logger.info(f"  [ID: {pub.id}] {original!r} → {normalized!r}")
                pub.publication_type = normalized
                changes += 1

        session.commit()
        logger.info(f"  ✓ {changes} cambios realizados\n")
        return changes

    except Exception as e:
        logger.error(f"Error normalizando canonical_publications: {e}")
        session.rollback()
        raise
    finally:
        session.close()


def normalize_source_record_types():
    """Normaliza publication_type en TODAS las tablas de fuente."""
    session = get_session()
    total_changes = 0

    try:
        logger.info("📚 Normalizando fuentes de datos (scopus, openalex, wos, etc.):")

        for source_name, model_cls in SOURCE_MODELS.items():
            if not hasattr(model_cls, 'publication_type'):
                continue

            records = session.query(model_cls).filter(
                model_cls.publication_type.isnot(None)
            ).all()

            if not records:
                continue

            changes = 0
            logger.info(f"\n  📖 {source_name}: {len(records)} registros")

            for record in records:
                original = record.publication_type
                normalized = normalize_publication_type(original)

                if original != normalized:
                    changes += 1
                    if changes <= 3:  # Solo log de los primeros 3
                        logger.info(f"    {original!r} → {normalized!r}")
                    record.publication_type = normalized

            if changes > 3:
                logger.info(f"    ... y {changes - 3} más")

            logger.info(f"    ✓ {changes} registros normalizados")
            total_changes += changes
            session.commit()

        return total_changes

    except Exception as e:
        logger.error(f"Error normalizando source records: {e}")
        session.rollback()
        raise
    finally:
        session.close()


def normalize_author_names():
    """Normaliza author names a mayúsculas."""
    session = get_session()

    try:
        authors = session.query(Author).filter(
            Author.name.isnot(None)
        ).all()

        changes = 0
        logger.info(f"\n👤 Author names: {len(authors)} autores")

        duplicates = {}  # Para detectar colisiones

        for author in authors:
            original = author.name
            normalized = normalize_author_name(original)

            if original != normalized:
                changes += 1
                if changes <= 3:
                    logger.info(f"  {original!r} → {normalized!r}")
                author.name = normalized

            # Track duplicates
            if normalized not in duplicates:
                duplicates[normalized] = []
            duplicates[normalized].append(author.id)

        if changes > 3:
            logger.info(f"  ... y {changes - 3} más")

        session.commit()
        logger.info(f"  ✓ {changes} autores normalizados\n")

        # Reportar duplicados potenciales
        actual_duplicates = {k: v for k, v in duplicates.items() if len(v) > 1}
        if actual_duplicates:
            logger.warning(f"  ⚠️  {len(actual_duplicates)} nombres con múltiples IDs (posibles duplicados):")
            for name, ids in sorted(actual_duplicates.items())[:5]:
                logger.warning(f"     {name!r}: IDs {ids}")

        return changes

    except Exception as e:
        logger.error(f"Error normalizando author names: {e}")
        session.rollback()
        raise
    finally:
        session.close()


def show_unique_publication_types():
    """Muestra todos los publication_type únicos después de normalización."""
    session = get_session()

    try:
        logger.info("\n📊 Tipos de publicación únicos en la BD:")

        # CanonicalPublications
        canon_types = session.query(CanonicalPublication.publication_type).filter(
            CanonicalPublication.publication_type.isnot(None)
        ).distinct().all()

        logger.info(f"\n  CanonicalPublication ({len(canon_types)} únicos):")
        for (pub_type,) in sorted(canon_types):
            count = session.query(CanonicalPublication).filter(
                CanonicalPublication.publication_type == pub_type
            ).count()
            logger.info(f"    {pub_type}: {count}")

        # Source tables
        for source_name, model_cls in SOURCE_MODELS.items():
            if not hasattr(model_cls, 'publication_type'):
                continue

            types = session.query(model_cls.publication_type).filter(
                model_cls.publication_type.isnot(None)
            ).distinct().all()

            if not types:
                continue

            logger.info(f"\n  {source_name.title()} ({len(types)} únicos):")
            for (pub_type,) in sorted(types):
                count = session.query(model_cls).filter(
                    model_cls.publication_type == pub_type
                ).count()
                logger.info(f"    {pub_type}: {count}")

    except Exception as e:
        logger.error(f"Error mostrando tipos: {e}")
        raise
    finally:
        session.close()


def normalize_all():
    """Ejecuta normalización completa de todas las tablas."""
    logger.info("=" * 70)
    logger.info("NORMALIZACIÓN COMPLETA: Publication Types + Author Names")
    logger.info("=" * 70)

    try:
        # Fase 1: Normalizar canonical publications
        canon_changes = normalize_canonical_publication_types()

        # Fase 2: Normalizar source records
        source_changes = normalize_source_record_types()

        # Fase 3: Normalizar author names
        author_changes = normalize_author_names()

        # Fase 4: Show summary
        show_unique_publication_types()

        total = canon_changes + source_changes + author_changes
        logger.info("=" * 70)
        logger.info(f"✓ NORMALIZACIÓN COMPLETADA: {total} cambios realizados")
        logger.info(f"  - CanonicalPublication: {canon_changes}")
        logger.info(f"  - Source records: {source_changes}")
        logger.info(f"  - Author names: {author_changes}")
        logger.info("=" * 70 + "\n")

    except Exception as e:
        logger.error(f"❌ Error durante normalización: {e}")
        raise


if __name__ == "__main__":
    normalize_all()

