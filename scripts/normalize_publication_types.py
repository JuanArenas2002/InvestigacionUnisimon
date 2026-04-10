"""
Script para normalizar todos los tipos de publicación a mayúsculas.

Este script:
1. Lee todos los canonical_publications con publication_type
2. Convierte los valores a mayúsculas si no lo están
3. Registra cambios realizados

Se ejecuta como:
    python scripts/normalize_publication_types.py
"""

from db.session import get_session
from db.models import CanonicalPublication
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_publication_types():
    """Normaliza todos los publication_type a mayúsculas."""
    
    session = get_session()
    
    try:
        # Obtener todas las publicaciones con publication_type
        publications = session.query(CanonicalPublication).filter(
            CanonicalPublication.publication_type.isnot(None)
        ).all()
        
        changes = 0
        duplicates_found = {}
        
        logger.info(f"Total de publicaciones con publication_type: {len(publications)}")
        
        for pub in publications:
            original = pub.publication_type
            normalized = original.strip().upper() if original else None
            
            if original != normalized:
                logger.info(f"[ID: {pub.id}] {original!r} → {normalized!r}")
                pub.publication_type = normalized
                changes += 1
        
        session.commit()
        logger.info(f"Total de cambios realizados: {changes}")
        
        # Verificar tipos únicos después de normalización
        unique_types = session.query(
            CanonicalPublication.publication_type
        ).filter(
            CanonicalPublication.publication_type.isnot(None)
        ).distinct().all()
        
        logger.info("\nTipos de publicación únicos después de normalización:")
        for (pub_type,) in sorted(unique_types):
            count = session.query(CanonicalPublication).filter(
                CanonicalPublication.publication_type == pub_type
            ).count()
            logger.info(f"  {pub_type}: {count}")
        
    except Exception as e:
        logger.error(f"Error durante normalización: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    normalize_publication_types()
