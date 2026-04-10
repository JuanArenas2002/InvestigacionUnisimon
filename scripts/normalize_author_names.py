"""
Script para normalizar todos los nombres de autores a mayúsculas.

Este script:
1. Lee todos los autores con nombre
2. Convierte los nombres a mayúsculas si no lo están
3. Registra cambios realizados
4. Muestra reporte de duplicados potenciales

Se ejecuta como:
    python scripts/normalize_author_names.py
"""

from db.session import get_session
from db.models import Author
from shared.normalizers import normalize_author_name
import logging
from collections import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_author_names():
    """Normaliza todos los nombres de autores a mayúsculas."""
    
    session = get_session()
    
    try:
        # Obtener todos los autores con nombre
        authors = session.query(Author).filter(
            Author.name.isnot(None),
            Author.name != ""
        ).all()
        
        changes = 0
        remapped_authors = []  # (old_name, new_name, count)
        normalized_names = []
        
        logger.info(f"Total de autores por normalizar: {len(authors)}")
        
        for author in authors:
            original = author.name
            normalized = normalize_author_name(original)
            
            if original != normalized:
                logger.info(f"[ID: {author.id}] {original!r} → {normalized!r}")
                author.name = normalized
                changes += 1
                remapped_authors.append((original, normalized))
            
            normalized_names.append(normalized)
        
        session.commit()
        logger.info(f"\n✅ Total de cambios realizados: {changes}")
        
        # Verificar nombres únicos después de normalización
        unique_names = session.query(Author.name).filter(
            Author.name.isnot(None)
        ).distinct(Author.name).all()
        
        logger.info(f"\n📊 Nombres únicos después de normalización: {len(unique_names)}")
        
        # Contar duplicados potenciales (mismos nombres con diferente ID)
        name_counts = Counter(normalized_names)
        duplicates = {name: count for name, count in name_counts.items() if count > 1}
        
        if duplicates:
            logger.info(f"\n⚠️  Potenciales duplicados encontrados ({len(duplicates)}):")
            for name, count in sorted(duplicates.items(), key=lambda x: x[1], reverse=True)[:20]:
                logger.info(f"    {name}: {count} registros")
        
        # Mostrar autores que tuvieron cambios
        if remapped_authors:
            logger.info(f"\n📝 Cambios realizados (primeros 20):")
            for orig, norm in remapped_authors[:20]:
                logger.info(f"    {orig!r} → {norm!r}")
        
    except Exception as e:
        logger.error(f"❌ Error durante normalización: {e}")
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    normalize_author_names()
