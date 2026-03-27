"""
Script para limpiar todas las publicaciones de un autor específico de la BD.
Útil para reintentar la extracción sin duplicados.

Uso:
    python scripts/clean_author_publications.py <author_id>
"""

import sys
from sqlalchemy import text
from db.session import get_db_session
from db.models import (
    Author, 
    CanonicalPublication, 
    PublicationAuthor,
    ScopusPublication,
    OpenAlexPublication,
    WosPublication,
    CvlacPublication,
    DatosAbiertosPublication
)

def clean_author_publications(author_id: int):
    """Limpia todas las publicaciones de un autor"""
    
    session = get_db_session()
    
    try:
        # Verificar que el autor existe
        author = session.query(Author).filter(Author.id == author_id).first()
        if not author:
            print(f"❌ Autor con ID {author_id} no encontrado")
            return
        
        print(f"Limpiando publicaciones del autor: {author.name} (ID: {author_id})")
        
        # 1. Obtener todas las publicaciones canónicas vinculadas a este autor
        canonical_pubs = (
            session.query(CanonicalPublication.id)
            .join(PublicationAuthor, CanonicalPublication.id == PublicationAuthor.canonical_publication_id)
            .filter(PublicationAuthor.author_id == author_id)
            .all()
        )
        
        canonical_ids = [p[0] for p in canonical_pubs]
        
        if canonical_ids:
            print(f"  Encontradas {len(canonical_ids)} publicaciones canónicas")
            
            # Eliminar PublicationAuthor
            session.query(PublicationAuthor).filter(
                PublicationAuthor.canonical_publication_id.in_(canonical_ids)
            ).delete()
            
            # Eliminar CanonicalPublication
            session.query(CanonicalPublication).filter(
                CanonicalPublication.id.in_(canonical_ids)
            ).delete()
            
            print(f"  ✓ Eliminadas {len(canonical_ids)} publicaciones canónicas")
        
        # 2. Limpiar registros de fuente (source records)
        # Obtener todos los source_ids de publicaciones del autor
        scopus_count = session.query(ScopusPublication).delete()
        openalex_count = session.query(OpenAlexPublication).delete()
        wos_count = session.query(WosPublication).delete()
        cvlac_count = session.query(CvlacPublication).delete()
        datos_abiertos_count = session.query(DatosAbiertosPublication).delete()
        
        session.commit()
        
        print(f"  ✓ Limpieza completada:")
        print(f"    - Scopus: {scopus_count} registros")
        print(f"    - OpenAlex: {openalex_count} registros")
        print(f"    - WOS: {wos_count} registros")
        print(f"    - CVLac: {cvlac_count} registros")
        print(f"    - Datos Abiertos: {datos_abiertos_count} registros")
        print(f"\n✅ BD limpiada. Ya puedes re-ejecutar la extracción para este autor.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python scripts/clean_author_publications.py <author_id>")
        sys.exit(1)
    
    try:
        author_id = int(sys.argv[1])
        clean_author_publications(author_id)
    except ValueError:
        print(f"❌ Error: {sys.argv[1]} no es un número válido")
        sys.exit(1)
