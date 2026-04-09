"""
Script de ejemplo: Crear un investigador con credenciales de prueba.
Ejecutar: python scripts/create_test_researcher.py
"""

import sys
from pathlib import Path

# Agregar el proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.session import get_session
from db.models import Author, ResearcherCredential
from datetime import datetime, timezone

def create_test_researcher():
    """Crea un investigador de prueba con credenciales."""
    session = get_session()
    
    try:
        # 1. Crear autor si no existe
        cedula = "1234567890"
        author = session.query(Author).filter(Author.cedula == cedula).first()
        
        if not author:
            author = Author(
                name="Juan Pérez García",
                cedula=cedula,
                normalized_name="juan perez garcia",
                is_institutional=True,
                verification_status="verified",
            )
            session.add(author)
            session.commit()
            print(f"✓ Investigador creado: {author.name} (Cédula: {cedula})")
        else:
            print(f"✓ Investigador encontrado: {author.name} (Cédula: {cedula})")
        
        # 2. Crear credencial
        password = "Password123!"
        
        # Desactivar credencial anterior si existe
        old_cred = (
            session.query(ResearcherCredential)
            .filter(
                ResearcherCredential.author_id == author.id,
                ResearcherCredential.is_active == True,
            )
            .first()
        )
        if old_cred:
            old_cred.is_active = False
            print(f"✓ Credencial anterior desactivada")
        
        # Crear nueva
        credential = ResearcherCredential(
            author_id=author.id,
            password_hash=ResearcherCredential.hash_password(password),
            is_active=True,
            activated_at=datetime.now(timezone.utc),
        )
        session.add(credential)
        session.commit()
        
        print(f"✓ Credencial creada (ID: {credential.id})")
        print("\n" + "="*60)
        print("DATOS DE PRUEBA - GUARDAR EN LUGAR SEGURO")
        print("="*60)
        print(f"Cédula:      {cedula}")
        print(f"Contraseña:  {password}")
        print(f"Endpoint:    POST /api/auth/login")
        print("="*60 + "\n")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    create_test_researcher()
