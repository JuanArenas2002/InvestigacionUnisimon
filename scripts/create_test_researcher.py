"""
Script de ejemplo: Crear un investigador con credenciales de prueba.

Uso:
    python scripts/create_test_researcher.py <cedula> <password>
    TEST_CEDULA=1234567890 TEST_PASSWORD=MiClave123! python scripts/create_test_researcher.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from db.session import get_session
from db.models import Author, ResearcherCredential
from datetime import datetime, timezone


def create_test_researcher():
    """Crea un investigador de prueba con credenciales."""
    # Credenciales desde args CLI o variables de entorno — nunca hardcodeadas
    if len(sys.argv) == 3:
        cedula = sys.argv[1]
        password = sys.argv[2]
    else:
        cedula = os.environ.get("TEST_CEDULA")
        password = os.environ.get("TEST_PASSWORD")

    if not cedula or not password:
        print("Uso: python scripts/create_test_researcher.py <cedula> <password>")
        print("  o: TEST_CEDULA=... TEST_PASSWORD=... python scripts/create_test_researcher.py")
        sys.exit(1)

    session = get_session()

    try:
        # 1. Crear autor si no existe
        author = session.query(Author).filter(Author.cedula == cedula).first()
        
        if not author:
            author = Author(
                name="Investigador de Prueba",
                cedula=cedula,
                normalized_name="investigador de prueba",
                is_institutional=True,
                verification_status="verified",
            )
            session.add(author)
            session.commit()
            print(f"Investigador creado: {author.name}")
        else:
            print(f"Investigador encontrado: {author.name}")

        # 2. Crear credencial — desactivar anterior si existe
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
            print("Credencial anterior desactivada")

        credential = ResearcherCredential(
            author_id=author.id,
            password_hash=ResearcherCredential.hash_password(password),
            is_active=True,
            activated_at=datetime.now(timezone.utc),
        )
        session.add(credential)
        session.commit()

        print(f"Credencial creada (ID: {credential.id})")
        print(f"Endpoint: POST /api/auth/login")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    create_test_researcher()
