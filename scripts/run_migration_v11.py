"""
Script para ejecutar la migración v11 de credenciales de investigadores.
Ejecutar: python scripts/run_migration_v11.py
"""

import sys
from pathlib import Path

# Agregar proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.session import get_engine
from sqlalchemy import text

def run_migration():
    """Ejecuta el SQL de la migración v11."""
    engine = get_engine()
    
    # Leer archivo de migración
    migration_file = Path(__file__).parent.parent / "db" / "migration_v11_researcher_credentials.sql"
    
    if not migration_file.exists():
        print(f"❌ Archivo no encontrado: {migration_file}")
        return False
    
    with open(migration_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Ejecutar migracion
    try:
        with engine.begin() as conn:
            # Leer el SQL completo y ejecutarlo de una vez
            conn.execute(text(sql_content))
        
        print("\n✅ Migración v11 ejecutada correctamente!")
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante la migración: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
