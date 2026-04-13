#!/usr/bin/env python
"""Aplicar migración v15 - Google Scholar records table"""

import sys
from pathlib import Path
from sqlalchemy import inspect

# Añadir directorio al path
sys.path.insert(0, str(Path(__file__).parent))

from db.session import get_session, get_engine

def apply_migration():
    """Aplicar migración v15"""
    print("🔄 Aplicando migración v15 - Google Scholar records table...")
    
    # Leer archivo de migración
    migration_path = Path(__file__).parent / "db" / "migration_v15_google_scholar.sql"
    
    if not migration_path.exists():
        print(f"❌ Archivo no encontrado: {migration_path}")
        return False
    
    print(f"📂 Leyendo: {migration_path}")
    
    with open(migration_path, "r", encoding="utf-8") as f:
        sql_content = f.read()
    
    engine = get_engine()
    
    try:
        print(f"\n📋 Ejecutando migración completa...")
        
        # Usar psycopg2 directamente
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        # Dividir más inteligentemente: buscar ; que no estén dentro de $$...$$
        sections = []
        current = ""
        in_dollar_quote = False
        
        for char in sql_content:
            if char == "$":
                in_dollar_quote = not in_dollar_quote
            current += char
            
            if char == ";" and not in_dollar_quote:
                sections.append(current)
                current = ""
        
        if current.strip():
            sections.append(current)
        
        print(f"   Encontrados {len(sections)} statements")
        
        executed = 0
        for i, statement in enumerate(sections, 1):
            stmt = statement.strip()
            if not stmt or stmt.startswith("--"):
                continue
            
            stmt_preview = stmt[:60].replace("\n", " ")
            print(f"\n   [{i}] {stmt_preview}...")
            
            try:
                cursor.execute(stmt)
                conn.commit()
                executed += 1
                print(f"       ✅ OK")
            except Exception as e:
                conn.rollback()
                error_msg = str(e).split("\n")[0]
                print(f"       ⚠️  {error_msg[:80]}")
        
        cursor.close()
        conn.close()
        
        print("\n" + "="*60)
        print(f"✅ Migración completada ({executed} statements ejecutados)")
        print("="*60)
        
        # Verificar tabla creada
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        if "google_scholar_records" in tables:
            print("\n✅ Tabla 'google_scholar_records' creada exitosamente")
            
            # Mostrar columnas
            columns = inspector.get_columns("google_scholar_records")
            print(f"\nColumnas ({len(columns)}):")
            for col in columns:
                print(f"  - {col['name']:30} {str(col['type'])}")
            
            # Mostrar índices
            indexes = inspector.get_indexes("google_scholar_records")
            print(f"\nÍndices ({len(indexes)}):")
            for idx in indexes:
                print(f"  - {idx['name']}")
            
            return True
        else:
            print("\n❌ Tabla NO fue creada")
            return False
        
    except Exception as e:
        print(f"\n❌ Error general: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = apply_migration()
    sys.exit(0 if success else 1)
