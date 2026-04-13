#!/usr/bin/env python
"""Debug: verificar tabla en BD"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from db.session import get_engine
from sqlalchemy import inspect, text

def debug():
    engine = get_engine()
    
    print("🔍 Debug: Verificar tabla en BD")
    print(f"   Engine: {engine.url}")
    
    # Listar todas las tablas
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    print(f"\n📋 Total de tablas en BD: {len(tables)}")
    print("\nTablas que contienen 'scholar':")
    for table in tables:
        if "scholar" in table.lower():
            print(f"   - {table}")
    
    # Buscar específicamente
    print("\n🔎 Buscando 'google_Scholar_records'...")
    if "google_Scholar_records" in tables:
        print("   ✅ Tabla exists!")
        columns = inspector.get_columns("google_Scholar_records")
        print(f"   Columnas: {len(columns)}")
        for col in columns[:5]:
            print(f"     - {col['name']}")
    else:
        print("   ❌ NO encontrada")
    
    # Ejecutar query directa
    print("\n🔗 Query directa:")
    try:
        conn = engine.raw_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name ILIKE '%scholar%'
        """)
        results = cursor.fetchall()
        print(f"   Resultado: {results}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"   Error: {e}")

if __name__ == "__main__":
    debug()
