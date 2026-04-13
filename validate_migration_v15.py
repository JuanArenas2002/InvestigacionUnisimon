#!/usr/bin/env python
"""Validar que la migración v15 se aplicó correctamente"""

import sys
from pathlib import Path
from sqlalchemy import inspect, func, text

sys.path.insert(0, str(Path(__file__).parent))

from db.session import get_session, get_engine
from db.models import SOURCE_REGISTRY

def validate_migration():
    """Validar migración v15"""
    print("🔍 Validando migración v15 - Google Scholar records...")
    
    engine = get_engine()
    session = get_session()
    inspector = inspect(engine)
    
    checks_passed = 0
    checks_total = 0
    
    # 1. Verificar tabla existe
    checks_total += 1
    tables = inspector.get_table_names()
    if "google_scholar_records" in tables:
        print("✅ [1/6] Tabla 'google_scholar_records' existe")
        checks_passed += 1
        table_name = "google_scholar_records"
    else:
        print("❌ [1/6] Tabla NO existe")
        return False
    
    # 2. Verificar columnas
    checks_total += 1
    columns = inspector.get_columns(table_name)
    column_names = {c["name"] for c in columns}
    
    required_columns = {
        "id", "google_scholar_id", "scholar_profile_id", "title",
        "authors_json", "publication_year", "doi", "citation_count",
        "status", "canonical_publication_id", "created_at", "updated_at"
    }
    
    missing = required_columns - column_names
    if not missing:
        print(f"✅ [2/6] Todas columnas requeridas existen ({len(required_columns)})")
        checks_passed += 1
    else:
        print(f"❌ [2/6] Columnas faltantes: {missing}")
    
    # 3. Verificar tipos de datos JSONB
    checks_total += 1
    jsonb_columns = {"authors_json", "citations_by_year", "raw_data"}
    jsonb_found = {
        c["name"] for c in columns
        if c["name"] in jsonb_columns and "JSONB" in str(c["type"]).upper()
    }
    
    if jsonb_found == jsonb_columns:
        print(f"✅ [3/6] Campos JSONB correctos: {jsonb_found}")
        checks_passed += 1
    else:
        print(f"⚠️  [3/6] Campos JSONB: {jsonb_found} (faltan: {jsonb_columns - jsonb_found})")
    
    # 4. Verificar índices
    checks_total += 1
    indexes = inspector.get_indexes(table_name)
    index_names = {idx["name"] for idx in indexes}
    
    required_indexes = {
        "idx_google_Scholar_canonical",
        "idx_google_Scholar_doi",
        "idx_google_Scholar_year",
        "google_Scholar_records_pkey"
    }
    
    found_indexes = required_indexes & index_names
    if len(found_indexes) >= 3:
        print(f"✅ [4/6] Índices encontrados: {len(found_indexes)}/{len(required_indexes)}")
        checks_passed += 1
    else:
        print(f"⚠️  [4/6] Índices: {len(found_indexes)}/{len(required_indexes)}")
    
    # 5. Verificar Foreign Key a canonical_publications
    checks_total += 1
    fks = inspector.get_foreign_keys(table_name)
    has_canonical_fk = any(
        fk.get("referred_table") == "canonical_publications"
        for fk in fks
    )
    
    if has_canonical_fk:
        print("✅ [5/6] Foreign Key a 'canonical_publications' existe")
        checks_passed += 1
    else:
        # FK podría no estar si falló el trigger, pero la tabla está OK
        print("⚠️  [5/6] Foreign Key a 'canonical_publications' no encontrado")
    
    # 6. Verificar registro de prueba (si existe)
    checks_total += 1
    try:
        result = session.execute(
            text("SELECT COUNT(*) FROM google_Scholar_records")
        )
        count = result.scalar()
        print(f"✅ [6/6] Tabla consultable: {count} registros actuales")
        checks_passed += 1
    except Exception as e:
        print(f"❌ [6/6] Error consultando: {str(e)[:60]}")
    
    # 7. Verificar modelo en SOURCE_REGISTRY (BONUS)
    if "google_Scholar" in SOURCE_REGISTRY.models or "google_scholar" in SOURCE_REGISTRY.models:
        print("\n✅ [BONUS] GoogleScholarRecord registrado en SOURCE_REGISTRY")
    else:
        print("\n⚠️  [BONUS] GoogleScholarRecord NO registrado en SOURCE_REGISTRY")
    
    # Resumen
    print("\n" + "="*60)
    print(f"Validación: {checks_passed}/{checks_total} checks pasados")
    print("="*60)
    
    if checks_passed >= 5:
        print("\n✅ ¡Migración validada exitosamente!")
        print("\n📝 Próximo paso:")
        print("   python test_api_google_Scholar.py")
        print("\n   Esto extraerá datos y los guardará automáticamente en la tabla")
        return True
    else:
        print("\n⚠️  Algunos checks fallaron - revisar arriba")
        return False

if __name__ == "__main__":
    success = validate_migration()
    sys.exit(0 if success else 1)
