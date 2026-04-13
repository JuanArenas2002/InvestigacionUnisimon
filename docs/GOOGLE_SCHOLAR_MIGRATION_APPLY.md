# 🚀 Aplicar Migración - Google Scholar BD

## Paso 1: Verificar Conexión a BD

```bash
# Test de conexión
psql -U convocatoria -d convocatoria -c "SELECT version();"
```

**Esperado:**
```
PostgreSQL 14.x on x86_64-...
(1 row)
```

---

## Paso 2: Aplicar Migración

### Opción A: Desde Terminal (Recomendado)

```bash
cd c:\Users\juan.arenas1\Desktop\CONVOCATORIA

psql -U convocatoria -d convocatoria -f db/migration_v15_google_scholar.sql
```

**Salida esperada:**
```
CREATE TABLE
CREATE INDEX
CREATE INDEX
CREATE INDEX
CREATE INDEX
CREATE INDEX
```

### Opción B: Desde psql Interactivo

```bash
psql -U convocatoria -d convocatoria

# Dentro de psql:
\i db/migration_v15_google_scholar.sql
```

---

## Paso 3: Verificar Tabla Creada

```bash
psql -U convocatoria -d convocatoria -c "\dt google_scholar_records;"
```

**Esperado:**
```
                 List of relations
 Schema |           Name            | Type  | Owner
--------+---------------------------+-------+-------
 public | google_scholar_records    | table | convocatoria
(1 row)
```

### Ver estructura completa

```bash
psql -U convocatoria -d convocatoria -c "\d+ google_scholar_records;"
```

---

## Paso 4: Verificar Índices

```bash
psql -U convocatoria -d convocatoria -c "SELECT indexname FROM pg_indexes WHERE tablename='google_scholar_records';"
```

**Esperado:**
```
                        indexname
-------------------------------------------------------
 google_scholar_records_pkey
 idx_gs_google_scholar_id
 idx_gs_scholar_profile_id
 idx_gs_status
 idx_gs_publication_year
 idx_gs_doi
 idx_gs_canonical_publication_id
(7 rows)
```

---

## Paso 5: Validar Relación Foreign Key

```bash
psql -U convocatoria -d convocatoria << EOF
SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'google_scholar_records'
AND constraint_type = 'FOREIGN KEY';
EOF
```

**Esperado:**
```
        constraint_name         | constraint_type
--------------------------------+-----------------
 fk_gs_canonical_publication_id | FOREIGN KEY
(1 row)
```

---

## Paso 6: Insertar Registro de Prueba

```bash
psql -U convocatoria -d convocatoria << EOF
INSERT INTO google_scholar_records (
    google_scholar_id,
    scholar_profile_id,
    title,
    authors_json,
    publication_year,
    publication_type,
    source_journal,
    doi,
    citation_count,
    url,
    status,
    extracted_at
) VALUES (
    'TEST_001',
    'V94aovUAAAAJ',
    'Test Publication for Schema Validation',
    '[{"name":"Test Author","orcid":null,"is_institutional":false}]'::jsonb,
    2024,
    'article',
    'Test Journal',
    '10.1234/test.001',
    5,
    'https://scholar.google.com/test',
    'pending',
    '2026-04-10T11:04:33'
);
EOF
```

**Esperado:**
```
INSERT 0 1
```

---

## Paso 7: Verificar Registro Insertado

```bash
psql -U convocatoria -d convocatoria -c "SELECT * FROM google_scholar_records WHERE google_scholar_id = 'TEST_001';"
```

**Esperado:**
```
 id | google_scholar_id | scholar_profile_id | title | authors_json | ... | status | ...
----+-------------------+--------------------+-------+--------------+-----+--------+
  1 | TEST_001          | V94aovUAAAAJ       | Test  | [{"name"...  | ... | pending
```

---

## Paso 8: Verificar JSONB Fields

```bash
psql -U convocatoria -d convocatoria -c "SELECT id, authors_json FROM google_scholar_records LIMIT 1;"
```

**Esperado:**
```
 id |                                   authors_json
----+---------------------------------------------------------------------------
  1 | [{"name":"Test Author","orcid":null,"is_institutional":false}]
```

---

## Paso 9: Limpiar (Opcional - solo para testing)

```bash
# Eliminar registro de prueba
psql -U convocatoria -d convocatoria -c "DELETE FROM google_scholar_records WHERE google_scholar_id = 'TEST_001';"
```

---

## Validación desde Python

Crea un script `verify_migration.py` en la raíz:

```python
#!/usr/bin/env python
"""Verificar que la migración se aplicó correctamente"""

from db.session import SessionLocal, engine
from db.models import SOURCE_REGISTRY
from sqlalchemy import inspect

def verify():
    print("🔍 Verificando migración de Google Scholar...")
    
    # 1. Verificar tabla existe
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    if "google_scholar_records" not in tables:
        print("❌ Tabla google_scholar_records NO existe")
        return False
    
    print("✅ Tabla google_scholar_records existe")
    
    # 2. Verificar columnas
    columns = inspector.get_columns("google_scholar_records")
    column_names = [c["name"] for c in columns]
    
    required_columns = [
        "id", "google_scholar_id", "scholar_profile_id", "title",
        "authors_json", "publication_year", "doi", "citation_count",
        "status", "canonical_publication_id", "created_at", "updated_at"
    ]
    
    missing = [c for c in required_columns if c not in column_names]
    if missing:
        print(f"❌ Columnas faltantes: {missing}")
        return False
    
    print(f"✅ Todas las columnas requeridas existen ({len(required_columns)})")
    
    # 3. Verificar índices
    indexes = inspector.get_indexes("google_Scholar_records")
    index_names = [idx["name"] for idx in indexes]
    
    required_indexes = [
        "idx_gs_google_scholar_id",
        "idx_gs_status",
        "idx_gs_publication_year"
    ]
    
    found_indexes = [idx for idx in required_indexes if idx in index_names]
    print(f"✅ Índices encontrados: {len(found_indexes)}/{len(required_indexes)}")
    
    # 4. Verificar Foreign Key
    pk_constraints = inspector.get_pk_constraint("google_scholar_records")
    print(f"✅ Clave primaria: {pk_constraints}")
    
    # 5. Verificar modelo en SOURCE_REGISTRY
    if "google_scholar" not in SOURCE_REGISTRY.models:
        print("❌ GoogleScholarRecord NO registrado en SOURCE_REGISTRY")
        return False
    
    print("✅ GoogleScholarRecord registrado en SOURCE_REGISTRY")
    
    # 6. Test de inserción
    session = SessionLocal()
    try:
        from sqlalchemy import text
        result = session.execute(
            text("SELECT COUNT(*) FROM google_scholar_records")
        )
        count = result.scalar()
        print(f"✅ Tabla consultable: {count} registros")
    except Exception as e:
        print(f"❌ Error consultando tabla: {e}")
        return False
    finally:
        session.close()
    
    print("\n✅ ¡Migración validada exitosamente!")
    return True

if __name__ == "__main__":
    import sys
    success = verify()
    sys.exit(0 if success else 1)
```

**Ejecutar:**
```bash
python verify_migration.py
```

**Esperado:**
```
🔍 Verificando migración de Google Scholar...
✅ Tabla google_scholar_records existe
✅ Todas las columnas requeridas existen (12)
✅ Índices encontrados: 3/3
✅ Clave primaria: {'constrained_columns': ['id']}
✅ GoogleScholarRecord registrado en SOURCE_REGISTRY
✅ Tabla consultable: 0 registros

✅ ¡Migración validada exitosamente!
```

---

## Troubleshooting

### Error: "permission denied for schema public"

```bash
# Verificar usuario
psql -U convocatoria -d convocatoria -c "SELECT current_user;"
```

Si no tienes permisos, ejecuta como superuser:

```bash
psql -U postgres -d convocatoria -f db/migration_v15_google_scholar.sql
```

### Error: "table already exists"

```bash
# Eliminar tabla anterior
psql -U convocatoria -d convocatoria -c "DROP TABLE IF EXISTS google_scholar_records CASCADE;"

# Reintentar migración
psql -U convocatoria -d convocatoria -f db/migration_v15_google_Scholar.sql
```

### Error: "foreign key constraint failed"

Asegurate que `canonical_publications` existe:

```bash
psql -U convocatoria -d convocatoria -c "SELECT COUNT(*) FROM canonical_publications;"
```

Si no existe, ejecuta migraciones previas primero.

---

## Siguiente Paso

Una vez validada la migración, ejecutar:

```bash
python test_api_google_scholar.py
```

Para verificar que los datos se persisten correctamente.

