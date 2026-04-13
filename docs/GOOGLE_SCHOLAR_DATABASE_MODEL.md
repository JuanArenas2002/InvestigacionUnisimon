# 📊 Modelo de BD para Google Scholar

## Estructura de Datos

### **Tabla: `google_scholar_records`**

Almacena los registros extraídos de Google Scholar de forma desnormalizada.

#### **Columnas Principales**

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | `SERIAL` | Clave primaria auto-incremental |
| `google_scholar_id` | `VARCHAR(50)` | ID único del registro en Google Scholar |
| `scholar_profile_id` | `VARCHAR(50)` | ID del perfil del que se extrajo |
| `title` | `VARCHAR(1000)` | Título de la publicación |
| `authors_json` | `JSONB` | Array de autores: `[{"name":"","orcid":null,...}]` |
| `publication_year` | `INTEGER` | Año de publicación (indices para queries rápidas) |
| `publication_date` | `VARCHAR(50)` | Fecha exacta si está disponible |
| `publication_type` | `VARCHAR(100)` | Tipo: article, conference, etc |
| `source_journal` | `VARCHAR(500)` | Nombre de la revista/conferencia |
| `issn` | `VARCHAR(20)` | ISSN de la revista |
| `doi` | `VARCHAR(100)` | Digital Object Identifier (unique index) |
| `citation_count` | `INTEGER` | Total de citas |
| `citations_by_year` | `JSONB` | Mapa año→citas: `{"2024":5,"2023":10}` |
| `url` | `TEXT` | URL del artículo en Google Scholar |
| `status` | `VARCHAR(30)` | pending \| linked \| flagged_review \| rejected |
| `raw_data` | `JSONB` | Datos crudos completos del extractor |
| `extracted_at` | `VARCHAR(50)` | Timestamp de extracción |
| `canonical_publication_id` | `INTEGER` | FK → canonical_publications(id) |
| `created_at` | `TIMESTAMP` | Cuándo se insertó en la BD |
| `updated_at` | `TIMESTAMP` | Cuándo se modificó por última vez |

---

## Flujo de Inserción Automática

### **1. Extracción (GoogleScholarExtractor)**
```
StandardRecord (extractor.py)
  ├─ source_name: "google_scholar"
  ├─ source_id: "GH12345ABC"
  ├─ title: "..."
  ├─ authors: [{"name":"...", "orcid":None, ...}]
  ├─ citation_count: 42
  └─ raw_data: {...}
```

### **2. Adaptación (GoogleScholarAdapter)**
```
StandardRecord
  ↓
Publication (domain object)
  ├─ source_name: "google_scholar"
  ├─ source_id: "GH12345ABC"
  ├─ title: "..."
  ├─ citation_count: 42
  └─ raw_data: {...}
```

### **3. Pipeline ETL**
```
Publication
  ├─ collect()      → recolecta del adapter
  ├─ deduplicate()  → elimina duplicados
  ├─ normalize()    → normaliza títulos/autores
  ├─ match()        → fuzzy matching vs canonical
  └─ enrich()       → agrega metadatos
```

### **4. Persistencia Automática**
```python
# IngestPipeline.run(persist=True) ejecuta:

# A. Guardar autores
repository.save_authors(publications)
  → INSERT INTO authors ... ON CONFLICT ...
  → UPDATE authors.external_ids['google_scholar'] = ...

# B. Guardar registros fuente
repository.save_source_records({"google_scholar": [pub1, pub2, ...]})
  → INSERT INTO google_scholar_records (...) VALUES (...)
  → Cada publication se convierte a GoogleScholarRecord

# C. Crear/actualizar canónicas
repository.upsert_canonical_publications(enriched)
  → INSERT INTO canonical_publications ...
  → UPDATE google_scholar_records SET canonical_publication_id = ...
```

---

## Ejemplos SQL

### **Insertar un registro**
```sql
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
    raw_data,
    extracted_at
) VALUES (
    'GH12345ABC',
    'V94aovUAAAAJ',
    'Safety and Efficacy of Single-Dose Ad26.COV2.S Vaccine against Covid-19',
    '[{"name":"Juan Arenas","orcid":null,"is_institutional":false}]'::jsonb,
    2021,
    'article',
    'Nature Reviews Immunology',
    '10.1038/...',
    250,
    'https://scholar.google.com/citations?...',
    'pending',
    '{"extra_field":"value"}'::jsonb,
    '2026-04-10T11:04:33'
);
```

### **Vincular a publicación canónica**
```sql
UPDATE google_scholar_records
SET canonical_publication_id = 42, status = 'linked'
WHERE id = 1;
```

### **Contar pendientes**
```sql
SELECT COUNT(*) FROM google_scholar_records WHERE status = 'pending';
```

### **Artículos por año**
```sql
SELECT publication_year, COUNT(*) as cantidad
FROM google_scholar_records
WHERE status = 'linked'
GROUP BY publication_year
ORDER BY publication_year DESC;
```

### **Más citados**
```sql
SELECT google_scholar_id, title, citation_count
FROM google_scholar_records
WHERE status = 'linked'
ORDER BY citation_count DESC
LIMIT 10;
```

---

## Integración con el Pipeline

### **Automatic Storage**

La inserción en BD es **completamente automática** vía el Pipeline:

```python
from project.config.container import build_pipeline

# Construir pipeline
pipeline = build_pipeline(["google_scholar"])

# Ejecutar → inserta automáticamente en google_scholar_records
result = pipeline.run(
    year_from=2020,
    max_results=50,
    persist=True,  # ← Activa la persistencia
    source_kwargs={
        "google_scholar": {
            "scholar_ids": ["V94aovUAAAAJ"]
        }
    }
)

# Resultado incluye conteo automático
print(f"Guardados: {result.source_saved}")  # ← Registros en google_scholar_records
```

### **Cómo Funciona Internamente**

1. **Repository.save_source_records()** itera cada fuente
2. Para "google_scholar", obtiene el modelo: `SOURCE_REGISTRY.models["google_scholar"]` → `GoogleScholarRecord`
3. Convierte cada `Publication` a kwargs específicos usando `build_google_scholar_kwargs()`
4. Ejecuta `INSERT INTO google_scholar_records ... Estos registros se crean automáticamente.

---

## Relaciones

### Principal: `google_scholar_records` → `canonical_publications`

```
google_scholar_records.canonical_publication_id
          ↓ (FK)
canonical_publications.id
```

**Usar para encontrar:**
- ¿A qué publicación canónica está vinculado este registro?
  ```sql
  SELECT c.* FROM canonical_publications c
  WHERE c.id = (SELECT canonical_publication_id FROM google_scholar_records WHERE id = 1);
  ```

- ¿Cuales registros de Google Scholar están en una canónica?
  ```sql
  SELECT * FROM google_scholar_records WHERE canonical_publication_id = 42;
  ```

### Secundaria: `authors.external_ids`

Cada autor en `authors.external_ids` tiene una clave `"google_scholar"`:

```python
# En BD:
{
    "openalex": "A123",
    "scopus": "456",
    "wos": "789",
    "cvlac": "CODE123",
    "google_scholar": "V94aovUAAAAJ"
}
```

```sql
-- Query: Autores con Scholar ID
SELECT * FROM authors 
WHERE external_ids->>'google_scholar' IS NOT NULL;
```

---

## Status del Registro

| Status | Significado |
|--------|------------|
| `pending` | Recién extraído, sin reconciliar aún |
| `linked` | Vinculado a una publicación canónica |
| `flagged_review` | Potencial discrepancia detectada |
| `rejected` | Descartado por el pipeline |

```sql
-- Monitorear reconciliación
SELECT status, COUNT(*) as cantidad
FROM google_scholar_records
GROUP BY status;
```

---

## Índices para Performance

```
- google_scholar_id       → Búsquedas por ID único
- status                  → Filtros por estado (pending, linked, etc)
- publication_year        → Queries temporales (año_from, año_to)
- canonical_publication_id → JOINs con canonical_publications
- doi                     → Matching exacto por DOI
```

---

## Queries Útiles

### **Registros sin vincular**
```sql
SELECT * FROM google_scholar_records 
WHERE canonical_publication_id IS NULL 
ORDER BY created_at DESC;
```

### **Últimos extraídos**
```sql
SELECT * FROM google_scholar_records 
ORDER BY created_at DESC 
LIMIT 20;
```

### **Distribucion por año**
```sql
SELECT publication_year, COUNT(*) as total
FROM google_scholar_records
WHERE canonical_publication_id IS NOT NULL
GROUP BY publication_year
ORDER BY publication_year DESC;
```

### **Profundidad de vinculación (cuántos artículos de GS en cada canónica)**
```sql
SELECT canonical_publication_id, COUNT(*) as gs_records
FROM google_scholar_records
WHERE canonical_publication_id IS NOT NULL
GROUP BY canonical_publication_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;
```

### **Validar integridad**
```sql
-- Registros con DOI duplicado (posible error)
SELECT doi, COUNT(*) as cantidad
FROM google_scholar_records
WHERE doi IS NOT NULL
GROUP BY doi
HAVING COUNT(*) > 1;
```

---

## Migración SQL

Para aplicar la estructura de la tabla, ejecuta:

```bash
psql -U usuario -d convocatoria < db/migration_v15_google_scholar.sql
```

O manualmente en psql:
```sql
\i db/migration_v15_google_scholar.sql
```

---

## Automático ← Lo Importante

**NO NECESITAS ejecutar SQL manualmente después de la migración inicial.**

El Pipeline automáticamente:
1. ✅ Inserta registros en `google_scholar_records`
2. ✅ Vincula a `canonical_publications`
3. ✅ Actualiza `authors.external_ids`
4. ✅ Registra en `reconciliation_log`

```python
# Todo lo anterior sucede con esto:
result = pipeline.run(persist=True)
```

---

## Monitoreo

### **Check de Salud**
```sql
-- ¿Cuántos registros tenemos?
SELECT COUNT(*) FROM google_scholar_records;

-- ¿Reconciliación en progreso?
SELECT status, COUNT(*) FROM google_scholar_records GROUP BY status;

-- ¿Algún error?
SELECT COUNT(*) FROM google_scholar_records WHERE status = 'flagged_review';
```

### **Python**
```python
from db.session import get_session
from db.models import SOURCE_REGISTRY
from sqlalchemy import func

session = get_session()
GoogleScholarRecord = SOURCE_REGISTRY.models["google_scholar"]

# Total de registros
total = session.query(func.count(GoogleScholarRecord.id)).scalar()
print(f"Total GS records: {total}")

# Por status
by_status = session.query(
    GoogleScholarRecord.status, 
    func.count(GoogleScholarRecord.id)
).group_by(GoogleScholarRecord.status).all()

for status, count in by_status:
    print(f"{status}: {count}")
```

