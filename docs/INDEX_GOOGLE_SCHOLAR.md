# 📚 Google Scholar - Documentación Completa

## 🎯 Índice de Documentación

### **1. Inicio Rápido**
- [README_GOOGLE_SCHOLAR.md](README_GOOGLE_SCHOLAR.md) - Guía de inicio rápido + primeros pasos

### **2. Arquitectura**
- [GOOGLE_SCHOLAR_ARCHITECTURE.md](GOOGLE_SCHOLAR_ARCHITECTURE.md) - Diagrama hexagonal y flujo completo

### **3. Extractores & Adaptadores**
- Ubicación: `extractors/google_scholar/extractor.py`
- Ubicación: `project/infrastructure/sources/google_scholar_adapter.py`

### **4. API Endpoints**
- [GOOGLE_SCHOLAR_ENDPOINTS.md](GOOGLE_SCHOLAR_ENDPOINTS.md) - 17+ ejemplos de uso (cURL, Python, JS, Postman)

### **5. Base de Datos**
- [GOOGLE_SCHOLAR_DATABASE_MODEL.md](GOOGLE_SCHOLAR_DATABASE_MODEL.md) - Estructura de tabla + SQL + relaciones
- [GOOGLE_SCHOLAR_MIGRATION_APPLY.md](GOOGLE_SCHOLAR_MIGRATION_APPLY.md) - Cómo aplicar la migración + validación

### **6. Testing**
- [GOOGLE_SCHOLAR_TESTING.md](GOOGLE_SCHOLAR_TESTING.md) - Guía de testing completa
- Ubicación: `test_api_google_scholar.py` - Tests de APIs
- Ubicación: `test_integration_google_scholar.py` - Tests de integración

---

## 🚀 Setup Rápido (3 Pasos)

### **1. Aplicar Migración a BD**
```bash
# Crear tabla google_scholar_records
psql -U convocatoria -d convocatoria -f db/migration_v15_google_scholar.sql

# Validar
python verify_migration.py
```

### **2. Iniciar API**
```bash
cd project
uvicorn app.main:app --reload
```

### **3. Extraer Datos**
```bash
# Opción A: cURL
curl -X POST http://localhost:8000/api/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["google_scholar"],
    "scholar_ids": ["V94aovUAAAAJ"]
  }'

# Opción B: Python
python test_api_google_scholar.py
```

---

## 📊 Flujo General

```
┌─────────────────────────────────────────────────────┐
│                  GoogleScholar API                  │
│        POST /api/ingest + scholar_ids               │
└──────────────────────────┬──────────────────────────┘
                           │
           ┌───────────────┴───────────────┐
           │                               │
           ▼                               ▼
    ┌────────────────────┐         ┌──────────────────┐
    │ GoogleScholar      │         │ Extractor Core   │
    │ Extractor          │         │ (scholarly lib)  │
    │ (StandardRecord    │         │                  │
    │  generation)       │         │ → scrape profile │
    └────────────┬───────┘         └────────┬─────────┘
                 │                          │
                 └──────────────┬───────────┘
                                │
                    ┌───────────▼────────────┐
                    │ GoogleScholarAdapter   │
                    │ (StandardRecord        │
                    │  → Publication)        │
                    └───────────┬────────────┘
                                │
                ┌───────────────▼────────────────┐
                │   IngestPipeline.run()         │
                │ ├─ deduplicate()               │
                │ ├─ normalize()                 │
                │ ├─ match()                     │
                │ └─ enrich()                    │
                └───────────────┬────────────────┘
                                │
                 ┌──────────────▼──────────────┐
                 │  Repository.save_*()        │
                 │ ├─ save_authors()           │
                 │ ├─ save_source_records()    │
                 │ └─ upsert_canonical()       │
                 └──────────────┬──────────────┘
                                │
              ┌─────────────────┴─────────────────┐
              │                                   │
              ▼                                   ▼
        ┌──────────────┐              ┌──────────────────┐
        │   authors    │              │ google_scholar   │
        │              │              │    _records      │
        │ ├─ id        │              │                  │
        │ ├─ name      │              │ ├─ id            │
        │ ├─ external_ │              │ ├─ google_       │
        │   ids (JSONB)│              │    scholar_id    │
        │ └─ ...       │              │ ├─ title         │
        └──────────────┘              │ ├─ authors_json  │
                                      │ ├─ status        │
                                      │ ├─ canonical_    │
                                      │    publication_  │
                                      │    id (FK)       │
                                      │ └─ ...           │
                                      └──────────────────┘

```

---

## 📁 Estructura de Archivos

```
c:\Users\juan.arenas1\Desktop\CONVOCATORIA\
├── docs/
│   ├── README_GOOGLE_SCHOLAR.md              ← Start here
│   ├── GOOGLE_SCHOLAR_ARCHITECTURE.md        ← Diagrama completo
│   ├── GOOGLE_SCHOLAR_ENDPOINTS.md           ← 17+ ejemplos API
│   ├── GOOGLE_SCHOLAR_DATABASE_MODEL.md      ← Tabla + SQL
│   ├── GOOGLE_SCHOLAR_MIGRATION_APPLY.md     ← Pasos de migración
│   ├── GOOGLE_SCHOLAR_TESTING.md             ← Tests
│   └── INDEX.md                              ← Este archivo
│
├── extractors/
│   ├── google_scholar/
│   │   ├── extractor.py                      ← Core de extracción
│   │   └── __init__.py
│   └── __init__.py
│
├── project/
│   ├── app/
│   │   ├── main.py                           ← FastAPI app
│   │   └── routes/
│   │       └── ingest.py                     ← POST /api/ingest
│   │
│   └── infrastructure/
│       └── sources/
│           └── google_scholar_adapter.py     ← StandardRecord → Publication
│
├── db/
│   ├── sources/
│   │   ├── google_scholar.py                 ← GoogleScholarRecord model
│   │   ├── openalex.py
│   │   ├── scopus.py
│   │   ├── wos.py
│   │   ├── cvlac.py
│   │   ├── datos_abiertos.py
│   │   └── __init__.py                       ← Auto-loader plugin system
│   │
│   ├── models.py                             ← Base models
│   ├── source_registry.py                    ← Central registry
│   ├── session.py                            ← DB session factory
│   └── migration_v15_google_scholar.sql      ← SQL migration
│
├── test_api_google_scholar.py                ← API tests
├── test_integration_google_scholar.py        ← Integration tests
├── verify_migration.py                       ← Migration validation
│
└── requirements.txt                          ← scholarly, etc.
```

---

## 🔑 Conceptos Clave

### **StandardRecord** (Nivel de Extractor)
```python
{
    "source_name": "google_scholar",
    "source_id": "GH12345ABC",
    "title": "...",
    "authors": [{"name":"...", "orcid":None, ...}],
    "publication_year": 2024,
    "citation_count": 42,
    "raw_data": {...}
}
```

### **Publication** (Nivel de Dominio)
```python
{
    "source_name": "google_scholar",
    "source_id": "GH12345ABC",
    "title": "...",
    "authors": [...],
    "citation_count": 42,
    "raw_data": {...},
    "metadata": {...}
}
```

### **GoogleScholarRecord** (Nivel de BD)
```python
# Tabla: google_scholar_records
# Columnas principales:
# - google_scholar_id: ID único
# - scholar_profile_id: ID del perfil
# - title: Título
# - authors_json: Array JSONB
# - citation_count: Total citas
# - status: pending|linked|flagged_review|rejected
# - canonical_publication_id: FK a canónicas
```

---

## ✅ Estado Actual

| Componente | Estado | Notas |
|-----------|--------|-------|
| Extractor | ✅ Completo | scholarly library, robust error handling |
| Adapter | ✅ Completo | Full field extraction |
| API Endpoint | ✅ Completo | POST /api/ingest con scholar_ids |
| Database Model | ✅ Completo | GoogleScholarRecord con JSONB |
| Source Registry | ✅ Completo | Auto-discovery plugin system |
| Migración SQL | ✅ Completo | migration_v15_google_scholar.sql |
| Documentación | ✅ Completa | 5 archivos .md + ejemplos |
| Tests | ✅ Implementados | test_api + test_integration |

---

## 🎓 Próximos Pasos

### **Paso 1: Aplicar Migración** ← START HERE
```bash
# Ver instrucciones detalladas en GOOGLE_SCHOLAR_MIGRATION_APPLY.md
psql -U convocatoria -d convocatoria -f db/migration_v15_google_Scholar.sql
python verify_migration.py
```

### **Paso 2: Probar API**
```bash
# Opción A: Postman
# Importar colección desde GOOGLE_SCHOLAR_ENDPOINTS.md

# Opción B: cURL
curl -X POST http://localhost:8000/api/ingest \
  -H "Content-Type: application/json" \
  -d '{"sources":["google_scholar"],"scholar_ids":["V94aovUAAAAJ"]}'

# Opción C: Python
python test_api_google_scholar.py
```

### **Paso 3: Ejecutar Tests Completos**
```bash
python test_integration_google_scholar.py
```

### **Paso 4: Monitorear Datos**
```bash
# Ver registros guardados
psql -U convocatoria -d convocatoria -c \
  "SELECT COUNT(*) as total, status FROM google_scholar_records GROUP BY status;"
```

---

## 📞 Debugging

### **Ver Logs del Extractor**
```python
# Añadir a código:
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
```

### **Validar Conexión a BD**
```bash
psql -U convocatoria -d convocatoria -c "SELECT version();"
```

### **Verificar Tabla Existe**
```bash
psql -U convocatoria -d convocatoria -c "
  SELECT table_name FROM information_schema.tables 
  WHERE table_name = 'google_scholar_records';
"
```

### **Ver Registros**
```bash
psql -U convocatoria -d convocatoria -c \
  "SELECT id, title, status FROM google_Scholar_records LIMIT 10;"
```

---

## 📖 Recursos Adicionales

- [Scholarly Docs](https://scholarly.readthedocs.io/)
- [FastAPI Docs](https://fastapi.tiangolo.com/)
- [SQLAlchemy ORM](https://docs.sqlalchemy.org/)
- [PostgreSQL JSONB](https://www.postgresql.org/docs/current/datatype-json.html)

