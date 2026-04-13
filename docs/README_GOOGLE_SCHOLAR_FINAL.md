# 📚 Google Scholar - Documentación Final Integrada

## 🎯 Estado

**✅ COMPLETADO:** Sistema de Google Scholar totalmente integrado en la API

## 🚀 Inicio Rápido (30 segundos)

### 1️⃣ Iniciar la API
```bash
cd c:\Users\juan.arenas1\Desktop\CONVOCATORIA
uvicorn api.main:app --reload --port 8000
```

### 2️⃣ Abrir Swagger UI
```
http://localhost:8000/docs
```

### 3️⃣ Probar Google Scholar
- En Swagger, busca **"Google Scholar"**
- Haz clic en `GET /api/scholar/test` → "Try it out" → "Execute"

---

## 📖 Documentación Completa

### Archivos de Referencia

| Documento | Propósito |
|-----------|----------|
| [GOOGLE_SCHOLAR_TESTING_API.md](GOOGLE_SCHOLAR_TESTING_API.md) | **⭐ EMPIEZA AQUÍ** - Guía de testing |
| [GOOGLE_SCHOLAR_ENDPOINTS.md](GOOGLE_SCHOLAR_ENDPOINTS.md) | 17+ ejemplos con cURL, Python, JS |
| [GOOGLE_SCHOLAR_DATABASE_MODEL.md](GOOGLE_SCHOLAR_DATABASE_MODEL.md) | Estructura de tabla + SQL |
| [GOOGLE_SCHOLAR_ARCHITECTURE.md](GOOGLE_SCHOLAR_ARCHITECTURE.md) | Arquitectura hexagonal |
| [INDEX_GOOGLE_SCHOLAR.md](INDEX_GOOGLE_SCHOLAR.md) | Índice maestro |

### Scripts de Testing

| Script | Propósito |
|--------|----------|
| `test_scholar_api.py` | **Recomendado** - Tests automáticos completos |
| `test_api_google_Scholar.py` | Tests de endpoints |

---

## 🧪 Testing (4 Opciones)

### Opción A: Script Automático (⭐ RECOMENDADO)
```bash
python test_scholar_api.py
```

- ✅ Verifica conexión a API
- ✅ Info de Google Scholar
- ✅ Extracción dry-run
- ✅ Extracción con persistencia (opcional)
- ✅ Verifica BD

### Opción B: Swagger UI (GUI Interactivo)
```
http://localhost:8000/docs
```

- Busca "Google Scholar"
- Click en endpoint
- "Try it out"
- "Execute"

### Opción C: cURL (Terminal)
```bash
# Test info
curl -X GET "http://localhost:8000/api/scholar/test"

# Extracción dry-run
curl -X POST "http://localhost:8000/api/scholar/extract?scholar_ids=%5B%22V94aovUAAAAJ%22%5D&dry_run=true"

# Extracción guardando
curl -X POST "http://localhost:8000/api/scholar/extract?scholar_ids=%5B%22V94aovUAAAAJ%22%5D&dry_run=false"
```

### Opción D: Python/Requests
```python
import requests

# Info
resp = requests.get("http://localhost:8000/api/scholar/test")
print(resp.json())

# Extracción
resp = requests.post(
    "http://localhost:8000/api/scholar/extract",
    params={
        "scholar_ids": ["V94aovUAAAAJ"],
        "dry_run": False
    }
)
print(resp.json())
```

---

## 📊 Endpoints en api/main.py

### 1. GET `/api/scholar/test`

**Info y ejemplos de cómo usar Google Scholar**

```bash
curl http://localhost:8000/api/scholar/test
```

Retorna:
- Información del servicio
- Scholar IDs de ejemplo
- Campos que se extraen
- Esquema de tabla
- Próximos pasos

### 2. POST `/api/scholar/extract`

**Extrae publicaciones de Google Scholar**

Parámetros:
- `scholar_ids`: List[str] - Scholar IDs (ej: ["V94aovUAAAAJ"])
- `year_from`: int = 2020
- `year_to`: int = 2024
- `max_results`: int = 50
- `dry_run`: bool = true (si false, guarda en BD)

Ejemplo:
```bash
curl -X POST "http://localhost:8000/api/scholar/extract?scholar_ids=%5B%22V94aovUAAAAJ%22%5D&dry_run=false"
```

### 3. POST `/api/hex/ingest` (Alternativa)

**Endpoint hexagonal con todas las fuentes**

```json
POST /api/hex/ingest
{
  "sources": ["google_Scholar"],
  "scholar_ids": ["V94aovUAAAAJ"],
  "dry_run": false
}
```

---

## 🔍 Cómo Encontrar Scholar IDs

### En Google Scholar
1. Ir a https://scholar.google.com
2. Buscar investigador
3. Copiar el ID de la URL:
   ```
   https://scholar.google.com/citations?user=V94aovUAAAAJ
                                             ↑↑↑↑↑↑↑↑↑↑↑↑↑
   ```

### Ejemplos
- Gustavo Aroca Martinez: `V94aovUAAAAJ`
- Simón Bauer: `jzXp-fUAAAAJ`

---

## 📋 Flujo Completo

```
1. Extracción (GET Scholar)
   ↓
   POST /api/scholar/extract
   ↓
2. Conversión
   └─ scholarly library → StandardRecord → Publication
   ↓
3. BD Intermedia
   └─ google_Scholar_records (status=pending)
   ↓
4. Reconciliación (Opcional)
   └─ POST /api/pipeline/reconcile-all
   ↓
5. BD Canónica
   └─ canonical_publications (vinculados)
```

---

## 🗄️ Base de Datos

### Tabla: `google_Scholar_records`

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id` | SERIAL | Clave primaria |
| `google_Scholar_id` | VARCHAR(50) | ID único del registro |
| `scholar_profile_id` | VARCHAR(50) | ID del perfil origen |
| `title` | VARCHAR(1000) | Título del artículo |
| `authors_json` | JSONB | Array de autores |
| `publication_year` | INTEGER | Año de publicación |
| `doi` | VARCHAR(100) | Digital Object Identifier |
| `citation_count` | INTEGER | Total de citas |
| `citations_by_year` | JSONB | Citas por año |
| `status` | VARCHAR(30) | pending/linked/flagged/rejected |
| `canonical_publication_id` | FK | Link a canonical_publications |

### Consultas Útiles

```sql
-- Contar registros
SELECT COUNT(*) FROM google_Scholar_records;

-- Ver por status
SELECT status, COUNT(*) FROM google_Scholar_records GROUP BY status;

-- Últimos insertados
SELECT * FROM google_Scholar_records ORDER BY created_at DESC LIMIT 10;

-- Más citados
SELECT title, citation_count FROM google_Scholar_records 
ORDER BY citation_count DESC LIMIT 10;
```

---

## ⚙️ Configuración

### Variables de Entorno

No se requieren variables especiales para Google Scholar (usa web scraping).

Opcional:
```
# En .env
GOOGLE_SCHOLAR_MIN_DELAY=1      # Mínimo delay entre requests (seg)
GOOGLE_SCHOLAR_MAX_DELAY=3      # Máximo delay entre requests (seg)
```

### Dependencias

```
scholarly>=1.7           # Web scraping de Google Scholar
fastapi>=0.104          # Framework API
sqlalchemy>=2.0         # ORM
psycopg2-binary         # Driver PostgreSQL
```

Instalar:
```bash
pip install scholarly fastapi sqlalchemy psycopg2-binary
```

---

## 🐛 Troubleshooting

### "No se puede conectar a API"
```bash
# Verificar que está corriendo:
uvicorn api.main:app --reload --port 8000
```

### "Module 'scholarly' not found"
```bash
pip install scholarly
```

### "No se pudo conectar a BD"
- Verificar PostgreSQL está corriendo
- Verificar credenciales en config.py
- Ejecutar migrate v15: `python apply_migration_v15.py`

### "Scholar ID inválido"
- Verificar que ID tenga el formato correcto (después de `user=` en URL)
- Probar manualmente: https://scholar.google.com/citations?user=V94aovUAAAAJ

---

## 📈 Monitoreo

### Health Check
```bash
curl http://localhost:8000/api/stats/health
```

### Estadísticas
```bash
curl http://localhost:8000/api/stats/summary
```

### Registros en BD
```bash
curl "http://localhost:8000/api/publications?source=google_Scholar"
```

---

## 🎓 Arquitectura Técnica

### Componentes

```
Arquitectura Hexagonal:
├─ Puertos (Interfaz)
│  └─ SourcePort
├─ Adaptadores
│  └─ GoogleScholarAdapter
│     ├─ fetch_records() → Publication[]
│     └─ StandardRecord → Publication mapping
├─ Dominio
│  └─ Publication
│     ├─ source_name: "google_Scholar"
│     ├─ source_id
│     ├─ title, authors, year
│     └─ raw_data
├─ Infraestructura
│  ├─ db/sources/google_Scholar.py
│  │  └─ GoogleScholarRecord ORM
│  ├─ db/source_registry.py
│  │  └─ Plugin system auto-discovery
│  └─ Pipeline
│     ├─ collect()
│     ├─ deduplicate()
│     ├─ normalize()
│     ├─ match()
│     └─ enrich()
└─ BD
   ├─ google_Scholar_records (intermedia)
   └─ canonical_publications (final)
```

### Flujo de Datos

```python
# 1. Extracción
scholarly.search_pubs_query() → StandardRecord[]

# 2. Adaptación
GoogleScholarAdapter.to_publication()

# 3. Pipeline
pipeline.run(publications)
  └─ deduplicate, normalize, match, enrich

# 4. Persistencia
repository.save_source_records() → INSERT INTO google_Scholar_records

# 5. Reconciliación
pipeline.reconcile_with_canonical() → canonical_publications
```

---

## 📚 Ejemplos de Uso

### Uso Básico
```python
from project.config.container import build_pipeline

pipeline = build_pipeline(["google_Scholar"])

result = pipeline.run(
    source_kwargs={
        "google_Scholar": {
            "scholar_ids": ["V94aovUAAAAJ"]
        }
    },
    persist=True
)

print(f"Extraídos: {result.collected}")
print(f"Guardados: {result.source_saved}")
```

### Con FastAPI
```python
from fastapi import FastAPI

app = FastAPI()

@app.post("/extract-scholar")
async def extract_scholar(scholar_ids: list[str]):
    pipeline = build_pipeline(["google_Scholar"])
    result = await pipeline.run(
        source_kwargs={"google_Scholar": {"scholar_ids": scholar_ids}},
        persist=True
    )
    return result
```

### Con Requests
```python
import requests

response = requests.post(
    "http://localhost:8000/api/scholar/extract",
    params={
        "scholar_ids": ["V94aovUAAAAJ"],
        "dry_run": False
    }
)

data = response.json()
print(f"Extraídos: {data['extraidos']}")
print(f"Guardados: {data['guardados']}")
```

---

## ✅ Checklist de Implementación

- ✅ Extractor Google Scholar (scholarly library)
- ✅ Adapter hexagonal (StandardRecord → Publication)
- ✅ Modelo ORM (GoogleScholarRecord)
- ✅ Plugin system (auto-discovery SourceRegistry)
- ✅ Migración SQL v15
- ✅ Endpoints en api/main.py
- ✅ Documentación completa (5 archivos .md)
- ✅ Scripts de testing (2 scripts)
- ✅ Persistencia automática
- ✅ Reconciliación compatible

---

## 🚀 Próximos Pasos

1. **Ejecutar test:**
   ```bash
   python test_scholar_api.py
   ```

2. **Abrir Swagger:**
   ```
   http://localhost:8000/docs
   ```

3. **Extraer datos:**
   - POST /api/scholar/extract
   - Scholar IDs: ["V94aovUAAAAJ"]
   - dry_run: false

4. **Verificar BD:**
   ```bash
   psql -U usuario -d convocatoria \
     -c "SELECT COUNT(*) FROM google_Scholar_records;"
   ```

5. **Reconciliar:**
   ```bash
   curl -X POST http://localhost:8000/api/pipeline/reconcile-all
   ```

---

## 📞 Soporte

### Documentación
- [GOOGLE_SCHOLAR_TESTING_API.md](GOOGLE_SCHOLAR_TESTING_API.md) - Guía completa
- [GOOGLE_SCHOLAR_ENDPOINTS.md](GOOGLE_SCHOLAR_ENDPOINTS.md) - Ejemplos
- `/docs` - Swagger UI interactivo

### Código
- `test_scholar_api.py` - Tests automáticos
- `api/main.py` - Endpoints
- `project/infrastructure/sources/google_Scholar_adapter.py` - Adapter
- `db/sources/google_Scholar.py` - Modelo

---

## 🎉 ¡Listo!

**Google Scholar está completamente integrado en tu API.**

Ejecuta:
```bash
# Terminal 1: API
uvicorn api.main:app --reload

# Terminal 2: Tests
python test_scholar_api.py

# Navegador: Swagger
http://localhost:8000/docs
```

¡Que disfrutes! 🚀

