# 🎓 Google Scholar - Documentación Completa

## 📚 Índice de Documentación

### **1. Extracción y Testing**
- [Google Scholar Testing Guide](./GOOGLE_SCHOLAR_TESTING.md)
  - Instalación de dependencias
  - Cómo obtener Scholar IDs
  - Scripts de prueba paso a paso
  - Validación de datos

### **2. API Endpoints**
- [Google Scholar API Endpoints](./GOOGLE_SCHOLAR_ENDPOINTS.md)
  - Endpoint `/ingest` con Google Scholar
  - Ejemplos en cURL, Python, JavaScript
  - Postman collection
  - Manejo de errores
  - Casos de uso avanzados

### **3. Arquitectura**
- [GOOGLE_SCHOLAR_ARCHITECTURE.py](../GOOGLE_SCHOLAR_ARCHITECTURE.py)
  - Arquitectura hexagonal
  - Flujo de datos
  - Componentes del sistema
  - Pasos de integración

---

## 🚀 Inicio Rápido

### **Opción 1: Prueba Standalone (sin API)**

```bash
# Instalar dependencias
pip install scholarly

# Test del extractor
python test_google_scholar.py

# Exportar a JSON
python export_google_scholar_json.py
```

### **Opción 2: API Endpoints (with BD)**

```bash
# 1. Iniciar API
uvicorn api.main:app --reload --port 8000

# 2. Probar endpoints
python test_api_google_scholar.py

# 3. Ver documentación interactiva
# → http://localhost:8000/docs
```

### **Opción 3: Pipeline Hexagonal (sin BD)**

```python
from project.config.container import build_pipeline

pipeline = build_pipeline(["google_scholar"])
result = pipeline.run(
    year_from=2020,
    max_results=10,
    persist=False,
    source_kwargs={"google_scholar": {"scholar_ids": ["V94aovUAAAAJ"]}}
)
```

---

## 📊 Flujo de Datos

```
Google Scholar Profile (scholar_ids)
           ↓
extractors/google_scholar/ (StandardRecord)
           ↓
GoogleScholarAdapter (Publication domain object)
           ↓
IngestPipeline (ETL)
  • collect    → recolecta de la API
  • deduplicate → elimina duplicados
  • normalize  → normaliza títulos, autores
  • match      → fuzzy matching
  • enrich     → agrega metadatos
           ↓
Repository (PostgreSQL)
  • save_authors
  • save_source_records
  • upsert_canonical_publications
```

---

## 🔑 Conceptos Clave

### **Scholar ID**
Identificador único en Google Scholar. Formato: `V94aovUAAAAJ`

Cómo obtener:
1. Ve a [https://scholar.google.com/](https://scholar.google.com/)
2. Busca un autor o tu perfil
3. URL: `https://scholar.google.com/citations?user=**V94aovUAAAAJ**`
4. Scholar ID = `V94aovUAAAAJ`

### **StandardRecord**
Formato interno de salida de extractores (antes de adaptación).

```python
{
    "source_name": "google_scholar",
    "title": "...",
    "authors": [{"name": "...", "orcid": None, ...}],
    "publication_year": 2024,
    "doi": "...",
    "citation_count": 42,
    "url": "...",
    ...
}
```

### **Publication (Domain Object)**
Objeto de dominio después de adaptación (ya con todas las validaciones).

```python
Publication(
    source_name="google_scholar",
    title="...",
    authors=[Author(name="...", orcid=None, ...)],
    publication_year=2024,
    ...
)
```

---

## 📁 Archivos Principales

### **Aplicación**
```
project/
├── infrastructure/sources/
│   └── google_scholar_adapter.py      ← Adapter principal
├── ports/
│   └── source_port.py                 ← Interfaz (SourcePort)
├── registry/
│   └── source_registry.py             ← Plugin system
└── app/routes/
    └── ingest.py                      ← Endpoint /ingest
```

### **Testing**
```
test_google_scholar.py                 ← Pruebas unitarias
test_api_google_scholar.py             ← Pruebas de API
test_integration_google_scholar.py     ← Pruebas de integración
export_google_scholar_json.py          ← Exportación a JSON
quick_test_scholar_id.py               ← Validación rápida
```

### **Extracción**
```
extractors/google_scholar/
├── extractor.py                       ← Lógica principal
├── application/
│   └── profile_service.py             ← Servicios de aplicación
├── infrastructure/
├── domain/
└── _exceptions.py
```

---

## 🧪 Pruebas Disponibles

### **1. Test del Extractor**
```bash
python test_google_scholar.py
```
Prueba:
- ✅ Dependencias instaladas
- ✅ Extractor importable
- ✅ Conexión a Google Scholar
- ✅ Estructura de datos
- ✅ Validación de records

### **2. Test de Integración**
```bash
python test_integration_google_scholar.py
```
Prueba:
- ✅ Registry detecta Google Scholar
- ✅ Adapter funciona
- ✅ Pipeline ETL completo
- ✅ Servicios de dominio

### **3. Test de API**
```bash
# Requiere: uvicorn api.main:app --reload
python test_api_google_scholar.py
```
Prueba:
- ✅ Conectar a API
- ✅ Endpoint /ingest
- ✅ Dry run (sin guardar)
- ✅ Extracción real (con BD)

---

## 💻 Ejemplos de Código

### **Script Autónomo (sin API)**
```python
from extractors.google_scholar.extractor import GoogleScholarExtractor

extractor = GoogleScholarExtractor()
records = extractor.extract(
    scholar_ids=["V94aovUAAAAJ"],
    year_from=2020,
    max_results=10
)

for record in records:
    print(f"{record.title} ({record.publication_year})")
```

### **Vía Adapter**
```python
from project.infrastructure.sources.google_scholar_adapter import GoogleScholarAdapter

adapter = GoogleScholarAdapter()
publications = adapter.fetch_records(
    scholar_ids=["V94aovUAAAAJ"],
    year_from=2020,
    max_results=10
)

for pub in publications:
    print(f"{pub.title}: {pub.citation_count} citas")
```

### **Vía Pipeline**
```python
from project.config.container import build_pipeline

pipeline = build_pipeline(["google_scholar"])
result = pipeline.run(
    year_from=2020,
    max_results=10,
    persist=True,  # Guardar en BD
    source_kwargs={"google_scholar": {"scholar_ids": ["V94aovUAAAAJ"]}}
)

print(f"Recolectados: {result.collected}")
print(f"Normalizados: {result.normalized}")
print(f"Canónicas creadas: {result.canonical_upserted}")
```

### **Vía API REST**
```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["google_scholar"],
    "scholar_ids": ["V94aovUAAAAJ"],
    "year_from": 2020,
    "max_results": 50,
    "dry_run": false
  }'
```

---

## 📊 Estadísticas de Respuesta

**Respuesta típica del endpoint `/ingest`:**

```json
{
  "status": "ok",
  "selected_sources": ["google_scholar"],
  "stages": {
    "collect": 15,
    "deduplicate": 15,
    "normalize": 15,
    "match": 12,
    "enrich": 12
  },
  "persistence": {
    "authors_saved": 45,
    "source_saved": 15,
    "canonical_upserted": 10,
    "dry_run": false
  },
  "by_source": {
    "google_scholar": 15
  },
  "errors": {}
}
```

**Interpretación:**
- 15 artículos extraídos
- Deduplicados y normalizados
- 12 coincidencias encontradas en BD
- 10 nuevas publicaciones canónicas creadas
- 45 autores guardados
- Sin errores

---

## 🔧 Configuración

### **Environment Variables**
```bash
# PostgreSQL (para persistencia)
DATABASE_URL=postgresql://user:pass@localhost/convocatoria

# Google Scholar (rate limiting)
GS_PROXY_ENABLED=false  # true si necesitas proxy

# Logging
LOG_LEVEL=INFO  # DEBUG para más detalle
```

### **Pipeline Configuration**
```python
# En project/config/container.py
pipeline = build_pipeline(
    source_names=["google_scholar"],  # Solo esta fuente
    # o múltiples:
    # source_names=["google_scholar", "openalex", "scopus"]
)
```

---

## 🚨 Troubleshooting

### **"Scholar ID not found"**
- Verifica que el ID sea correcto: `https://scholar.google.com/citations?user=**V94aovUAAAAJ**`
- Asegúrate que el perfil sea público
- Prueba con: `python quick_test_scholar_id.py`

### **"Rate limit exceeded"**
- Espera 1-2 horas
- Reduce `max_results`
- Implementa proxy (ver docs avanzadas)

### **"API connection refused"**
- Inicia el servidor: `uvicorn api.main:app --reload`
- Verifica puerto: `:8000`
- Comprueba firewall

### **"Database connection error"**
- Verifica PostgreSQL corriendo
- Comprueba `DATABASE_URL`
- Usa `persist=False` para debugging

---

## 📚 Recursos Adicionales

### **Google Scholar**
- Sitio: https://scholar.google.com/
- Búsqueda de perfiles: https://scholar.google.com/citations

### **Scholarly Library**
- GitHub: https://github.com/scholarly-python-package/scholarly
- Documentación: Incluida en el repo
- Issues/Community: GitHub discussions

### **Proyecto**
- Architecture: Hexagonal/Clean Architecture
- Tests: pytest
- API: FastAPI
- DB: PostgreSQL
- Reconciliation: Fuzzy matching + DOI exact

---

## ✅ Checklist de Implementación

- [x] Extractor de Google Scholar funcional
- [x] Adapter integrado en arquitectura hexagonal
- [x] Registry detecta automáticamente
- [x] Pipeline ETL completo
- [x] Endpoint `/ingest` con soporte
- [x] Tests unitarios
- [x] Tests de integración
- [x] Tests de API
- [x] Documentación completa
- [ ] Persistencia en BD (opcional)
- [ ] Scheduler de extracciones (opcional)
- [ ] Caché con Redis (opcional)
- [ ] Proxy para rate limiting (opcional)

---

## 🎯 Próximos Pasos

1. **Corta plazo:**
   - Ejecutar `test_api_google_scholar.py` con API corriendo
   - Verificar extracción real a BD
   - Validar datos en publicaciones canónicas

2. **Mediano plazo:**
   - Implementar scheduler (Celery/APScheduler)
   - Almacenar mapping researcher ↔ Scholar IDs
   - Caché de extracciones

3. **Largo plazo:**
   - Proxy para throttling
   - Análisis de tendencias
   - Métricas de productividad

---

## 📞 Soporte

Para preguntas o problemas:
1. Consulta la documentación específica arriba
2. Revisa `docs/GOOGLE_SCHOLAR_ENDPOINTS.md`
3. Ejecuta los tests: `python test_integration_google_scholar.py`
4. Revisa los logs de la API

