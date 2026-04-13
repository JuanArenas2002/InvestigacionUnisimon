#!/usr/bin/env python
"""
RESUMEN: Integración de Google Scholar en Arquitectura Hexagonal

Este archivo documenta cómo Google Scholar ha sido integrado en la arquitectura
limpia/hexagonal del proyecto, siguiendo los 3 pasos:

  1. Crear adapter en project/infrastructure/sources/
  2. Registry detecta automáticamente (plugin system)
  3. Pipeline ejecuta la extracción

═══════════════════════════════════════════════════════════════════════════════
COMPONENTES
═══════════════════════════════════════════════════════════════════════════════

┌─ CAPA DE PUERTOS (Abstractos)
│  └─ project/ports/source_port.py
│     • Interfaz SourcePort (abstracta)
│     • Métodos: source_name, fetch_records()
│
├─ CAPA DE INFRAESTRUCTURA (Adapters)
│  └─ project/infrastructure/sources/
│     ├─ google_scholar_adapter.py ← NUEVA
│     ├─ openalex_adapter.py
│     ├─ scopus_adapter.py
│     ├─ wos_adapter.py
│     ├─ cvlac_adapter.py
│     └─ datos_abiertos_adapter.py
│
│     GoogleScholarAdapter:
│     • Implementa SourcePort
│     • SOURCE_NAME = "google_scholar"
│     • fetch_records() → List[Publication]
│     • Convierte StandardRecord → Publication
│
├─ REGISTRY (Plugin System)
│  └─ project/registry/source_registry.py
│     • SourceRegistry.autodiscover() detecta automáticamente
│     • Registra todas las clases que heredan de SourcePort
│     • create(source_name) → SourcePort instance
│     • create_many([...]) → SourcePort instances
│
├─ CASOS DE USO
│  └─ project/application/
│     ├─ ingest_pipeline.py
│     │  • Orquesta: collect, deduplicate, normalize, match, enrich
│     │  • Ejecuta adapters en paralelo
│     │
│     └─ Servicios de dominio:
│        ├─ DeduplicationService
│        ├─ NormalizationService
│        └─ MatchingService
│
└─ CONFIGURACIÓN (Container DI)
   └─ project/config/container.py
      • build_source_registry() → autodiscover
      • build_pipeline(source_names) → IngestPipeline
      • build_repository() → PostgresRepository

═══════════════════════════════════════════════════════════════════════════════
FLUJO DE DATOS
═══════════════════════════════════════════════════════════════════════════════

Entrada:
  scholar_ids = ["V94aovUAAAAJ"]
  year_from = 2020
  max_results = 10

┌─────────────────────────────┐
│  GoogleScholarExtractor     │
│  extractors/google_scholar/ │
│                             │
│  extract() → StandardRecord │
└────────────┬────────────────┘
             ↓
┌─────────────────────────────────────┐
│  GoogleScholarAdapter               │
│  project/infrastructure/sources/    │
│                                     │
│  fetch_records() → Publication      │
│  (StandardRecord → Publication)     │
└────────────┬────────────────────────┘
             ↓
┌──────────────────────────────────────────┐
│  IngestPipeline.collect()                │
│                                          │
│  Itera sources, ejecuta fetch_records()  │
│  Resultado: Dict[source_name, [Pub]]    │
└────────────┬─────────────────────────────┘
             ↓
┌──────────────────────────────────────────┐
│  Pipeline Transformations                │
│                                          │
│  • deduplicate()   → dedup publications  │
│  • normalize()     → titlecase, fix   │
│  • match()         → fuzzy matching      │
│  • enrich()        → add metadata        │
└────────────┬─────────────────────────────┘
             ↓
┌──────────────────────────────────────────┐
│  Repository (Opcional)                   │
│                                          │
│  Si persist=True:                        │
│  • save_authors()                        │
│  • save_source_records()                 │
│  • upsert_canonical_publications()       │
└──────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
USO EN CÓDIGO
═══════════════════════════════════════════════════════════════════════════════

# Opción 1: Pipeline completo
────────────────────────────────
from project.config.container import build_pipeline

pipeline = build_pipeline(["google_scholar"])
result = pipeline.run(
    year_from=2020,
    max_results=10,
    persist=False,
    source_kwargs={
        "google_scholar": {
            "scholar_ids": ["V94aovUAAAAJ"]
        }
    }
)

print(f"Recolectados: {result.collected}")
print(f"Normalizados: {result.normalized}")
print(f"Coincidencias: {result.matched}")


# Opción 2: Adapter directo
────────────────────────────────
from project.infrastructure.sources.google_scholar_adapter import GoogleScholarAdapter

adapter = GoogleScholarAdapter()
publications = adapter.fetch_records(
    year_from=2020,
    max_results=10,
    scholar_ids=["V94aovUAAAAJ"]
)

for pub in publications:
    print(f"{pub.title} ({pub.publication_year})")
    print(f"  Citas: {pub.citation_count}")


# Opción 3: Vía Registry
────────────────────────────────
from project.config.container import build_source_registry

registry = build_source_registry()
print(registry.source_names)  
# → ['cvlac', 'datos_abiertos', 'google_scholar', 'openalex', 'scopus', 'wos']

adapter = registry.create("google_scholar")
publications = adapter.fetch_records(
    scholar_ids=["V94aovUAAAAJ"],
    max_results=5
)

═══════════════════════════════════════════════════════════════════════════════
VENTAJAS DE ESTA ARQUITECTURA
═══════════════════════════════════════════════════════════════════════════════

✅ Desacoplamiento
   • GoogleScholar está en INFRAESTRUCTURA
   • Pipeline en APLICACIÓN (sin dependencias concretas)
   • Puertos en DOMAIN (interfaces puras)

✅ Extensibilidad
   • Agregar nueva fuente = crear un adapter + SOURCE_NAME
   • Registry la detecta sola (sin código de registro manual)
   • Cero cambios en pipeline o servicios de dominio

✅ Testabilidad
   • Mock de CuerposAdapter fácil (solo implementar SourcePort)
   • Pipeline se prueba sin BD (persist=False)
   • Cada servicio de dominio es testeable

✅ Configurabilidad
   • source_kwargs permite parámetros específicos por fuente
   • build_pipeline(["google_scholar", "openalex"]) multifuente
   • Container centraliza inyección de dependencias

═══════════════════════════════════════════════════════════════════════════════
PRÓXIMOS PASOS (Opcionales)
═══════════════════════════════════════════════════════════════════════════════

1. Integrar con FastAPI
   • POST /ingest con source_names=["google_scholar"]
   • Query params: scholar_ids, year_from, year_to

2. Scheduler (Celery/APScheduler)
   • Extracciones periódicas de Google Scholar
   • Reconciliación automática

3. Almacenar perfiles
   • Tabla researcher_profiles (scholar_id, scholar_ids[])
   • Actualizar automáticamente

4. Caché
   • Redis para throttling
   • Evitar rate-limiting de Google Scholar

═══════════════════════════════════════════════════════════════════════════════
ARCHIVOS MODIFICADOS/CREADOS
═══════════════════════════════════════════════════════════════════════════════

MEJORADO:
  ✏️ project/infrastructure/sources/google_scholar_adapter.py
     • Se actualizó para traer TODOS los campos disponibles
     • Manejo robusto de autores e instituciones
     • Logging completo

CREADOS (para testing):
  ✨ test_integration_google_scholar.py
     • 4 pruebas de integración
     • Valida adapter, registry, pipeline, reconciliación

  ✨ export_google_scholar_json.py
     • Exporta datos extraídos a JSON

  ✨ test_google_scholar.py
     • 5 pruebas unitarias del extractor

  ✨ quick_test_scholar_id.py
     • Prueba rápida de Scholar ID válido

═══════════════════════════════════════════════════════════════════════════════
COMANDOS PARA PROBAR
═══════════════════════════════════════════════════════════════════════════════

# Prueba de integración completa
python test_integration_google_scholar.py

# Prueba de extractor standalone
python test_google_scholar.py

# Validar Scholar ID
python quick_test_scholar_id.py

# Exportar a JSON
python export_google_scholar_json.py

# API FastAPI (si está corriendo)
curl http://localhost:8000/ingest?source_names=google_scholar

═══════════════════════════════════════════════════════════════════════════════
"""

def print_summary():
    """Imprime el resumen de la integración"""
    content = __doc__
    print(content)

if __name__ == "__main__":
    print_summary()
