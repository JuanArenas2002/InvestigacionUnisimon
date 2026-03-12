# Reconciliación Bibliográfica Institucional — Documentación Técnica Completa

> Última actualización: 11 de marzo de 2026

---

## Tabla de contenidos

1. [Visión general](#1-visión-general)
2. [Stack tecnológico](#2-stack-tecnológico)
3. [Estructura del proyecto](#3-estructura-del-proyecto)
4. [Configuración y variables de entorno](#4-configuración-y-variables-de-entorno)
5. [Base de datos](#5-base-de-datos)
6. [Extractores](#6-extractores)
7. [Motor de reconciliación](#7-motor-de-reconciliación)
8. [API REST — Endpoints completos](#8-api-rest--endpoints-completos)
9. [Exportadores Excel](#9-exportadores-excel)
10. [Módulo compartido (shared)](#10-módulo-compartido-shared)
11. [Scripts de administración](#11-scripts-de-administración)
12. [Flujo de datos completo](#12-flujo-de-datos-completo)
13. [Cómo ejecutar el proyecto](#13-cómo-ejecutar-el-proyecto)
14. [Decisiones de arquitectura (DDD)](#14-decisiones-de-arquitectura-ddd)

---

## 1. Visión general

Sistema de **reconciliación bibliográfica institucional** que permite:

- **Extraer** publicaciones científicas de múltiples fuentes externas (OpenAlex, Scopus, Web of Science, CvLAC, Datos Abiertos Colombia).
- **Reconciliar** los registros, deduplicando y unificando cada publicación en un registro canónico único.
- **Verificar cobertura Scopus** de listados de publicaciones cargados como Excel.
- **Enriquecer** publicaciones con metadatos de OpenAlex (revistas, ISSN, acceso abierto, citas, autores).
- **Exportar** reportes en Excel con colores, múltiples hojas y fórmulas COUNTIF.
- **Consultar** el inventario bibliográfico vía API REST con filtros y estadísticas.

El sistema sigue arquitectura **DDD (Domain-Driven Design)**, con la lógica de dominio separada en capas: extractores, reconciliación, persistencia y API.

---

## 2. Stack tecnológico

| Componente | Tecnología | Versión |
|---|---|---|
| Framework web | FastAPI | 0.132.0 |
| Servidor ASGI | Uvicorn | 0.41.0 |
| ORM | SQLAlchemy | 2.0.46 |
| Base de datos | PostgreSQL | — |
| Driver PostgreSQL | psycopg2-binary | 2.9.11 |
| Validación de esquemas | Pydantic | 2.12.5 |
| API OpenAlex | PyAlex | 0.21 |
| Excel | openpyxl | 3.1.5 |
| Fuzzy matching | RapidFuzz | 3.14.3 |
| Normalización texto | Unidecode | 1.4.0 |
| Scraping HTML | BeautifulSoup4 | 4.14.3 |
| HTTP client | Requests | 2.32.5 |
| Variables de entorno | python-dotenv | 1.2.1 |
| Migraciones BD | Alembic | 1.18.4 |
| Data analysis | Pandas / NumPy | 2.3.3 / 2.4.2 |
| Visualización (opcional) | Streamlit / Plotly | 1.54.0 / 6.5.2 |
| Python | CPython | ≥ 3.11 |

---

## 3. Estructura del proyecto

```
CONVOCATORIA/
│
├── config.py                       ← Configuración centralizada (dataclasses)
├── requirements.txt
│
├── shared/                         ← Utilidades transversales
│   ├── __init__.py
│   └── normalizers.py              ← normalize_doi, normalize_title, normalize_author_name…
│
├── db/                             ← Capa de persistencia
│   ├── __init__.py
│   ├── models.py                   ← Modelos SQLAlchemy (11 tablas)
│   ├── session.py                  ← Engine, SessionLocal, create_all_tables()
│   ├── migration_v2.sql
│   ├── migration_v3_field_provenance.sql
│   ├── migration_v4_author_provenance.sql
│   └── truncate_all.sql
│
├── extractors/                     ← Un extractor por fuente
│   ├── __init__.py
│   ├── base.py                     ← StandardRecord + BaseExtractor ABC
│   ├── openalex/                   ← Paquete OpenAlex
│   │   ├── __init__.py             ← Re-exports públicos
│   │   ├── _rate_limit.py          ← OpenAlexAPIError, OpenAlexRateLimitError
│   │   ├── extractor.py            ← OpenAlexExtractor (extracción por ROR)
│   │   └── enricher.py             ← OpenAlexEnricher (enriquecimiento Excel)
│   ├── scopus.py                   ← ScopusExtractor (Elsevier API)
│   ├── cvlac.py                    ← CvLACExtractor (scraping Minciencias)
│   ├── datos_abiertos.py           ← DatosAbiertosExtractor (SODA API)
│   ├── serial_title.py             ← SerialTitleExtractor (Scopus serial-title API)
│   └── wos.py                      ← WosExtractor (Clarivate API)
│
├── reconciliation/                 ← Motor de reconciliación
│   ├── __init__.py
│   ├── engine.py                   ← ReconciliationEngine (DOI exact → fuzzy → nuevo)
│   └── fuzzy_matcher.py            ← compare_records(), FuzzyMatchResult
│
├── api/                            ← Capa de presentación (FastAPI)
│   ├── __init__.py
│   ├── main.py                     ← App FastAPI, lifespan, CORS, registro routers
│   ├── dependencies.py             ← Inyección de dependencias (get_db, auth…)
│   ├── utils.py                    ← Helpers de respuesta HTTP
│   │
│   ├── schemas/                    ← Modelos Pydantic de entrada/salida
│   │   ├── authors.py
│   │   ├── common.py
│   │   ├── external_records.py
│   │   ├── publications.py
│   │   ├── scopus.py
│   │   ├── serial_title.py
│   │   └── stats.py
│   │
│   ├── routers/                    ← Un router por dominio
│   │   ├── publications.py         ← CRUD publicaciones canónicas
│   │   ├── authors.py              ← Gestión de autores
│   │   ├── external_records.py     ← Registros externos y log de reconciliación
│   │   ├── scopus.py               ← Dashboard Scopus
│   │   ├── stats.py                ← Estadísticas y métricas del sistema
│   │   ├── search.py               ← Búsqueda en vivo (OpenAlex) + enriquecimiento
│   │   ├── catalogs.py             ← Journals e instituciones normalizadas
│   │   ├── _pipeline_helpers.py    ← Helpers privados compartidos entre pipeline/*
│   │   │
│   │   └── pipeline/               ← Paquete pipeline (ingesta y reconciliación)
│   │       ├── __init__.py         ← Router principal prefix="/pipeline"
│   │       ├── _ids.py             ← Resolución de EIDs, build_pub_entry
│   │       ├── _json_loader.py     ← Detección y parseo de JSON multi-fuente
│   │       ├── extraction.py       ← Extracción desde OpenAlex/Scopus/JSON
│   │       ├── coverage.py         ← Verificación de cobertura Scopus
│   │       ├── reconciliation_ops.py ← Operaciones de reconciliación
│   │       └── admin.py            ← Administración (truncate, init-db, test)
│   │
│   └── exporters/                  ← Generadores de archivos Excel
│       ├── __init__.py
│       └── excel/                  ← Paquete Excel
│           ├── __init__.py         ← Re-exports públicos
│           ├── _styles.py          ← Constantes de color, bordes, caches de estilos
│           ├── reader.py           ← Lectura de Excel (ISSNs, publicaciones)
│           ├── journal_coverage.py ← Excel de cobertura por revista
│           └── publications_coverage.py ← Excel de cobertura por publicación (6 hojas)
│
├── scripts/                        ← Scripts de mantenimiento one-off
│   ├── backfill_provenance.py
│   ├── migrate_author_provenance.py
│   ├── migrate_field_provenance.py
│   └── truncate_all.py
│
├── OpenAlexJson/                   ← JSONs de extracciones anteriores (cache local)
│   └── openalex_publications_*.json
│
└── docs/                           ← Documentación
    ├── PROYECTO.md                 ← Este archivo
    ├── ENDPOINTS_ALL.md
    ├── ENDPOINTS_PIPELINE.md
    └── OPENALEX_PIPELINE.md
```

---

## 4. Configuración y variables de entorno

Todas las variables se definen en un archivo **`.env`** en la raíz del proyecto y se cargan con `python-dotenv`.

### Archivo `.env` de referencia

```env
# ── Base de datos ──────────────────────────────────────────
DB_HOST=localhost
DB_PORT=5432
DB_NAME=reconciliacion_bibliografica
DB_USER=postgres
DB_PASSWORD=tu_password

# ── Institución ────────────────────────────────────────────
ROR_ID=https://ror.org/02njbw696
INSTITUTION_NAME=Universidad Ejemplo
CONTACT_EMAIL=biblioteca@universidad.edu
SCOPUS_AFFILIATION_ID=60XXXXXXX

# ── APIs externas ──────────────────────────────────────────
OA_KEY=                        # API key OpenAlex (opcional pero recomendado)
SCOPUS_API_KEY=                # API key Elsevier/Scopus
SCOPUS_INST_TOKEN=             # Token institucional Scopus (mejora cuota)
WOS_API_KEY=                   # API key Clarivate Web of Science
DATOS_ABIERTOS_TOKEN=          # App token datos.gov.co (opcional)
```

### Configuración centralizada (`config.py`)

| Clase | Propósito |
|---|---|
| `DatabaseConfig` | Host, puerto, nombre de BD, usuario, contraseña → `.url` (SQLAlchemy URL) |
| `InstitutionConfig` | ROR ID, nombre, email, Scopus affiliation ID |
| `OpenAlexConfig` | `base_url`, `api_key`, `timeout`, `max_per_page`, `rate_limit_delay` |
| `ScopusConfig` | `base_url`, `api_key`, `inst_token`, `timeout`, `max_per_page` |
| `WosConfig` | `base_url`, `api_key`, `timeout`, `max_per_page` |
| `CvlacConfig` | URLs de CvLAC/GrupLAC, timeout, delay entre peticiones |
| `DatosAbiertosConfig` | `base_url`, `app_token`, `max_per_page` (hasta 50k con SODA) |
| `ReconciliationConfig` | Umbrales fuzzy, pesos de scoring, max_candidates |

**Instancias globales** (importables directamente):
```python
from config import (
    db_config, institution, openalex_config, scopus_config,
    wos_config, cvlac_config, datos_abiertos_config,
    reconciliation_config
)
```

---

## 5. Base de datos

### Diagrama de tablas

```
journals ─────────────────────────────────────────────────────────────────────┐
institutions ────────────────────────────────────────────────────────────────┐ │
                                                                              │ │
authors ──────────────────────────────────────────────────────┐              │ │
  author_institutions (N:M) ←→ institutions                  │              │ │
                                                              │              │ │
canonical_publications ←──────── journal_id ─────────────────┼──────────────┘ │
  │                                                           │                │
  ├── publication_authors (N:M) ←→ authors ──────────────────┘                │
  │                                                                            │
  ├── openalex_records                                                         │
  ├── scopus_records                                                           │
  ├── wos_records                                                              │
  ├── cvlac_records                                                            │
  └── datos_abiertos_records                                                   │
                                                                               │
reconciliation_log                                                             │
```

### Tablas principales

#### `canonical_publications`
Registro "dorado" unificado. Una fila = un producto bibliográfico único.

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | PK int | Auto-incremental |
| `doi` | varchar(255) UNIQUE | DOI normalizado |
| `title` | text NOT NULL | Título original |
| `normalized_title` | text | Título sin acentos/mayúsculas (para matching) |
| `publication_year` | int | Año |
| `publication_type` | varchar(100) | article, conference paper, etc. |
| `is_open_access` | bool | |
| `oa_status` | varchar(50) | gold, green, bronze, closed |
| `citation_count` | int | Citas (fuente más reciente) |
| `field_provenance` | JSONB | `{"doi": "openalex", "source_journal": "scopus"}` |
| `sources_count` | int | Número de fuentes en que aparece |
| `created_at / updated_at` | timestamptz | |

#### Tablas de fuente (`openalex_records`, `scopus_records`, etc.)
Cada tabla conserva los datos crudos de la fuente y los campos de vinculación:

| Columna común | Descripción |
|---|---|
| `canonical_publication_id` | FK → canonical_publications |
| `status` | `pending` / `reconciled` / `unmatched` / `manual_review` |
| `match_score` | Score del fuzzy matching (0.0–1.0) |
| `match_type` | `doi_exact` / `fuzzy` / `manual` / `unmatched` |
| `reconciled_at` | Fecha de reconciliación |

#### `reconciliation_log`
Auditoría de cada decisión del motor: source, record_id, canonical_id, match_type, score, detalle JSON.

#### `authors`
Autores con identificadores multi-fuente: ORCID, OpenAlex ID, Scopus ID, WoS ID, CvLAC ID.

### Scripts SQL de migración

| Archivo | Propósito |
|---|---|
| `migration_v2.sql` | Estructura base v2 |
| `migration_v3_field_provenance.sql` | Añade columna `field_provenance` JSONB |
| `migration_v4_author_provenance.sql` | Añade provenance en tabla de autores |
| `truncate_all.sql` | Trunca todas las tablas (mantiene esquema) |

---

## 6. Extractores

Todos los extractores heredan de `BaseExtractor` (`extractors/base.py`) y producen objetos `StandardRecord`.

### `extractors/base.py`

- **`StandardRecord`** — dataclass con todos los campos normalizados que se persisten en las tablas por fuente.
- **`BaseExtractor`** — clase abstracta con métodos: `extract()`, `save_to_db()`, `save_to_json()`.
- Funciones de normalización: `normalize_doi()`, `normalize_text()`, `normalize_author_name()`.

### `extractors/openalex/` — Paquete OpenAlex

#### `_rate_limit.py`
- `OpenAlexAPIError` — excepción base.
- `OpenAlexRateLimitError(retry_after)` — lanzada al detectar 429.
- `extract_retry_after(exc)` — extrae el tiempo de espera de la respuesta 429 (headers, JSON body, fallback).

#### `extractor.py` — `OpenAlexExtractor`

Extracción masiva de publicaciones institucionales por ROR ID.

```python
extractor = OpenAlexExtractor()
records = extractor.extract(
    ror_id="https://ror.org/02njbw696",
    year_from=2015,
    year_to=2024,
)
extractor.save_to_json(records, path="OpenAlexJson/")
```

Métodos:
- `extract(ror_id, year_from, year_to)` — paginación por cursor, manejo de rate limit, timeout 30s.
- `search_by_doi(doi)` — búsqueda exacta por DOI.
- `_parse_record(work_dict)` → `StandardRecord`.
- `save_to_json(records, path)` — guarda JSON con timestamp en nombre.

#### `enricher.py` — `OpenAlexEnricher`

Enriquecimiento de listados Excel con datos de OpenAlex. Pipeline de 3 etapas:

1. **Por DOI en lote** — hasta 50 DOIs por request (filtro OR).
2. **Por título** — búsqueda full-text + filtro de año, fuzzy matching (token_sort_ratio).
3. **Por título sin año** — fallback si el año no está disponible.

```python
enricher = OpenAlexEnricher()
rows = enricher.enrich_from_excel_bytes(file_bytes)
# o directamente:
rows = enricher.enrich(publications_list)
```

Métodos:
- `enrich_from_excel(path)`, `enrich_from_excel_bytes(bytes)` — lectura + enrich.
- `enrich(rows)` → `list[dict]` con campos `oa_*`.
- `_enrich_by_doi_batch(rows)` — lotes de 50 DOIs.
- `_enrich_by_title(row)` — búsqueda por título con `per_page=10`, normalización de diacríticos.
- `_best_match(candidates, row)` — score compuesto: título×0.85 + año×0.15.
- `save_to_excel(rows, path)` — Excel de resultados.

**Configuración interna:**
```python
pyalex.config.retry_http_codes = [500, 502, 503, 504]  # 429 excluido: manejo manual
socket.setdefaulttimeout(30)
MIN_SCORE = 80.0  # umbral fuzzy mínimo (configurable)
```

### `extractors/scopus.py` — `ScopusExtractor`

Extracción vía Elsevier Scopus API (Search API + Abstract Retrieval).

- Paginación automática (máx. 25 por página).
- Soporte de `inst_token` para mayor cuota.
- Mapeo de campos Scopus → `StandardRecord`.

### `extractors/serial_title.py` — `SerialTitleExtractor`

Consulta la Scopus Serial Title API para metadatos de revistas: estado (active/discontinued), periodos de cobertura, áreas temáticas, editorial.

Usado internamente por los endpoints de verificación de cobertura.

### `extractors/cvlac.py` — `CvLACExtractor`

Scraping de perfiles CvLAC y GrupLAC de Minciencias (Colombia).
- Parser HTML con BeautifulSoup4.
- Delay configurable entre peticiones.
- Extrae productos bibliográficos con año, tipo, autores, ISSN.

### `extractors/datos_abiertos.py` — `DatosAbiertosExtractor`

Consulta la API SODA de datos.gov.co.
- Soporte de app_token.
- Paginación hasta 50,000 registros por petición.

### `extractors/wos.py` — `WosExtractor`

Extracción vía Clarivate Web of Science Starter API.
- Autenticación por API key en headers.
- Paginación, manejo de errores 400/404.

---

## 7. Motor de reconciliación

### `reconciliation/fuzzy_matcher.py`

- **`FuzzyMatchResult`** — dataclass con `score_title`, `score_year`, `score_authors`, `combined_score`, `is_match`.
- **`compare_records(r1, r2, config)`** — compara dos `StandardRecord` con pesos configurables.
  - Título: `token_sort_ratio` (RapidFuzz) normalizado con Unidecode.
  - Año: score binario (1.0 si coincide, 0.0 si difiere en >1).
  - Autores: coincidencia de apellidos tokenizados.

### `reconciliation/engine.py` — `ReconciliationEngine`

Flujo de reconciliación en cascada:

```
Por cada registro fuente con status='pending':

  1. ¿Tiene DOI normalizado?
     SÍ → buscar en canonical_publications.doi
       ✓ Match → vincular (match_type=doi_exact)
       ✗ → buscar el mismo DOI en otras tablas ya reconciliadas
         ✓ → vincular al mismo canónico
         ✗ → ir al paso 3

  2. Sin DOI → fuzzy matching
     Candidatos: últimos N canónicos por año (configurable)
     Score combinado ≥ threshold → vincular (match_type=fuzzy)
     Score ≥ manual_threshold → marcar manual_review
     Score < manual_threshold → ir al paso 3

  3. Crear nueva canonical_publication
     Insertar registro canónico → vincular
```

Métodos principales:
- `reconcile_source(source_name)` — reconcilia todos los pendientes de una fuente.
- `reconcile_all()` — itera las 5 fuentes.
- `_reconcile_record(record, session)` → decision.
- `_create_canonical(record, session)` → `CanonicalPublication`.

---

## 8. API REST — Endpoints completos

**Base URL:** `http://localhost:8000`  
**Documentación interactiva:** `http://localhost:8000/docs`

Todos los endpoints están bajo el prefijo `/api`.

---

### `GET /` — Raíz

Información básica del servicio. No requiere autenticación.

---

### Publicaciones — `/api/publications`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/publications/exists` | Verificar si una publicación existe (por DOI) |
| `GET` | `/publications/by-year` | Distribución de publicaciones por año |
| `GET` | `/publications/field-coverage` | Cobertura de campos (DOI, ORCID, etc.) |
| `GET` | `/publications/types` | Distribución por tipo de publicación |
| `GET` | `/publications/duplicates` | Detectar posibles duplicados |
| `GET` | `/publications/{pub_id}` | Detalle de una publicación canónica |
| `GET` | `/publications/{pub_id}/authors` | Autores de una publicación |

---

### Autores — `/api/authors`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/authors/duplicates-orcid` | Autores con ORCID duplicado |
| `POST` | `/authors/enrich-missing-orcid` | Enriquecer autores sin ORCID vía OpenAlex |
| `GET` | `/authors/stats` | Estadísticas de autores |
| `GET` | `/authors/ids-coverage` | Cobertura de identificadores (ORCID, Scopus ID…) |
| `GET` | `/authors/without-orcid` | Autores sin ORCID |
| `GET` | `/authors/duplicates` | Posibles autores duplicados por nombre |
| `POST` | `/authors/merge` | Fusionar dos autores en uno |
| `DELETE` | `/authors/{author_id}` | Eliminar un autor |
| `GET` | `/authors/inventory` | Inventario completo de autores |
| `GET` | `/authors/{author_id}` | Detalle de un autor |
| `GET` | `/authors/{author_id}/publications` | Publicaciones de un autor |
| `GET` | `/authors/{author_id}/coauthors` | Coautores de un autor |
| `POST` | `/authors/enrich-orcid` | Enriquecer ORCID de un autor específico |

---

### Registros Externos — `/api/external-records`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/external-records/by-source-status` | Conteos por fuente y estado de reconciliación |
| `GET` | `/external-records/match-types` | Distribución de tipos de match |
| `GET` | `/external-records/manual-review` | Registros pendientes de revisión manual |
| `GET` | `/external-records/reconciliation-log` | Log de auditoría del motor |
| `GET` | `/external-records/{source}/{record_id}` | Detalle de un registro de fuente |
| `PATCH` | `/external-records/{source}/{record_id}/resolve` | Resolver manualmente un registro |

---

### Scopus — `/api/scopus`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/scopus/insights` | KPIs generales del dashboard Scopus |
| `GET` | `/scopus/records` | Listado filtrable de registros Scopus |
| `GET` | `/scopus/records/{record_id}` | Detalle de un registro Scopus |
| `GET` | `/scopus/records/by-eid/{eid}` | Registro por EID |
| `GET` | `/scopus/not-found` | Publicaciones no encontradas en Scopus |
| `GET` | `/scopus/enriched-fields` | Registros con campos enriquecidos |
| `GET` | `/scopus/authors` | Autores en registros Scopus |

---

### Estadísticas — `/api/stats`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/stats/health` | Health check del sistema (BD, memoria, etc.) |
| `GET` | `/stats/system` | Métricas del sistema (tablas, registros, sesiones) |
| `GET` | `/stats/overview` | KPIs globales del inventario bibliográfico |
| `GET` | `/stats/reconciliation-timeline` | Evolución temporal de la reconciliación |
| `GET` | `/stats/year-source-matrix` | Matriz año × fuente de publicaciones |
| `GET` | `/stats/quality` | Métricas de calidad de datos |
| `GET` | `/stats/quality/{category}` | Calidad por categoría específica |
| `GET` | `/stats/json-files` | Listado de archivos JSON disponibles |

---

### Búsqueda — `/api/search`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/search/openalex` | Búsqueda en vivo en la API de OpenAlex |
| `POST` | `/search/enrich-excel` | Enriquecer Excel con datos de OpenAlex |

#### `GET /api/search/openalex`

Parámetros:

| Parámetro | Tipo | Descripción |
|---|---|---|
| `query` | string | Texto libre de búsqueda |
| `doi` | string | DOI exacto |
| `title` | string | Palabras del título |
| `author` | string | Nombre de autor |
| `year_from` | int | Año de publicación desde |
| `year_to` | int | Año de publicación hasta |
| `max_results` | int (1–200) | Máx. resultados (default 25) |

Respuesta: `{ "count": N, "results": [{ "openalex_id", "doi", "title", "publication_year", "is_open_access", "oa_status", "source_journal", "issn", "all_authors", "institutional_authors", ... }] }`

> **Nota:** Requiere créditos en la API de OpenAlex. Si la cuota diaria está agotada, retorna `429` con `retry_after_seconds` y un mensaje del reset. Configurar `OA_KEY` en `.env` para mayor cuota.

#### `POST /api/search/enrich-excel`

Sube un Excel con columnas `Título`, `Año`, `doi` y devuelve un Excel enriquecido con dos hojas:
- **Encontrados** — revista, ISSN-L, editorial, acceso abierto, citas, autores, URL.
- **No encontrados** — publicaciones que OpenAlex no pudo resolver.

Parámetros form:
- `file` — archivo `.xlsx` / `.xls`
- `fuzzy_threshold` — umbral de similitud 50–100 (default 80)

---

### Catálogos — `/api/catalogs`

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/catalogs/journals` | Listado de revistas normalizadas |
| `GET` | `/catalogs/journals/{journal_id}` | Detalle de una revista |
| `POST` | `/catalogs/journals` | Crear nueva revista |
| `GET` | `/catalogs/institutions` | Listado de instituciones |
| `GET` | `/catalogs/institutions/{institution_id}` | Detalle de una institución |
| `POST` | `/catalogs/institutions` | Crear nueva institución |

---

### Pipeline — `/api/pipeline`

#### Extracción (`pipeline/extraction.py`)

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/pipeline/extract/openalex` | Extraer publicaciones de OpenAlex por ROR + rango de años |
| `POST` | `/pipeline/extract/scopus` | Extraer publicaciones de Scopus |
| `POST` | `/pipeline/load-json` | Cargar JSON de extracción previa a la BD |
| `POST` | `/pipeline/search-doi-in-sources` | Buscar DOIs específicos en todas las fuentes |

#### Cobertura Scopus (`pipeline/coverage.py`)

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/pipeline/scopus/journal-coverage` | Cobertura de una revista por ISSN |
| `POST` | `/pipeline/scopus/journal-coverage/bulk` | Cobertura de múltiples revistas (JSON) |
| `POST` | `/pipeline/scopus/journal-coverage/bulk-from-file` | Cobertura desde Excel con ISSNs |
| `GET` | `/pipeline/scopus/journal-coverage/debug` | Debug de cobertura para un ISSN |
| `POST` | `/pipeline/scopus/check-publications-coverage` | **Verificar cobertura de un listado de publicaciones** (Excel upload) |
| `POST` | `/pipeline/scopus/reprocess-coverage` | Reprocesar cobertura de registros existentes |
| `GET` | `/pipeline/scopus/debug/raw` | Respuesta cruda de Scopus Serial Title para un ISSN |
| `GET` | `/pipeline/scopus/by-institution` | Listar DOIs Scopus por institución |
| `POST` | `/pipeline/scopus/by-institution/reconcile` | Reconciliar registros Scopus de una institución |

##### `POST /api/pipeline/scopus/check-publications-coverage` — Detalle

El endpoint más completo del sistema. Proceso:

1. Lee el Excel de publicaciones (columnas Scopus, OpenAlex BD, o mixto).
2. Extrae ISSNs / EIDs de cada fila.
3. Consulta la API Scopus Serial Title para cada revista → estado, periodos de cobertura, áreas.
4. Compara el año de publicación con los periodos de cobertura → `Sí / No / Sin datos`.
5. Para revistas discontinuadas → busca en `openalex_records` local para cruce de datos.
6. Genera Excel de respuesta con 6 hojas: Cobertura, Autores, Datos originales, Descontinuadas, Descont. OpenAlex, Resumen.

Retorna: `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`

#### Reconciliación (`pipeline/reconciliation_ops.py`)

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/pipeline/reconcile` | Reconciliar registros pendientes de una fuente |
| `POST` | `/pipeline/reconcile-all` | Reconciliar todas las fuentes |
| `POST` | `/pipeline/reconcile/all-sources` | Reconciliación completa con informe |
| `POST` | `/pipeline/crossref-scopus` | Cruzar registros Scopus con referencias existentes |

#### Administración (`pipeline/admin.py`)

| Método | Ruta | Descripción |
|---|---|---|
| `DELETE` | `/pipeline/truncate-all` | ⚠ Vaciar todas las tablas de datos |
| `POST` | `/pipeline/init-db` | Crear/verificar tablas en la BD |
| `GET` | `/pipeline/scopus/test-extract` | Test de extracción Scopus (sin guardar) |

---

## 9. Exportadores Excel

### Paquete `api/exporters/excel/`

#### `_styles.py` — Estilos y colores

Constantes de color (formato hex sin `#`):

| Constante | Uso |
|---|---|
| `COLOR_HEADER_BG="1F4E79"` | Fondo de cabeceras de tabla |
| `COLOR_HEADER_FONT="FFFFFF"` | Texto de cabeceras |
| `COLOR_ACTIVE="1E8449"` | Revista activa |
| `COLOR_DISCONT="C0392B"` | Revista discontinuada |
| `COLOR_IN_COV="1E6B2F"` | Publicación en cobertura |
| `COLOR_OUT_COV="922B21"` | Publicación fuera de cobertura |
| `COLOR_NO_DATA="7D6608"` | Sin datos suficientes |
| `COLOR_NOT_FOUND="595959"` | Revista no encontrada en Scopus |
| `COLOR_FALLBACK="F1C40F"` | Resuelta sin ISSN (por título/DOI) |

Funciones con caché de objetos (evita crear miles de instancias openpyxl):
- `_fill(color)` — `PatternFill` cacheado.
- `_align(h, v, wrap)` — `Alignment` cacheado.
- `_font(bold, color, size, italic)` — `Font` cacheado.
- `_write_sheet_header(ws, labels, title)` — escribe fila de título + fila de cabeceras.

#### `reader.py` — Lectura de Excel

- `read_issns_from_excel(file_bytes)` → `list[str]` — extrae todos los ISSNs del Excel.
- `read_publications_from_excel(file_bytes)` → `(headers, rows)` — lee el Excel de publicaciones, detecta y omite filas de encabezado duplicadas.

#### `journal_coverage.py` — Excel de cobertura por revista

- `generate_journal_coverage_excel(results)` → `bytes`
- Columnas: ISSN, Título, Editorial, Estado, Periodos de cobertura, Áreas temáticas, Encontradas en BD, % cobertura, etc.
- Hoja de resumen con conteos automáticos.

#### `publications_coverage.py` — Excel de cobertura por publicación

- `generate_publications_coverage_excel(headers, rows)` → `bytes`
- `get_column_letter_offset(col_letter, offset)` → `str`

**6 hojas generadas:**

| Hoja | Contenido |
|---|---|
| **Cobertura** | Una fila por publicación. Columnas: #, Fuente, En Scopus, ¿En cobertura?, Estado revista, Título, Año, Tipo, Revista, Editorial, Periodos, Áreas, Encontrado vía, DOI, EID, ISSNs resueltos. Leyenda de colores embebida. |
| **Autores** | Una fila por autor: datos de la publicación + N° autor, Nombre (Scopus), Afiliación, Autores (OpenAlex). |
| **Datos originales** | Columnas tal como llegan en el Excel fuente. |
| **Descontinuadas** | Una fila por revista única descontinuada/inactiva detectada. |
| **Descont. OpenAlex** | Publicaciones en revistas discontinuadas cruzadas con `openalex_records` local. |
| **Resumen** | Fórmulas COUNTIF que se actualizan al editar la hoja Cobertura. Métricas por fuente (Scopus Export vs OpenAlex BD). |

---

## 10. Módulo compartido (`shared/`)

### `shared/normalizers.py`

Funciones de normalización reutilizables en toda la aplicación:

| Función | Descripción |
|---|---|
| `normalize_doi(doi)` | Limpia DOI: minúsculas, elimina prefijo `https://doi.org/`, quita espacios |
| `normalize_year(year)` | Convierte a int, retorna None si inválido |
| `normalize_text(text)` | Minúsculas + strip + Unidecode (elimina acentos/diacríticos) |
| `normalize_author_name(name)` | Normaliza nombre de autor: apellido, nombre → lowercase sin acentos |
| `normalize_title_for_search(title)` | Título para búsqueda: lowercase, sin puntuación, sin stopwords triviales |

---

## 11. Scripts de administración

Ubicados en `scripts/`, se ejecutan directamente con Python:

```bash
# Migrar campo provenance a registros existentes
python scripts/backfill_provenance.py

# Migrar provenance de autores
python scripts/migrate_author_provenance.py

# Migrar provenance de campos
python scripts/migrate_field_provenance.py

# Truncar todas las tablas (⚠ irreversible en producción)
python scripts/truncate_all.py
```

---

## 12. Flujo de datos completo

```
                         FUENTES EXTERNAS
          ┌──────────────────────────────────────────┐
          │ OpenAlex  │ Scopus │ WoS │ CvLAC │ D.A. │
          └─────┬─────────┬────────┬──────┬──────┬──┘
                │         │        │      │      │
          EXTRACTORES (extractors/)
          StandardRecord estandarizado
                │
                ▼
          TABLAS DE FUENTE (PostgreSQL)
          openalex_records / scopus_records / …
          status = 'pending'
                │
                ▼
          MOTOR DE RECONCILIACIÓN (reconciliation/)
          1. DOI exact match
          2. Fuzzy matching (título + año + autores)
          3. Crear nuevo canónico
                │
                ▼
          canonical_publications
          status = 'reconciled'
                │
                ▼
          API REST (FastAPI)
          /api/publications, /api/authors, /api/stats…
                │
                ▼
          VERIFICACIÓN DE COBERTURA (pipeline/coverage)
          Excel upload → Scopus Serial Title API
          → Excel de cobertura (6 hojas)
```

---

## 13. Cómo ejecutar el proyecto

### Requisitos previos

- Python 3.11+
- PostgreSQL 14+
- (Opcional) Claves de API: OpenAlex, Scopus, WoS

### Instalación

```powershell
# Clonar el repositorio
cd C:\ruta\al\proyecto

# Crear entorno virtual
python -m venv venv
.\venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
copy .env.example .env
# Editar .env con credenciales
```

### Iniciar la API

```powershell
# Desarrollo (recarga automática)
uvicorn api.main:app --reload --port 8000

# Producción
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 2
```

### Inicializar la base de datos

```bash
# Vía API (crea tablas si no existen)
POST http://localhost:8000/api/pipeline/init-db

# O directamente
python -c "from db.session import create_all_tables; create_all_tables()"
```

### Flujo típico de uso

```bash
# 1. Extraer publicaciones de OpenAlex
POST /api/pipeline/extract/openalex
Body: { "ror_id": "...", "year_from": 2020, "year_to": 2024 }

# 2. Reconciliar
POST /api/pipeline/reconcile-all

# 3. Ver estadísticas
GET /api/stats/overview

# 4. Verificar cobertura Scopus (subir Excel)
POST /api/pipeline/scopus/check-publications-coverage
Body: form-data con el Excel
```

---

## 14. Decisiones de arquitectura (DDD)

### Refactoring realizado (marzo 2026)

El proyecto fue refactorizado de 3 módulos monolíticos (~4,500 líneas totales) a paquetes DDD desacoplados:

| Módulo original | Reemplazado por | Líneas antes → después |
|---|---|---|
| `extractors/openalex.py` | `extractors/openalex/` (4 archivos) | ~1050 → 4×250 |
| `api/routers/pipeline.py` | `api/routers/pipeline/` (7 archivos) | ~1980 → 7×350 |
| `api/exporters/excel.py` | `api/exporters/excel/` (5 archivos) | ~1455 → 5×300 |

### Principios aplicados

1. **Single Responsibility** — cada archivo tiene un único propósito concreto: `_rate_limit.py` solo maneja errores de cuota; `_styles.py` solo define estilos Excel.

2. **Paquetes Python sobre módulos** — Python resuelve `from extractors.openalex import X` desde el directorio `extractors/openalex/__init__.py` sin necesidad de cambiar ningún código existente que ya importaba del módulo plano.

3. **Caches de objetos openpyxl** — Los objetos `PatternFill`, `Alignment`, `Font` se cachean en dicts a nivel de módulo para evitar crear miles de instancias idénticas durante la generación de Excel.

4. **429 nunca en `status_forcelist`** — Las peticiones a APIs con rate limit (OpenAlex, Scopus) nunca incluyen 429 en la lista de reintentos automáticos. El 429 se maneja explícitamente leyendo el header `Retry-After` y retornando un error descriptivo al cliente.

5. **Importaciones lazy dentro de endpoints** — Los imports pesados (pyalex, openpyxl) se hacen dentro del cuerpo del endpoint en lugar de en el módulo para no penalizar el tiempo de arranque.

6. **`field_provenance` JSONB** — Cada campo de `canonical_publications` y `authors` registra de qué fuente proviene su valor actual (ej. `{"doi": "openalex", "source_journal": "scopus"}`), permitiendo auditoría y reenvíos selectivos.

---

*Documentación generada automáticamente el 11/03/2026.*
