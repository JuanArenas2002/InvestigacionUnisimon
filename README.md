# Convocatoria — Reconciliación Bibliográfica

API y pipeline ETL para extracción, deduplicación y reconciliación de publicaciones científicas desde múltiples fuentes (OpenAlex, Scopus, Web of Science, CvLAC, Datos Abiertos, Google Scholar).

---

## Arquitectura Hexagonal (Ports & Adapters)

El proyecto sigue **Hexagonal Architecture** (también llamada Ports & Adapters o Clean Architecture). La regla central es la **Regla de Dependencia**: las capas internas nunca importan de las capas externas.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Interfaces (HTTP / CLI)                       │
│              project/interfaces/api/   api/                     │
├─────────────────────────────────────────────────────────────────┤
│                  Application (Use Cases)                         │
│         project/application/   project/registry/                │
├─────────────────────────────────────────────────────────────────┤
│                    Domain (Business Logic)                       │
│                      project/domain/                            │
├─────────────────────────────────────────────────────────────────┤
│              Infrastructure (Adapters / DB / APIs)               │
│   project/infrastructure/   sources/   extractors/   db/        │
└─────────────────────────────────────────────────────────────────┘
```

### Capa de Dominio — `project/domain/`

Núcleo del sistema. Sin dependencias externas (no FastAPI, no SQLAlchemy).

| Módulo | Contenido |
| --- | --- |
| `domain/models/` | `Publication`, `Author` — entidades de dominio |
| `domain/ports/` | Puertos abstractos (ABCs): `SourcePort`, `PublicationRepositoryPort`, `AuthorRepositoryPort`, `RepositoryPort` |
| `domain/services/` | Servicios puros: `MatchingService`, `NormalizationService`, `DeduplicationService` |
| `domain/value_objects/` | `DOI` y `ORCID` — objetos de valor inmutables y hasheables |

**Puertos definidos:**

```python
# Adaptador de fuente — implementado por cada extractor
class SourcePort(ABC):
    @property
    def source_name(self) -> str: ...
    def fetch_records(self, **kwargs) -> List[Publication]: ...

# Repositorio de publicaciones
class PublicationRepositoryPort(ABC):
    def save_authors(self, publications): ...
    def save_source_records(self, records_by_source): ...
    def upsert_canonical_publications(self, publications): ...
    def list_publications(self, limit, offset): ...

# Repositorio de autores
class AuthorRepositoryPort(ABC):
    def get_author_by_id(self, author_id): ...
    def get_author_name_options(self, author_id): ...
    # … 6 métodos más
```

### Capa de Aplicación — `project/application/`

Orquesta el dominio. Solo importa de `project/domain/`.

| Módulo | Responsabilidad |
| --- | --- |
| `ingest_pipeline.py` | Pipeline ETL: collect → deduplicate → normalize → match → enrich → persist |
| `author_profile_use_case.py` | Gestión de perfil de autor: nombres, vínculos, ORCID |
| `schemas/publication_schemas.py` | DTOs inmutables para publicaciones (`PublicationSnapshot`, `MergePublicationsCommand`, `AutoMergeFilters`) |
| `schemas/author_schemas.py` | DTOs inmutables para autores (`AuthorSnapshot`, `MergeAuthorsCommand`) |
| `use_cases/publications/merge.py` | Lógica pura de fusión de publicaciones duplicadas |
| `use_cases/authors/merge.py` | Lógica pura de fusión de autores duplicados |
| `use_cases/authors/validate_external_id.py` | Validación cruzada de IDs externos (ORCID, OpenAlex, Scopus, WoS) |

**Pipeline ETL:**

```
collect() → deduplicate() → normalize() → match() → enrich() → persist()
   ↑                                          ↑
SourcePort                           RepositoryPort
```

### Capa de Infraestructura — `project/infrastructure/`

Implementaciones concretas de los puertos.

#### Persistencia — `project/infrastructure/persistence/`

Ubicación canónica de los modelos SQLAlchemy y la sesión de base de datos.  
El directorio `db/` existe como **shim de compatibilidad** — redirige cada import al nuevo camino.

| Archivo | Contenido |
| --- | --- |
| `models_base.py` | `Base` (DeclarativeBase) y `SourceRecordMixin` con ~20 columnas comunes |
| `source_registry.py` | `SourceRegistry` y `SOURCE_REGISTRY` — singleton global |
| `models.py` | Todos los modelos ORM: `CanonicalPublication`, `Author`, `Journal`, etc. |
| `session.py` | `get_engine()`, `get_session()`, `create_all_tables()`, `ensure_constraints()` |
| `postgres_repository.py` | `PostgresRepository` — implementa `RepositoryPort` con PostgreSQL + pg_trgm |

#### Adaptadores de Fuente — `project/infrastructure/sources/`

Cada adaptador implementa `SourcePort` y delega al extractor correspondiente.

| Adaptador | Fuente |
| --- | --- |
| `openalex_adapter.py` | OpenAlex API (pyalex) |
| `scopus_adapter.py` | Scopus API |
| `wos_adapter.py` | Web of Science API |
| `cvlac_adapter.py` | CvLAC (Minciencias) |
| `datos_abiertos_adapter.py` | Datos Abiertos Colombia |
| `google_scholar_adapter.py` | Google Scholar (scholarly) |

### Capa de Interfaces — `project/interfaces/api/`

Controladores HTTP delgados. Solo traducen HTTP ↔ casos de uso.

| Archivo | Endpoints |
| --- | --- |
| `routers/ingest.py` | `POST /ingest` — ejecuta el pipeline ETL completo |
| `routers/publications.py` | `GET /publications` — lista publicaciones canónicas |
| `routers/author_profile.py` | `GET/PATCH /authors/id/{id}/…` — perfil de autor |
| `schemas/authors.py` | Modelos Pydantic de request/response para autores |
| `main.py` | FastAPI app hexagonal (arranque independiente en puerto 8001) |

---

## Estructura de Directorios

```
convocatoria/
│
├── project/                          ← Núcleo de la arquitectura hexagonal
│   ├── domain/
│   │   ├── models/                   ← Entidades (Publication, Author)
│   │   ├── ports/                    ← Puertos abstractos (ABCs)
│   │   ├── services/                 ← Servicios de dominio puros
│   │   └── value_objects/            ← DOI, ORCID (inmutables, hasheables)
│   │
│   ├── application/
│   │   ├── ingest_pipeline.py        ← Pipeline ETL
│   │   ├── author_profile_use_case.py
│   │   ├── schemas/                  ← DTOs (frozen dataclasses)
│   │   └── use_cases/
│   │       ├── publications/merge.py ← Fusión de duplicados
│   │       └── authors/
│   │           ├── merge.py
│   │           └── validate_external_id.py
│   │
│   ├── infrastructure/
│   │   ├── persistence/              ← Canónico (db/ es shim de compatibilidad)
│   │   │   ├── models_base.py
│   │   │   ├── source_registry.py
│   │   │   ├── models.py
│   │   │   ├── session.py
│   │   │   └── postgres_repository.py
│   │   └── sources/                  ← Adaptadores de fuente
│   │
│   ├── interfaces/
│   │   └── api/                      ← Canónico (project/app/ es shim de compatibilidad)
│   │       ├── routers/
│   │       ├── schemas/
│   │       └── main.py
│   │
│   ├── registry/
│   │   └── source_registry.py        ← Auto-descubrimiento de fuentes
│   │
│   └── config/
│       ├── settings.py               ← Configuración (Pydantic Settings)
│       └── container.py              ← Fábrica de dependencias (DI manual)
│
├── api/                              ← API legado completa (funcional)
│   ├── main.py                       ← FastAPI app principal (puerto 8000)
│   ├── routers/                      ← ~15 routers (authors, publications, stats…)
│   ├── schemas/                      ← Esquemas Pydantic
│   ├── services/                     ← Servicios de análisis, Excel, PDF
│   └── exporters/                    ← Exportadores Excel
│
├── sources/                          ← Modelos SQLAlchemy por fuente
│   ├── openalex.py
│   ├── scopus.py
│   ├── wos.py
│   ├── cvlac.py
│   ├── datos_abiertos.py
│   └── google_scholar.py
│
├── extractors/                       ← Conectores a APIs externas
│   ├── openalex/
│   ├── scopus.py
│   ├── wos.py
│   ├── cvlac.py
│   └── google_scholar/
│
├── reconciliation/                   ← Motor de reconciliación
│   ├── engine.py
│   └── fuzzy_matcher.py
│
├── db/                               ← Shims de compatibilidad → project/infrastructure/persistence/
│   ├── models_base.py
│   ├── source_registry.py
│   ├── models.py
│   └── session.py
│
├── shared/                           ← Normalizadores compartidos
├── scripts/                          ← Scripts de mantenimiento
├── tests/                            ← Suite de tests (pytest)
├── docs/                             ← Documentación técnica adicional
├── config.py                         ← Configuración de acceso a BD y APIs
└── requirements.txt
```

---

## Regla de Dependencia

```
domain  ←  application  ←  infrastructure
   ↑             ↑               ↑
   └─────────────┴───────────────┘
        interfaces solo importa de application
```

- `project/domain/` no importa nada del proyecto
- `project/application/` solo importa de `project/domain/`
- `project/infrastructure/` implementa los puertos de `project/domain/`
- `project/interfaces/api/` llama a use cases de `project/application/`

---

## Requisitos

- Python 3.11+
- PostgreSQL 14+ con extensión `pg_trgm`
- Credenciales API: Scopus (`SCOPUS_API_KEY`), OpenAlex (pública), WoS (opcional)

---

## Instalación

```bash
git clone <repo>
cd convocatoria

python -m venv venv
# Windows
.\venv\Scripts\Activate.ps1
# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

---

## Configuración

Crea `.env` en la raíz:

```env
# Base de datos
DATABASE_URL=postgresql://usuario:password@localhost:5432/convocatoria
# o individualmente:
DB_HOST=localhost
DB_PORT=5432
DB_DATABASE=convocatoria
DB_USER=postgres
DB_PASSWORD=secret

# APIs externas
SCOPUS_API_KEY=tu_clave_scopus
WOS_API_KEY=tu_clave_wos           # opcional

# Entorno
APP_ENV=development                 # production activa CORS estricto
ALLOWED_ORIGINS=https://tu-dominio.com
```

---

## Ejecución

### API completa (legado + hexagonal)

```bash
# Incluye todos los endpoints: /api/authors, /api/publications, /api/pipeline, etc.
# + endpoints hexagonales montados en /api/hex/
uvicorn api.main:app --reload --port 8000
```

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### API hexagonal independiente

```bash
# Solo los endpoints del núcleo hexagonal
uvicorn project.interfaces.api.main:app --reload --port 8001
```

- `POST /ingest` — ejecutar pipeline ETL
- `GET /publications` — listar publicaciones canónicas
- `GET /authors/id/{id}/name-options` — perfil de autor

---

## Endpoints Principales

### Pipeline ETL

```http
POST /ingest
Content-Type: application/json

{
  "sources": ["openalex", "scopus"],   // null = todas las fuentes
  "year_from": 2020,
  "year_to": 2024,
  "max_results": 200,
  "source_kwargs": {
    "cvlac": {"cvlac_codes": ["0001234567"]}
  },
  "dry_run": false
}
```

Respuesta:
```json
{
  "status": "ok",
  "stages": {
    "collect": 150,
    "deduplicate": 142,
    "normalize": 142,
    "match": 138,
    "enrich": 138
  },
  "persistence": {
    "authors_saved": 87,
    "source_saved": 142,
    "canonical_upserted": 138,
    "dry_run": false
  },
  "by_source": { "openalex": 90, "scopus": 60 },
  "errors": {}
}
```

### Autores (API legado)

```
GET  /api/authors                         — listar autores
GET  /api/authors/{id}                    — detalle
POST /api/authors/merge                   — fusionar duplicados
GET  /api/authors/id/{id}/name-options    — nombres desde fuentes
PATCH /api/authors/id/{id}/source-link    — vincular perfil externo
PATCH /api/authors/id/{id}/orcid          — actualizar ORCID
```

### Publicaciones (API legado)

```
GET  /api/publications                    — listar con filtros
GET  /api/publications/{id}               — detalle
POST /api/publications/merge              — fusionar duplicados
POST /api/publications/auto-merge         — auto-fusión por similitud
GET  /api/publications/duplicates         — pares de posibles duplicados
```

### Stats y análisis

```
GET /api/stats/overview                   — resumen general
GET /api/charts/author/{id}               — gráficos de autor
GET /api/authors/{id}/excel               — exportar a Excel
```

---

## Agregar una Nueva Fuente

Para integrar una nueva fuente de datos (ej. PubMed) sin modificar ningún archivo existente:

**1. Modelo SQLAlchemy** — `sources/pubmed.py`:

```python
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String
from project.infrastructure.persistence.models_base import Base, SourceRecordMixin
from project.infrastructure.persistence.source_registry import SOURCE_REGISTRY, SourceDefinition

class PubmedRecord(SourceRecordMixin, Base):
    __tablename__ = "pubmed_records"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pubmed_id: Mapped[str] = mapped_column(String(50), nullable=True, index=True)

    @property
    def source_name(self) -> str:
        return "pubmed"

    @property
    def source_id(self):
        return self.pubmed_id

def _build_kwargs(record, raw, kwargs):
    kwargs["pubmed_id"] = raw.get("pmid")

SOURCE_REGISTRY.register(SourceDefinition(
    name="pubmed",
    model_class=PubmedRecord,
    id_attr="pubmed_id",
    author_id_key="pubmed",
    build_specific_kwargs=_build_kwargs,
))
```

**2. Adaptador hexagonal** — `project/infrastructure/sources/pubmed_adapter.py`:

```python
from project.domain.ports.source_port import SourcePort
from project.domain.models.publication import Publication

class PubMedAdapter(SourcePort):
    SOURCE_NAME = "pubmed"

    @property
    def source_name(self) -> str:
        return self.SOURCE_NAME

    def fetch_records(self, **kwargs) -> list[Publication]:
        # Implementar llamada a la API de PubMed
        ...
```

**3. Migración SQL**:

```sql
CREATE TABLE pubmed_records (
    id SERIAL PRIMARY KEY,
    pubmed_id VARCHAR(50),
    -- columnas del SourceRecordMixin se agregan vía ensure_constraints()
    ...
);
```

El registry, el motor de reconciliación y los endpoints lo detectan automáticamente.

---

## Objetos de Valor de Dominio

### `DOI`

```python
from project.domain.value_objects.doi import DOI

doi = DOI.parse("https://doi.org/10.1038/nature12345")
# doi.value == "10.1038/nature12345"
# doi == DOI.parse("DOI:10.1038/nature12345")  → True (normalización)
```

- Normaliza prefijos (`https://doi.org/`, `doi:`, `DOI:`)
- Valida patrón `10.xxxx/...`
- Hasheable → usable en sets y dicts

### `ORCID`

```python
from project.domain.value_objects.orcid import ORCID

orcid = ORCID.parse("https://orcid.org/0000-0001-2345-6789")
# orcid.value == "0000-0001-2345-6789"
ORCID.validate("0000-0001-2345-6789")  # → True
```

---

## Tests

```bash
# Suite completa
python -m pytest tests/ -v

# Solo tests de arquitectura hexagonal
python -m pytest tests/project/ -v

# Con cobertura
python -m pytest tests/project/ --cov=project --cov-report=term-missing
```

### Estado actual

```
tests/project/   → 71 passing  (arquitectura hexagonal)
tests/           → suite legado (requires BD activa para algunos tests)
```

Los 12 fallos conocidos en `test_pipeline.py` y `test_author_matching.py` son pre-existentes (`ReconciliationConfig.min_title_word_overlap`) y no están relacionados con la migración hexagonal.

---

## Compatibilidad hacia Atrás

Durante la migración, los módulos originales se convirtieron en **shims de 1-3 líneas** que re-exportan desde la ubicación canónica. El código existente sigue funcionando sin cambios:

| Import antiguo | Ubicación canónica |
| --- | --- |
| `from db.models import CanonicalPublication` | `project.infrastructure.persistence.models` |
| `from db.session import get_session` | `project.infrastructure.persistence.session` |
| `from db.source_registry import SOURCE_REGISTRY` | `project.infrastructure.persistence.source_registry` |
| `from project.ports.source_port import SourcePort` | `project.domain.ports.source_port` |
| `from project.ports.repository_port import RepositoryPort` | `project.domain.ports.repository_port` |
| `from project.app.main import app` | `project.interfaces.api.main` |

---

## Scripts de Mantenimiento

```bash
# Reportes de calidad de datos
python scripts/quality_reports.py

# Rellenar campos field_provenance faltantes
python scripts/backfill_provenance.py

# Limpieza de registros huérfanos
python scripts/clean_data.py
```

---

## Documentación Técnica

| Documento | Contenido |
| --- | --- |
| `docs/DATA_DICTIONARY.md` | Diccionario de datos: modelos ORM, esquemas Pydantic |
| `docs/ENDPOINTS_ALL.md` | Todos los endpoints con ejemplos |
| `docs/CRITERIA.md` | Criterios de negocio para reconciliación |
| `docs/OPTIMIZATIONS.md` | Técnicas de procesamiento (chunking, fuzzy matching) |

---

## Stack Tecnológico

| Componente | Tecnología |
| --- | --- |
| Framework web | FastAPI 0.132 + Uvicorn |
| ORM | SQLAlchemy 2.x (mapped columns, DeclarativeBase) |
| Base de datos | PostgreSQL 14+ con `pg_trgm` (fuzzy matching) |
| Validación | Pydantic v2 |
| Autenticación | JWT (python-jose) + bcrypt |
| Fuzzy matching | RapidFuzz + Levenshtein |
| Exportación | openpyxl, reportlab, plotly, matplotlib |
| Scraping | scholarly (Google Scholar), selenium |
| Tests | pytest |
