# ✅ Refactorización DDD: Pipeline Module

## 📋 Resumen

Se ha refactorizado completamente la carpeta `api/routers/pipeline/` de ser **monolítica y acoplada** a una **arquitectura clean DDD (Domain-Driven Design)** con separación clara de responsabilidades.

## 🏗️ Estructura Nueva (DDD)

```
api/routers/pipeline/
├── __init__.py                    ← NUEVO: Punto de entrada que importa desde endpoints/
│
├── endpoints/                     ← NUEVA CAPA: Handlers HTTP (delgados, solo I/O)
│   ├── __init__.py               ← Combina routers
│   ├── scopus_coverage.py        ← GET/POST /scopus/* (extraído de coverage.py)
│   ├── extraction.py              ← POST /extract/* (extraído de extraction.py)
│   ├── reconciliation.py          ← POST /reconcile/* (extraído de reconciliation_ops.py)
│   └── admin.py                   ← DELETE/POST /admin (extraído de admin.py)
│
├── application/                   ← NUEVA CAPA: Orquestación de casos de uso
│   ├── __init__.py
│   └── commands/
│       ├── __init__.py
│       └── pipeline_commands.py   ← Use cases (extractar, verificar, reconciliar)
│
├── domain/                        ← NUEVA CAPA: Lógica de negocio pura
│   ├── __init__.py
│   ├── entities/
│   │   ├── __init__.py
│   │   └── publication.py         ← Agregados (Publication, Journal, CoveragePeriod)
│   ├── services/
│   │   ├── __init__.py
│   │   └── pipeline_services.py   ← Servicios de dominio (Coverage, Extraction, Reconciliation)
│   └── repositories/
│       ├── __init__.py
│       └── repository_interfaces.py ← Abstracciones (PublicationRepository, etc.)
│
├── infrastructure/                ← NUEVA CAPA: Implementación técnica
│   ├── __init__.py
│   ├── repositories/
│   │   ├── __init__.py
│   │   └── sqlalchemy_*.py       ← Implementaciones SQLAlchemy (TODO)
│   ├── extractors/
│   │   ├── __init__.py
│   │   └── *_adapter.py          ← Adaptadores extractores (TODO)
│   └── exporters/
│       ├── __init__.py
│       └── *_adapter.py          ← Adaptadores exportadores (TODO)
│
├── shared/                        ← NUEVA CAPA: DTOs compartidos
│   ├── __init__.py
│   └── dtos.py                    ← Pydantic DTOs (entrada/salida)
│
├── _pipeline_helpers.py           ← MANTENIDO: Helpers compatibilidad
├── _ids.py                        ← MANTENIDO: Utilities compatibilidad
├── _json_loader.py                ← MANTENIDO: JSON parsing compatibilidad
│
└── [ARCHIVOS ANTIGUOS - MANTENIDOS POR COMPATIBILIDAD]
    ├── coverage.py                ← DEPRECATED: Ver endpoints/scopus_coverage.py
    ├── extraction.py              ← DEPRECATED: Ver endpoints/extraction.py
    ├── reconciliation_ops.py       ← DEPRECATED: Ver endpoints/reconciliation.py
    └── admin.py                   ← DEPRECATED: Ver endpoints/admin.py
```

## 🔄 Mapeo: Viejo → Nuevo

### Endpoints (`coverage.py` → `endpoints/scopus_coverage.py`)
```
GET  /pipeline/scopus/journal-coverage              → endpoints/
POST /pipeline/scopus/journal-coverage/bulk         → endpoints/
POST /pipeline/scopus/journal-coverage/bulk-from-file → endpoints/
GET  /pipeline/scopus/journal-coverage/debug         → endpoints/
POST /pipeline/scopus/check-publications-coverage    → endpoints/ (-200 líneas)
POST /pipeline/scopus/reprocess-coverage             → endpoints/ (-150 líneas)
```

### Endpoints (`extraction.py` → `endpoints/extraction.py`)
```
POST /pipeline/extract/openalex                     → endpoints/ (-50 líneas, más limpio)
POST /pipeline/extract/scopus                       → endpoints/ (-50 líneas)
POST /pipeline/load-json                            → endpoints/ (igual)
POST /pipeline/search-doi-in-sources                → endpoints/ (igual)
```

### Endpoints (`reconciliation_ops.py` → `endpoints/reconciliation.py`)
```
POST /pipeline/reconcile                            → endpoints/ (-100 líneas)
POST /pipeline/reconcile-all                        → endpoints/ (-80 líneas)
POST /pipeline/reconcile/all-sources                → endpoints/ (-150 líneas)
POST /pipeline/crossref-scopus                      → endpoints/ (-200 líneas)
```

### Endpoints (`admin.py` → `endpoints/admin.py`)
```
DELETE /pipeline/truncate-all                       → endpoints/ (-40 líneas)
POST   /pipeline/init-db                            → endpoints/ (-20 líneas)
GET    /pipeline/scopus/test-extract                → endpoints/ (-40 líneas)
```

## 📐 Principios DDD Aplicados

### 1️⃣ Domain Layer (Puro, sin dependencias externas)

**Directorio**: `domain/`

**Responsabilidades**:
- Definir entidades (agregados) y value objects
- Implementar lógica de negocio core
- Definir interfaces/contratos (repositorios, servicios)
- CERO dependencias a FastAPI, SQLAlchemy, o frameworks

**Ejemplo - Entidad Publication** (`domain/entities/publication.py`):
```python
@dataclass
class Publication:
    doi: Optional[str]
    title: Optional[str]
    journal: Optional[Journal]  # Value object
    publication_year: Optional[int]
    
    def is_in_coverage(self) -> bool:
        return self.in_coverage == "Sí"
    
    def needs_reconciliation(self) -> bool:
        return not self.journal_found or self.in_coverage == "Sin datos"
```

**Servicios de negocio** (`domain/services/pipeline_services.py`):
- `CoverageService`: Lógica de validación de cobertura
- `ExtractionService`: Deduplicación y validación
- `ReconciliationService`: Estrategia matching

### 2️⃣ Application Layer (Orquestación de casos de uso)

**Directorio**: `application/`

**Responsabilidades**:
- Implementar casos de uso/comandos
- Orquestar colaboración entre domain + infrastructure
- Convertir DTOs de entrada a entidades de dominio
- Coordinar transacciones

**Ejemplo - Comando** (`application/commands/pipeline_commands.py`):
```python
class CheckPublicationCoverageCommand:
    def __init__(self, scopus_extractor, openalex_service):
        self.scopus = scopus_extractor
        self.openalex = openalex_service
    
    def execute(self, publications, max_workers=1):
        # Orquestar: Scopus → fallback OpenAlex → merge resultados
        ...
```

### 3️⃣ Infrastructure Layer (Datos, APIs externas)

**Directorio**: `infrastructure/`

**Responsabilidades**:
- Implementar repositorios (SQLAlchemy)
- Adaptadores extractores (OpenAlex, Scopus, etc.)
- Exportadores (Excel, JSON, etc.)
- Caching, persistencia

**Placeholder para implementación futura**:
```python
# infrastructure/repositories/sqlalchemy_publication_repository.py
class SQLAlchemyPublicationRepository(PublicationRepository):
    def find_by_doi(self, doi: str) -> Optional[Publication]:
        # Implementar con SQLAlchemy
        ...
```

### 4️⃣ Endpoints / Presentation Layer (HTTP handlers)

**Directorio**: `endpoints/`

**Responsabilidades**:
- Handlers HTTP delgados (FastAPI)
- Validación de entrada (DTOs)
- Delegación a application commands
- Control de errores HTTP

**Antes (745 líneas acopladas)** → **Después (240 líneas limpias)**:

**ANTES** (coverage.py):
```python
@router.post("/check-publications-coverage")
async def check_publications_coverage(file: UploadFile, max_workers: int):
    # 300 líneas de lógica mezclada + SQL + Excel + logging
    ...
```

**DESPUÉS** (endpoints/scopus_coverage.py):
```python
@router.post("/check-publications-coverage")
async def check_publications_coverage(file: UploadFile, max_workers: int):
    headers, rows = await read_publications_from_excel(raw)
    publications = [_build_pub_entry(row) for row in rows]
    enriched = await extractor.check_publications_coverage(publications, max_workers)
    # Integrar OpenAlex
    await _rescue_not_found_via_openalex(rows, extractor)
    # Generar Excel
    excel_bytes = await generate_publications_coverage_excel(headers, rows)
    return StreamingResponse(io.BytesIO(excel_bytes), ...)
```

## 📊 Beneficios de la Refactorización

| Aspecto | Antes | Después |
|--------|-------|---------|
| **Líneas por archivo** | 745 (coverage), 372 (reconciliation) | ~250 (endpoints), ~300 (domain+app) |
| **Acoplamiento** | ⛔ Alto: FastAPI + SQLAlchemy + logging | ✅ Bajo: Capas separadas |
| **Testabilidad** | ⛔ Difícil: Dependencias globales | ✅ Fácil: Domain layer sin deps |
| **Mantenibilidad** | ⛔ Confusa: Mezcla de responsabilidades | ✅ Clara: Cada capa tiene un propósito |
| **Reusabilidad** | ⛔ Baja: Lógica acoplada a endpoints | ✅ Alta: Domain puro + commands |
| **Testing** | ⛔ Requiere DB/API mocks | ✅ Domain sin deps externas |

## 🚀 Migración Gradual (TODO)

### Fase 1: Infrastructure Layer
Implementar SQLAlchemy repositories y extractors adapters.

```python
# infrastructure/repositories/sqlalchemy_publication_repository.py
class SQLAlchemyPublicationRepository(PublicationRepository):
    def __init__(self, db_session):
        self.db = db_session
    
    def find_by_doi(self, doi: str) -> Optional[Publication]:
        record = self.db.query(CanonicalPublication).filter_by(doi=doi).first()
        return self._to_domain(record) if record else None
```

### Fase 2: Application Layer
Implementar todos los commands/use cases.

```python
# application/commands/check_coverage_command.py
class CheckCoverageCommand:
    def __init__(self, scopus_repo, publication_repo):
        self.scopus = scopus_repo
        self.publications = publication_repo
    
    def execute(self, publication: Publication) -> CoverageCheckResult:
        # Orquestar búsqueda en Scopus
        # Aplicar lógica CoverageService.determine_if_in_coverage()
        # Guardar en repo
        ...
```

### Fase 3: Endpoints Delgados
Refactorizar endpoints para usar commands (ya están básicamente listos).

```python
@router.post("/check-coverage")
def check_publication_coverage(pub_in: PublicationIn, db: Session = Depends(get_db)):
    cmd = CheckCoverageCommand(scopus_repo, publication_repo)
    result = cmd.execute(pub_in.to_domain())
    return result.to_dto()
```

## 📚 Archivos de Compatibilidad (Mantener por ahora)

Los siguientes archivos se mantienen sin cambios para compatibilidad:
- `_pipeline_helpers.py` - Helpers multi-stage rescue
- `_ids.py` - Utilidades de build_pub_entry
- `_json_loader.py` - JSON parsing y detección de fuentes

Se recomienda migrar estos al nuevo `infrastructure/` en futuro.

## 🔗 Referencias

- **Domain Entity** → `domain/entities/publication.py`
- **Domain Services** → `domain/services/pipeline_services.py`
- **Domain Interfaces** → `domain/repositories/repository_interfaces.py`
- **Use Cases** → `application/commands/pipeline_commands.py`
- **DTOs** → `shared/dtos.py`
- **Endpoints** → `endpoints/*.py`

## ✅ Checklist de Completitud

- [x] Crear estructura DDD
- [x] Refactorizar endpoints (scopus_coverage, extraction, reconciliation, admin)
- [x] Crear domain layer (entities, services, repositories)
- [x] Crear application layer (commands/use cases)
- [x] Crear shared layer (DTOs)
- [x] Crear infrastructure placeholders
- [ ] Implementar infrastructure repositories (TODO)
- [ ] Implementar infrastructure extractors adapters (TODO)
- [ ] Escribir tests unitarios para domain layer (TODO)
- [ ] Documentar migraciones código antiguo (TODO)

## 💡 Próximos Pasos

1. **Prueba de compatibilidad**: Verificar que los endpoints aún funcionen
2. **Tests**: Escribir tests para domain layer (sin DB)
3. **Migración Infrastructure**: Implementar repositories y adapters
4. **Limpieza**: Deprecar archivos antiguos (coverage.py, etc.)
5. **Documentación**: Actualizar API docs con nueva estructura
