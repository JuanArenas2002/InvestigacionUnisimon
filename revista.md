# Propuesta de Proyecto: Módulo de Gestión y Extracción de Datos de Revistas Científicas
## Extracción, Normalización y Análisis Bibliométrico con Arquitectura Hexagonal

**Versión**: 1.0 - Actualizada  
**Fecha**: Abril 2026  
**Duración**: 10 semanas (7 de abril - 16 de junio 2026)  
**Público**: - Práctica Profesional

---

## 1. Objetivo del Proyecto

Desarrollar un módulo para la extracción, normalización y gestión de información de revistas científicas, integrando datos de fuentes como Scopus, Web of Science y SciELO, con el fin de consolidar información estructurada y soportar análisis bibliométrico institucional.

El sistema debe ser **escalable, mantenible y flexible** ante cambios tecnológicos, aplicando patrones de arquitectura profesionales que permitan evolucionar sin afectar la lógica de negocio.

---

## 2. Alcance del Proyecto

El practicante deberá implementar:

### 2.1 Extracción de Datos
- Conexión con APIs de fuentes externas (máximo 2 iniciales)
- Scripts de extracción automatizados
- Manejo de paginación y límites de API
- Gestión de reintentos y tolerancia a fallos

### 2.2 Normalización y Limpieza
- Estandarización de formatos (ISSN, URLs, códigos de país, emails)
- Conversión de tipos de datos
- Detección y corrección de inconsistencias
- Normalización de textos

### 2.3 Deduplicación
- Unificación por ISSN como clave lógica
- Resolución de duplicados entre múltiples fuentes
- Validación de integridad

### 2.4 Gestión de Datos Históricos
- Versionamiento SCD2 (Slowly Changing Dimension)
- Almacenamiento de cambios sin sobrescritura
- Auditoría completa de cambios

### 2.5 Almacenamiento de Métricas Bibliométricas
- Recolección anual de cuartiles, h-index, CiteScore, SJR, SNIP, FWCI
- Soporte para múltiples categorías temáticas por revista
- Soporte para ODS multivalor
- Trazabilidad de fuente de datos

### 2.6 Validación y Control de Calidad
- Validación de integridad referencial
- Detección de inconsistencias
- Generación de flags de calidad
- Registro de errores en logs

### 2.7 Exposición de Datos
- Consultas SQL optimizadas
- Vistas para análisis (evolución de métricas, rankings)
- Documentación de endpoints

---

## 3. Enfoque de Arquitectura: Arquitectura Hexagonal (Ports & Adapters)

### 3.1 Principio Clave

**La lógica de negocio NO debe depender de la base de datos ni de APIs externas.**

```
┌──────────────────────────────────────────────────────┐
│                                                      │
│         ADAPTADORES DE ENTRADA (Inbound)             │
│         - API REST / CLI / Scripts ETL               │
│                                                      │
│    ┌───────────────────────────────────────────┐    │
│    │                                           │    │
│    │     PUERTOS (Interfaces / Contratos)     │    │
│    │  - JournalRepository                      │    │
│    │  - MetricsRepository                      │    │
│    │  - ExternalSourceService                  │    │
│    │                                           │    │
│    ├───────────────────────────────────────────┤    │
│    │                                           │    │
│    │      NÚCLEO (Dominio / Lógica)           │    │
│    │  ✓ Entidades: Journal, Metrics, etc.     │    │
│    │  ✓ Reglas: Deduplicación, Validación     │    │
│    │  ✓ Casos de uso: ETL, Análisis           │    │
│    │                                           │    │
│    ├───────────────────────────────────────────┤    │
│    │                                           │    │
│    │   ADAPTADORES DE SALIDA (Outbound)       │    │
│    │  - PostgreSQL / MongoDB                   │    │
│    │  - APIs externas (Scopus, WoS)            │    │
│    │                                           │    │
│    └───────────────────────────────────────────┘    │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 3.2 Estructura de Carpetas

```
journal-management-module/
├── domain/                           # Lógica de negocio (sin dependencias)
│   ├── entities/
│   │   ├── journal.py
│   │   ├── metrics.py
│   │   ├── classification.py
│   │   └── review_policy.py
│   ├── value_objects/
│   │   ├── issn.py
│   │   ├── quartile.py
│   │   ├── country_code.py
│   │   ├── email.py
│   │   └── ods.py
│   ├── repositories/                # Puertos (Interfaces)
│   │   ├── journal_repository.py
│   │   ├── metrics_repository.py
│   │   └── external_source_service.py
│   └── use_cases/
│       ├── extract_journals.py
│       ├── normalize_data.py
│       ├── deduplicate_by_issn.py
│       └── store_metrics.py
│
├── application/                      # Orquestación de casos de uso
│   ├── etl_orchestrator.py
│   ├── journal_service.py
│   └── metrics_service.py
│
├── adapters/
│   ├── inbound/                     # Entrada
│   │   ├── api/
│   │   │   └── fastapi_app.py
│   │   ├── cli/
│   │   │   └── etl_commands.py
│   │   └── schedulers/
│   │       └── cron_jobs.py
│   │
│   └── outbound/                    # Salida
│       ├── database/
│       │   ├── postgresql_adapter.py
│       │   └── models.py
│       └── external_sources/
│           ├── scopus_adapter.py
│           ├── wos_adapter.py
│           └── scielo_adapter.py
│
├── config/
│   ├── settings.py
│   ├── database.py
│   └── logging.py
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
│
├── scripts/
│   ├── init_database.py
│   └── seed_data.py
│
├── requirements.txt
├── setup.py
├── README.md
└── .env.example
```

### 3.3 Beneficio Clave de la Arquitectura Hexagonal

**Si en el futuro se cambia:**
- PostgreSQL → MongoDB: ✅ Solo cambias el adaptador `outbound/database`
- Scopus → otra API: ✅ Solo cambias el adaptador `outbound/external_sources`
- REST API → GraphQL: ✅ Solo cambias el adaptador `inbound/api`

**La lógica de negocio NO se ve afectada.**

---

## 4. Componentes del Sistema

### 4.1 Capa de Dominio (Domain)

**Responsabilidad**: Encapsular la lógica de negocio pura, sin dependencias externas.

#### Entidades principales:

```python
# domain/entities/journal.py
@dataclass
class Journal:
    id: int
    name: str
    name_abbreviated: str
    email: str
    is_peer_reviewed: bool
    publisher_id: int
    country_code: str
    website: Optional[str]
    
    def add_identifier(self, issn: ISSN) -> None:
        """Regla: Agregar identificador validado"""
        if not issn.is_valid():
            raise InvalidISSNError()
    
    def validate_email(self) -> bool:
        """Validar formato de email"""
        return "@" in self.email

@dataclass
class Metrics:
    journal_id: int
    metric_year: int
    category_id: int
    quartile: Quartile
    impact_factor: float
    snip: float
    fwci: float  # Field-Weighted Citation Impact
    h_index: int
    
    def is_top_tier(self) -> bool:
        """Regla: Determinar si es Q1"""
        return self.quartile == Quartile.Q1

@dataclass
class ReviewPolicy:
    journal_id: int
    peer_review_type: str  # 'simple_blind', 'double_blind', 'open', 'other'
    number_of_reviewers: int
    overton: bool

@dataclass
class Classification:
    journal_id: int
    category_id: int
    source: str
    is_primary: bool
```

#### Value Objects (Tipos de valor):

```python
# domain/value_objects/issn.py
@dataclass(frozen=True)  # Immutable
class ISSN:
    value: str
    identifier_type: str  # 'ISSN', 'ISSN_PRINT', 'E-ISSN'
    
    def __post_init__(self):
        if not self._validate():
            raise InvalidISSNError()
    
    def _validate(self) -> bool:
        """Validar formato ISSN"""
        return bool(re.match(r'^\d{4}-?\d{3}[0-9X]$', self.value))
    
    def normalize(self) -> str:
        """Devolver ISSN sin guión"""
        return self.value.replace('-', '')

# domain/value_objects/email.py
@dataclass(frozen=True)
class Email:
    value: str
    
    def __post_init__(self):
        if not self._validate():
            raise InvalidEmailError()
    
    def _validate(self) -> bool:
        """Validar formato email"""
        return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', self.value))

# domain/value_objects/quartile.py
class Quartile(Enum):
    Q1 = "Q1"
    Q2 = "Q2"
    Q3 = "Q3"
    Q4 = "Q4"
    NOT_CLASSIFIED = None

# domain/value_objects/ods.py
@dataclass(frozen=True)
class ODS:
    """Objetivo de Desarrollo Sostenible"""
    number: int  # 1-17
    name: str
    
    def __post_init__(self):
        if not (1 <= self.number <= 17):
            raise ValueError("ODS debe estar entre 1 y 17")
```

### 4.2 Puertos (Interfaces / Contratos)

**Responsabilidad**: Definir contratos que el dominio espera que implementen los adaptadores.

```python
# domain/repositories/journal_repository.py
from abc import ABC, abstractmethod

class JournalRepository(ABC):
    """Puerto: Contrato para persistencia de revistas"""
    
    @abstractmethod
    def find_by_issn(self, issn: ISSN) -> Optional[Journal]:
        """Buscar revista por ISSN"""
        pass
    
    @abstractmethod
    def save(self, journal: Journal) -> Journal:
        """Guardar o actualizar revista"""
        pass
    
    @abstractmethod
    def find_by_id(self, journal_id: int) -> Optional[Journal]:
        """Buscar revista por ID"""
        pass

# domain/repositories/external_source_service.py
class ExternalSourceService(ABC):
    """Puerto: Contrato para integración con fuentes externas"""
    
    @abstractmethod
    def fetch_journals(self, query: str, limit: int = 100) -> List[dict]:
        """Extraer revistas de API externa"""
        pass
    
    @abstractmethod
    def fetch_metrics(self, journal_id: str) -> dict:
        """Extraer métricas de API externa"""
        pass
```

### 4.3 Casos de Uso (Use Cases)

**Responsabilidad**: Orquestar la lógica de negocio, usando puertos.

```python
# domain/use_cases/extract_journals.py
class ExtractJournalsUseCase:
    def __init__(self, external_source: ExternalSourceService):
        self.external_source = external_source
    
    def execute(self, source_name: str, query: str) -> List[dict]:
        """
        Extraer revistas desde fuente externa
        Devuelve datos sin procesar
        """
        raw_journals = self.external_source.fetch_journals(query)
        return raw_journals

# domain/use_cases/deduplicate_by_issn.py
class DeduplicateByISSNUseCase:
    def __init__(self, journal_repo: JournalRepository):
        self.journal_repo = journal_repo
    
    def execute(self, journals: List[Journal]) -> List[Journal]:
        """
        Deduplicar revistas por ISSN
        Fusiona datos de múltiples fuentes
        """
        unique_journals = {}
        
        for journal in journals:
            issn = journal.primary_issn
            
            if issn in unique_journals:
                # Fusionar datos
                existing = unique_journals[issn]
                existing.merge(journal)
            else:
                unique_journals[issn] = journal
        
        return list(unique_journals.values())

# domain/use_cases/store_metrics.py
class StoreMetricsUseCase:
    def __init__(self, metrics_repo: MetricsRepository):
        self.metrics_repo = metrics_repo
    
    def execute(self, journal_id: int, metrics: Metrics) -> Metrics:
        """
        Almacenar métricas sin sobrescribir histórico
        Utiliza SCD2
        """
        # Validaciones de dominio
        if not metrics.is_valid():
            raise InvalidMetricsError()
        
        # Delegar persistencia al repositorio
        return self.metrics_repo.save(metrics)
```

### 4.4 Adaptadores (Implementaciones concretas)

#### Entrada (Inbound):

```python
# adapters/inbound/api/fastapi_app.py
from fastapi import FastAPI
from domain.use_cases import ExtractJournalsUseCase

app = FastAPI()

@app.post("/journals/extract")
async def extract_journals(source: str, query: str):
    """Endpoint para iniciar extracción"""
    use_case = ExtractJournalsUseCase(external_source)
    journals = use_case.execute(source, query)
    return {"journals": journals, "count": len(journals)}

@app.get("/journals/{journal_id}/metrics")
async def get_metrics(journal_id: int, year: int = None):
    """Endpoint para consultar métricas"""
    metrics = metrics_service.get_by_journal(journal_id, year)
    return metrics

@app.get("/journals/filter/ods")
async def get_journals_by_ods(ods_number: int):
    """Endpoint para consultar revistas por ODS"""
    journals = journal_service.get_by_ods(ods_number)
    return journals
```

#### Salida (Outbound) - PostgreSQL:

```python
# adapters/outbound/database/postgresql_adapter.py
import sqlalchemy as sa
from domain.repositories import JournalRepository
from domain.entities import Journal, ISSN

class PostgreSQLJournalRepository(JournalRepository):
    """Adaptador: Implementación de JournalRepository con PostgreSQL"""
    
    def __init__(self, db_session):
        self.db = db_session
    
    def find_by_issn(self, issn: ISSN) -> Optional[Journal]:
        """Buscar revista por ISSN en PostgreSQL"""
        result = self.db.query(JournalModel).join(
            JournalIdentifierModel
        ).filter(
            JournalIdentifierModel.value == issn.normalize(),
            JournalIdentifierModel.is_canonical == True
        ).first()
        
        return self._to_entity(result) if result else None
    
    def save(self, journal: Journal) -> Journal:
        """Guardar revista en PostgreSQL"""
        model = JournalModel(
            name=journal.name,
            name_abbreviated=journal.name_abbreviated,
            email=journal.email,
            is_peer_reviewed=journal.is_peer_reviewed,
            publisher_id=journal.publisher_id,
            country_code=journal.country_code,
            website=journal.website
        )
        self.db.add(model)
        self.db.commit()
        return self._to_entity(model)
    
    def _to_entity(self, model: JournalModel) -> Journal:
        """Convertir modelo DB a entidad de dominio"""
        return Journal(
            id=model.id,
            name=model.name,
            name_abbreviated=model.name_abbreviated,
            email=model.email,
            is_peer_reviewed=model.is_peer_reviewed,
            publisher_id=model.publisher_id,
            country_code=model.country_code,
            website=model.website
        )

# adapters/outbound/external_sources/scopus_adapter.py
import requests
from domain.repositories import ExternalSourceService

class ScopusAdapter(ExternalSourceService):
    """Adaptador: Implementación de ExternalSourceService para Scopus API"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.elsevier.com/content/search/scopus"
    
    def fetch_journals(self, query: str, limit: int = 100) -> List[dict]:
        """Extraer revistas de Scopus API"""
        params = {
            'query': query,
            'apiKey': self.api_key,
            'count': limit
        }
        
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()
        
        return response.json().get('search-results', {}).get('entry', [])
    
    def fetch_metrics(self, journal_id: str) -> dict:
        """Extraer métricas de Scopus API (incluyendo FWCI, SNIP)"""
        url = f"{self.base_url}/{journal_id}"
        response = requests.get(url, headers={'API-KEY': self.api_key})
        return response.json()

# Si cambias a Web of Science, solo creas:
# adapters/outbound/external_sources/wos_adapter.py
# que implementa la misma interfaz ExternalSourceService
```

### 4.5 Capa de Aplicación (Application)

**Responsabilidad**: Orquestar casos de uso, inyectar dependencias.

```python
# application/etl_orchestrator.py
class ETLOrchestrator:
    """Orquestador: Coordina el flujo ETL completo"""
    
    def __init__(self, 
                 external_source: ExternalSourceService,
                 journal_repo: JournalRepository,
                 metrics_repo: MetricsRepository):
        self.external_source = external_source
        self.journal_repo = journal_repo
        self.metrics_repo = metrics_repo
    
    def run_etl(self, source_name: str, query: str):
        """
        Ejecutar flujo ETL completo:
        1. Extracción
        2. Normalización (incluyendo emails, nombres abreviados, ODS)
        3. Deduplicación
        4. Persistencia
        5. Métricas (incluyendo FWCI, SNIP)
        """
        print(f"[ETL] Iniciando extracción desde {source_name}")
        
        # Paso 1: Extracción
        extract_use_case = ExtractJournalsUseCase(self.external_source)
        raw_journals = extract_use_case.execute(source_name, query)
        
        # Paso 2: Transformación/Normalización
        normalize_use_case = NormalizeDataUseCase()
        normalized = normalize_use_case.execute(raw_journals)
        
        # Paso 3: Deduplicación
        dedupe_use_case = DeduplicateByISSNUseCase(self.journal_repo)
        unique_journals = dedupe_use_case.execute(normalized)
        
        # Paso 4: Persistencia
        for journal in unique_journals:
            saved_journal = self.journal_repo.save(journal)
            print(f"[ETL] Guardado: {saved_journal.name}")
        
        # Paso 5: Métricas
        for journal in unique_journals:
            metrics = self.external_source.fetch_metrics(journal.id)
            store_metrics = StoreMetricsUseCase(self.metrics_repo)
            store_metrics.execute(journal.id, metrics)
        
        print(f"[ETL] Completado: {len(unique_journals)} revistas procesadas")
```

---

## 5. Flujo del Sistema

```
┌─────────────────────────────────────────────────────────────┐
│  1. EXTRACCIÓN (Extract)                                    │
│  └─ Consumir API de Scopus/WoS/SciELO                       │
│  └─ Devolver datos sin procesar (JSON)                      │
├─────────────────────────────────────────────────────────────┤
│  2. VALIDACIÓN (Validate)                                   │
│  └─ Verificar estructura, formatos, emails                  │
│  └─ Detectar errores tempranos                              │
├─────────────────────────────────────────────────────────────┤
│  3. TRANSFORMACIÓN (Transform)                              │
│  └─ Normalizar ISSN (print, e-issn), países, áreas         │
│  └─ Normalizar emails, nombres abreviados                   │
│  └─ Mapear ODS (pueden ser múltiples)                       │
│  └─ Convertir tipos de datos                                │
├─────────────────────────────────────────────────────────────┤
│  4. DEDUPLICACIÓN (Deduplicate)                             │
│  └─ Identificar revistas por ISSN (incluyendo ISSN print)  │
│  └─ Fusionar datos de múltiples fuentes                     │
├─────────────────────────────────────────────────────────────┤
│  5. PERSISTENCIA (Load)                                     │
│  └─ Insertar revistas en BD                                 │
│  └─ Registrar identidades (incluyendo RNI, RII)             │
│  └─ Registrar ODS multivalor                                │
│  └─ Versionamiento SCD2                                     │
├─────────────────────────────────────────────────────────────┤
│  6. METADATOS (Store Metadata)                              │
│  └─ Guardar políticas editorial (revisión, pares, Overton) │
│  └─ Guardar metadatos de editorial e institución            │
│  └─ Sin sobrescribir histórico                              │
├─────────────────────────────────────────────────────────────┤
│  7. MÉTRICAS (Store Metrics)                                │
│  └─ Guardar métricas anuales (FWCI, SNIP, etc.)             │
│  └─ Desglosar por categoría                                 │
│  └─ Sin sobrescribir histórico                              │
├─────────────────────────────────────────────────────────────┤
│  8. ANÁLISIS (Query)                                        │
│  └─ Consultas SQL optimizadas                               │
│  └─ Vistas para análisis                                    │
│  └─ Reportes                                                │
└─────────────────────────────────────────────────────────────┘
```

---

## 6. Datos a Recolectar (COMPLETO)

### 6.1 Información General

- **Nombre oficial** de la revista
- **Nombre abreviado** (ej: "Nat." para "Nature", "Am. J. Med.")
- **País** (ISO 3166-1)
- **Editorial**
- **Sitio web**
- **Correo electrónico de contacto** (email institucional)
- **Idiomas de publicación** (multivalor)
- **Revista arbitrada** (Sí/No) - Booleano peer-reviewed

### 6.2 Identificación

- **ISSN** (Número de Serie Normalizado Internacional) - versión en línea
- **ISSN Print** (versión impresa, si existe)
- **E-ISSN** (versión electrónica alternativa, si existe)
- **Scopus ID**
- **Web of Science ID**
- **SciELO ID**
- **RNI** (Registro Nacional de Indexación)
- **RII** (Registro de Indexación Institucional)

### 6.3 Clasificación Temática

- **Área** (Computer Science, Medicine, Physics, etc.)
- **Subárea** (Artificial Intelligence, Cardiology, Quantum Mechanics)
- **Categoría** (Machine Learning, Heart Failure, Quantum Computing)
- **Fuente de clasificación** (Scopus, WoS, SciELO)
- **ODS** (Objetivos de Desarrollo Sostenible) - **MULTIVALOR**
  - ODS 1 a 17 (una revista puede tener múltiples ODS)

### 6.4 Políticas Editorial

- **Tipo de revisión por pares**:
  - Simple ciego (peer anónimo, autores visibles)
  - Doble ciego (ambos anónimos)
  - Revisión abierta (ambos visibles)
  - Otro/Combinado
- **Número de pares evaluadores activos** (cantidad aproximada)
- **Overton** (Acceso a políticas de transparencia editorial: Sí/No)

### 6.5 Indexación

- Base de datos (Scopus, WoS, SciELO, DOAJ, etc.)
- Año de inicio de cobertura
- Año de fin (si aplica)
- Estado (activa/histórica)

### 6.6 Acceso Abierto

- Tipo (Gold, Hybrid, Green, Bronze, Closed, Delayed)
- Costo de procesamiento (APC)
- Año de cambio de modelo

### 6.7 Metadatos de la Editorial (Publisher Metadata)

- **Nombre de la editorial**
- **País de la editorial** (ISO 3166-1)
- **Sitio web de la editorial**
- **Correo de contacto de la editorial**
- **Teléfono de la editorial**
- **Persona de contacto principal**
- **Empresa matriz** (si aplica)
- **Tipo de editorial** (Academia, Comercial, ONG, Gobierno, Otro)

### 6.8 Metadatos de la Empresa/Institución (Organization Metadata)

- **Nombre de la empresa/institución propietaria**
- **Tipo de organización** (Universidad, Instituto, Editorial, ONG, Gobierno, Asociación Profesional)
- **País de origen** (ISO 3166-1)
- **Sitio web de la institución**
- **Contacto principal**
- **Sector** (Académico, Comercial, Gobierno, Público, Otro)

### 6.9 Métricas Bibliométricas (Por año y categoría)

- **Cuartil** (Q1, Q2, Q3, Q4)
- **Índice H** (h-index)
- **Impact Factor** (2-year y 5-year)
- **CiteScore**
- **SJR** (SCImago Journal Rank)
- **SNIP** (Source Normalized Impact per Paper)
- **FWCI** (Field-Weighted Citation Impact - métrica de Scopus)
- **Total de documentos**
- **Total de citas**
- **Tasa de citación promedio**

---

## 7. Tecnologías Recomendadas

### 7.1 Backend / ETL
- **Python 3.10+**: Lenguaje principal
- **SQLAlchemy**: ORM independiente de BD
- **Requests**: Consumo de APIs
- **APScheduler**: Scheduling de ETL
- **Loguru**: Logging estructurado
- **Pydantic**: Validación de datos (emails, ISSN, ODS)

### 7.2 Base de Datos
- **PostgreSQL 14+**: Principal (por características avanzadas)
- **Alternativa**: MongoDB (si en el futuro se requiere)

### 7.3 Testing
- **Pytest**: Framework de tests
- **pytest-cov**: Cobertura

### 7.4 Entrada
- **FastAPI**: API REST (opcional)
- **Click**: CLI para ETL manual
- **APScheduler**: Cron jobs

### 7.5 Visualización (Opcional)
- Framework de Frontend moderno

---

## 8. Plan de Trabajo: 10 Semanas

### Fase 1: Análisis y Diseño (Semanas 1-2)

**Objetivos**:
- Seleccionar 2 fuentes iniciales
- Analizar APIs completas
- Definir reglas de negocio (incluyendo ODS, tipos revisión, etc.)
- Diseñar arquitectura hexagonal

**Entregables**:
- [ ] Documento de análisis de APIs
- [ ] Diagrama de arquitectura
- [ ] Reglas de negocio
- [ ] Backlog priorizado

**Horas estimadas**: 80 horas

---

### Fase 2: Extracción (Semanas 3-4)

**Objetivos**:
- Extractores funcionales (DOAJ + otra fuente)
- Manejo de todos los campos nuevos
- Rate limiting y reintentos
- Tests unitarios >80%

**Entregables**:
- [ ] Adaptadores funcionales
- [ ] Tests >80%
- [ ] Logging estructurado

**Horas estimadas**: 100 horas

---

### Fase 3: Transformación (Semanas 5-6)

**Objetivos**:
- Normalizar ISSN (incluyendo print)
- Validar emails
- Mapear ODS (multivalor)
- Normalizar tipos revisión
- Value Objects

**Entregables**:
- [ ] Módulo transformación
- [ ] Value Objects (Email, ODS, etc.)
- [ ] Tests transformación

**Horas estimadas**: 80 horas

---

### Fase 4: Persistencia (Semanas 7-8)

**Objetivos**:
- Deduplicación ISSN completa
- SCD2 para todos los atributos
- Tablas para ODS, metadatos, políticas
- Transacciones atómicas

**Entregables**:
- [ ] Repositorio PostgreSQL
- [ ] SCD2 funcional
- [ ] Tests integridad

**Horas estimadas**: 120 horas

---

### Fase 5: Validación y Calidad (Semana 9)

**Objetivos**:
- QA completo
- Validadores de dominio
- Flags de integridad
- Logs ETL

**Entregables**:
- [ ] Validadores
- [ ] Quality flags
- [ ] Tests E2E

**Horas estimadas**: 60 horas

---

### Fase 6: Entrega (Semana 10)

**Objetivos**:
- Documentación completa
- Demo funcional
- Capacitación equipo
- Producción lista

**Entregables**:
- [ ] README.md
- [ ] Documentación arquitectura
- [ ] Demo
- [ ] Runbooks

**Horas estimadas**: 60 horas

---

## 9. Indicadores de Éxito

| Métrica | Objetivo |
|---------|----------|
| % Revistas unificadas ISSN | >95% |
| % Registros con ODS | >70% |
| % Métricas FWCI/SNIP | >60% |
| Emails validados | >98% |
| Tiempo ETL (1000) | <10 min |
| Cobertura tests | >85% |

---

## 10. Riesgos y Mitigación

| Riesgo | Probabilidad | Impacto | Mitigación |
|--------|-------------|--------|-----------|
| APIs cambian | Media | Alto | Versionamiento, tests robustos |
| Datos incompletos | Alta | Medio | Quality flags, alerts |
| Duplicación | Alta | Crítico | ISSN clave, deduplicación |
| Rate limiting | Alta | Medio | Backoff exponencial |
| Performance | Media | Alto | Índices, particionamiento |

---

## 11. Ventajas de la Arquitectura Hexagonal

### 11.1 Flexibilidad Futura

```
Cambio de tecnología → Solo reemplaza adaptador
- PostgreSQL → MongoDB ✅
- Scopus → otra API ✅
- REST → GraphQL ✅
```

### 11.2 Testabilidad

Inyectas mocks sin tocar domain.

### 11.3 Escalabilidad

Domain puro, adaptadores distribuibles, ETL multiworker.

---

## 12. Recomendaciones como Gestor

1. **Mantén simple** - Máximo 2 fuentes
2. **Validaciones temprano** - Detecta errores rápido
3. **Reuniones semanales** - Sincronización
4. **Testing constante** - >80% cobertura
5. **Documentación inline** - Código autodocumentado

---

## 13. Estructura de Entrega Final

```
journal-management-module/
├── README.md
├── ARCHITECTURE.md
├── SETUP.md
├── OPERATIONS.md
├── domain/
├── adapters/
├── application/
├── tests/
├── scripts/
├── requirements.txt
└── .env.example
```

---

## 14. Resumen Ejecutivo

Módulo de extracción y gestión de datos de revistas científicas con **Arquitectura Hexagonal** que captura información completa:

- ✅ Identificadores múltiples (ISSN, print, RNI, RII)
- ✅ Metadatos completos (editorial, organización, contactos)
- ✅ Políticas editoriales (tipo revisión, pares, Overton)
- ✅ ODS multivalor
- ✅ Métricas avanzadas (FWCI, SNIP)
- ✅ Escalable, mantenible, flexible

**Resultado**: Módulo production-ready que el practicante puede usar como referencia profesional.

---

**Versión**: 1.0 - Actualizada  
**Fecha**: Abril 2026  
**Duración**: 10 semanas  
**Estado**: Aprobado para implementación