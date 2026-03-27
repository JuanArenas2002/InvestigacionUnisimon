# Convocatoria

API y utilidades para la gestión, extracción y reconciliación de datos científicos: publicaciones, autores y catálogos provenientes de fuentes como OpenAlex, Scopus y Web of Science.

Arquitectura **Domain-Driven Design (DDD)** con separación clara de responsabilidades entre capas de presentación, aplicación, dominio e infraestructura.

## Estructura

```
convocatoria/
├── api/
│   ├── routers/
│   │   ├── pipeline/              # Pipeline DDD: extracción, reconciliación, cobertura
│   │   │   ├── domain/            # Capa de dominio: entidades y lógica de negocio
│   │   │   ├── application/       # Capa de aplicación: casos de uso y orquestación
│   │   │   ├── endpoints/         # Capa de presentación: handlers HTTP
│   │   │   ├── infrastructure/    # Capa de infraestructura: persistencia e integraciones
│   │   │   └── shared/            # DTOs y utilidades compartidas
│   │   ├── admin.py               # Administración general
│   │   ├── authors.py             # Gestión de autores
│   │   ├── charts.py              # Generación de gráficos y análisis
│   │   ├── publications.py        # Gestión de publicaciones (filtro institucional_only disponible)
│   │   ├── scopus.py              # Búsqueda en Scopus
│   │   ├── search.py              # Búsqueda general
│   │   └── stats.py               # Estadísticas
│   ├── services/                  # Servicios de aplicación (Excel, gráficos)
│   ├── exporters/                 # Exportadores (Excel con estilos)
│   ├── schemas/                   # Esquemas Pydantic para validación
│   ├── dependencies.py            # Inyección de dependencias FastAPI
│   ├── main.py                    # Punto de entrada
│   └── utils.py                   # Utilidades generales
│
├── extractors/                    # Conectores a fuentes externas
│   ├── base.py                    # Interfaz base (StandardRecord)
│   ├── scopus.py                  # Extractor Scopus
│   ├── openalex/                  # OpenAlex con DDD
│   │   ├── domain/                # Modelos de dominio OpenAlex
│   │   ├── application/           # Casos de uso OpenAlex
│   │   └── infrastructure/        # Acceso a API OpenAlex
│   ├── cvlac.py                   # Extractor CVLaC
│   ├── datos_abiertos.py          # Extractor Datos Abiertos
│   ├── serial_title.py            # Extractor Serial Title
│   └── wos.py                     # Extractor Web of Science
│
├── db/
│   ├── models.py                  # Modelos SQLAlchemy
│   ├── session.py                 # Sesiones de base de datos
│   ├── migration_*.sql            # Scripts de migración
│   └── truncate_all.sql           # Limpieza de datos
│
├── reconciliation/
│   ├── engine.py                  # Motor de reconciliación
│   ├── fuzzy_matcher.py           # Matching difuso
│   └── __init__.py
│
├── scripts/                       # Tareas administrativas
│   ├── clean_data.py              # Limpieza de datos
│   ├── quality_reports.py         # Reportes de calidad
│   ├── backfill_provenance.py     # Rellenado de provenance
│   └── *.py                       # Otros scripts
│
├── shared/                        # Código compartido
│   └── normalizers.py             # Normalizadores
│
├── docs/                          # Documentación
│   ├── DATA_DICTIONARY.md         # Diccionario de datos
│   ├── ENDPOINTS_*.md             # Documentación de endpoints
│   ├── CRITERIA.md                # Criterios de negocio
│   ├── OPTIMIZATIONS.md           # Optimizaciones
│   └── *.md                       # Otros documentos
│
├── tests/                         # Tests unitarios
├── OpenAlexJson/                  # Datos de ejemplo
├── reports/                       # Reportes generados
├── config.py                      # Configuración central
└── requirements.txt               # Dependencias Python
```

## Características Principales

- **Extracción Multi-Fuente**: Integración con Scopus, OpenAlex, Web of Science, CVLaC y Datos Abiertos
- **Análisis de Citaciones**: Extracción y visualización de métricas de citación desde Scopus
- **Reconciliación de Datos**: Motor fuzzy matching para deduplicación e identificación de duplicados
- **Exportación Versátil**: Generación de reportes en Excel con estilos personalizados y gráficos PNG
- **Cobertura de Revistas**: Análisis de cobertura de publicaciones por revista
- **API RESTful**: Documentación interactiva com Swagger UI y ReDoc

## Arquitectura

### Capas DDD

La aplicación sigue el patrón **Domain-Driven Design** organizando el código en cuatro capas:

1. **Capa de Presentación (Endpoints)**
   - Handlers HTTP delgados en `api/routers/pipeline/endpoints/`
   - Responsabilidad: Validar entrada, invocar casos de uso, formatear salida
   - No contiene lógica de negocio

2. **Capa de Aplicación (Application)**
   - Orquestación de casos de uso en `api/routers/pipeline/application/`
   - Responsabilidad: Coordinar flujos, transformar DTOs
   - Casos de uso: extracción, verificación, reconciliación

3. **Capa de Dominio (Domain)**
   - Lógica de negocio pura en `api/routers/pipeline/domain/`
   - Responsabilidad: Reglas de negocio, entidades, servicios de dominio
   - Independiente de frameworks y bases de datos

4. **Capa de Infraestructura (Infrastructure)**
   - Implementaciones técnicas en `api/routers/pipeline/infrastructure/`
   - Responsabilidad: Persistencia, integraciones externas, adaptadores
   - Implementa interfaces definidas en el dominio

### Extractores

Cada fuente externa tiene su propio extractor que implementa `StandardRecord`:
- `StandardRecord`: Modelo de datos normalizado compartido
- Cada extractor convierte datos fuente al formato estándar
- Compatible con reconciliación y exportación

## Requisitos

- Python 3.8+
- Entorno virtual recomendado
- Credenciales de API para Scopus y OpenAlex

## Instalación

```bash
# Clonar el repositorio
git clone https://github.com/tu-usuario/convocatoria.git
cd convocatoria

# Crear entorno virtual
python -m venv venv

# Activar entorno (Windows)
.\venv\Scripts\Activate.ps1

# Activar entorno (Linux/macOS)
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

## Configuración

La aplicación requiere credenciales para acceder a las APIs externas. Configura las siguientes variables:

### Método 1: Variables de Entorno (.env)

Crea un archivo `.env` en la raíz del proyecto:

```bash
SCOPUS_API_KEY=tu_clave_scopus
OPENALEX_API_KEY=tu_clave_openalex
DATABASE_URL=sqlite:///./data.db
```

### Método 2: Archivo config.py

Edita `config.py` directamente con tus credenciales. **No es recomendado para producción.**

## Ejecución

### Desarrollo

```bash
# Ejecutar con auto-reload
python -m uvicorn api.main:app --reload --port 8000
```

### Producción

```bash
# Ejecutar con workers
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

La documentación interactiva estará disponible en:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## Endpoints Principales

### Pipeline de Extracción y Análisis
- `GET /api/pipeline/` - Estado del pipeline
- `POST /api/pipeline/extract` - Extraer publicaciones
- `POST /api/pipeline/reconcile` - Reconciliar datos
- `GET /api/pipeline/coverage` - Análisis de cobertura

### Autores
- `GET /api/authors` - Listar autores
- `GET /api/authors/{author_id}` - Detalle del autor
- `POST /api/authors/{author_id}/charts/download-all` - Generar gráficos + Excel + ZIP

### Búsqueda
- `GET /api/search/publications` - Buscar publicaciones
- `GET /api/search/authors` - Buscar autores
- `POST /api/scopus/search` - Búsqueda directa en Scopus

### Catálogos
- `GET /api/catalogs/journals` - Listar revistas
- `POST /api/catalogs/journals/analyze` - Analizar cobertura

Para más detalles, consulta la documentación en `docs/ENDPOINTS_*.md`

## Estructura de Datos

Consulta `docs/DATA_DICTIONARY.md` para la descripción completa de:
- Modelos de base de datos
- Esquemas Pydantic
- DTOs del pipeline
- Formatos de exportación

## Scripts Disponibles

```bash
# Reportes de calidad
python scripts/quality_reports.py

# Limpieza de datos
python scripts/clean_data.py

# Rellenar campos de provenance
python scripts/backfill_provenance.py

# Truncar todas las tablas
python scripts/truncate_all.py
```

## Documentación

La documentación técnica se encuentra en `docs/`:
- `ENDPOINTS_ALL.md` - Todos los endpoints disponibles
- `ENDPOINTS_PIPELINE.md` - Endpoints específicos del pipeline
- `CRITERIA.md` - Criterios de negocio y validación
- `OPTIMIZATIONS.md` - Mejoras de rendimiento aplicadas
- `IMPLEMENTATION_SUMMARY.md` - Resumen de implementación
- `PROYECTO.md` - Descripción general del proyecto

## Pruebas

La suite de pruebas se encuentra en `tests/`:

```bash
# Ejecutar tests
python -m pytest tests/

# Con cobertura
python -m pytest tests/ --cov=api --cov=extractors
```

## Troubleshooting

### Error de conexión a Scopus
- Verifica que `SCOPUS_API_KEY` esté configurado correctamente en `.env`
- Comprueba la conectividad de red

### Error de migraciones de base de datos
- Ejecuta: `python scripts/migrate_*.py` en orden numérico (v2, v3, v4)
- Consulta `docs/IMPLEMENTATION_SUMMARY.md` para instrucciones detalladas

### Métricas de memoria alta en OpenAlex
- Lee `docs/OPTIMIZATIONS.md` para técnicas de procesamiento chunked
- Considera ejecutar con menos registros por batch

## Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Abre un issue describiendo la feature o bug
2. Crea una rama: `git checkout -b feature/mi-feature`
3. Realiza los cambios siguiendo la arquitectura DDD
4. Añade tests para nuevas funcionalidades
5. Envía un pull request referenciando el issue

## Licencia

[MIT](LICENSE)

## Contacto

Para preguntas o sugerencias, abre un issue en el repositorio.