# Diccionario de Datos — Base de Datos de Reconciliación Bibliográfica

> Última actualización: 13 de marzo de 2026
> Documentación completa de todos los campos de la BD

---

## Tabla de Contenidos

1. [Tablas Canónicas](#tablas-canónicas)
2. [Tablas por Fuente](#tablas-por-fuente)
3. [Tablas de Auditoría](#tablas-de-auditoría)
4. [Tipos de Datos](#tipos-de-datos)

---

## TABLAS CANÓNICAS

### `canonical_publications`

Registro "dorado" único que representa una publicación científica unificada.

| Campo | Tipo | Nulo | Índice | Descripción |
|-------|------|------|--------|-------------|
| `id` | INTEGER | NO | PK | Identificador único |
| `doi` | VARCHAR(100) | SÍ | UNIQUE | Digital Object Identifier normalizado (10.xxxx/yyyy) |
| `pmid` | VARCHAR(50) | SÍ | UNIQUE | PubMed ID |
| `pmcid` | VARCHAR(50) | SÍ | UNIQUE | PubMed Central ID |
| `title` | TEXT | NO | FTS | Título de la publicación. Min 5 caracteres. |
| `normalized_title` | TEXT | SÍ | - | Título sin tildes (para búsquedas) |
| `publication_year` | INTEGER | SÍ | INDEX | Año de publicación (1900-2099) |
| `publication_date` | VARCHAR(50) | SÍ | - | Fecha completa (ej: "2023-03-15") |
| `publication_type` | VARCHAR(100) | SÍ | INDEX | journal-article\|review\|proceeding\|book\|report\|... |
| `language` | VARCHAR(10) | SÍ | - | Código ISO 639-1 (en, es, fr, ...) |
| `journal_id` | INTEGER | SÍ | FK | Referencia a `journals` |
| `source_journal` | VARCHAR(500) | SÍ | - | Nombre de revista (texto libre si no existe en BD) |
| `issn` | VARCHAR(20) | SÍ | UNIQUE | International Standard Serial Number |
| `is_open_access` | BOOLEAN | SÍ | - | ¿Open Access? |
| `oa_status` | VARCHAR(50) | SÍ | - | OA status (green, gold, hybrid, closed) |
| `citation_count` | INTEGER | NO | - | Total de citas (agregado de fuentes) Default: 0 |
| `institutional_authors_count` | INTEGER | NO | - | Autores de la institución configurada. Default: 0 |
| `sources_count` | INTEGER | NO | - | Número de fuentes que reportan esta publicación. Default: 1 |
| `field_provenance` | JSONB | SÍ | - | {"title": "openalex", "year": "scopus", ...} |
| `created_at` | TIMESTAMP | NO | - | Fecha de creación del registro canónico |
| `updated_at` | TIMESTAMP | NO | - | Fecha de última actualización |

**Índices:**
- PK: `id`
- UNIQUE: `doi`, `pmid`, `pmcid` (pueden ser NULL múltiples)
- INDEX: `publication_year`, `publication_type`
- FTS: Full-text search en `title`

---

### `authors`

Autores normalizados con múltiples identificadores.

| Campo | Tipo | Nulo | Índice | Descripción |
|-------|------|------|--------|-------------|
| `id` | INTEGER | NO | PK | ID único |
| `name` | VARCHAR(500) | NO | INDEX | Nombre normalizado del autor |
| `orcid` | VARCHAR(50) | SÍ | UNIQUE | ORCID (0000-0000-0000-000X) |
| `scopus_author_id` | VARCHAR(50) | SÍ | UNIQUE | ID de Scopus |
| `openalex_author_id` | VARCHAR(100) | SÍ | UNIQUE | ID de OpenAlex |
| `wos_author_id` | VARCHAR(100) | SÍ | - | ID de Web of Science |
| `researcher_id` | VARCHAR(100) | SÍ | - | ResearcherID (deprecado, usar ORCID) |
| `institution_id` | INTEGER | SÍ | FK | Institución afiliada (puede cambiar en el tiempo) |
| `author_provenance` | JSONB | SÍ | - | {"orcid": "openalex", "name": "scopus", ...} |
| `created_at` | TIMESTAMP | NO | - | |
| `updated_at` | TIMESTAMP | NO | - | |

**Nota:** Un autor puede tener múltiples afiliaciones a lo largo del tiempo (no normalizamos esto en v1).

---

### `publication_authors`

Relación N:M entre publicaciones y autores (preserva orden).

| Campo | Tipo | Nulo | Índice | Descripción |
|-------|------|------|--------|-------------|
| `id` | INTEGER | NO | PK | |
| `publication_id` | INTEGER | NO | FK | Referencia a `canonical_publications` |
| `author_id` | INTEGER | NO | FK | Referencia a `authors` |
| `author_position` | INTEGER | SÍ | - | Posición en lista (1, 2, 3, ...) |
| `is_institutional` | BOOLEAN | NO | - | ¿Es de la institución configurada? |
| `created_at` | TIMESTAMP | NO | - | |

**Índices:** PK `id`, FK `publication_id`, FK `author_id`, UK `(publication_id, author_id)`

---

### `journals`

Catálogo normalizado de revistas.

| Campo | Tipo | Nulo | Índice | Descripción |
|-------|------|------|--------|-------------|
| `id` | INTEGER | NO | PK | |
| `issn` | VARCHAR(20) | SÍ | UNIQUE | ISSN (8 dígitos, formateado) |
| `name` | VARCHAR(500) | NO | INDEX | Nombre oficial de la revista |
| `publisher` | VARCHAR(300) | SÍ | - | Editorial |
| `country` | VARCHAR(100) | SÍ | - | País de origen |
| `created_at` | TIMESTAMP | NO | - | |

---

### `institutions`

Catálogo de instituciones afiliadas.

| Campo | Tipo | Nulo | Índice | Descripción |
|-------|------|------|--------|-------------|
| `id` | INTEGER | NO | PK | |
| `ror_id` | VARCHAR(100) | SÍ | UNIQUE | ROR ID (Research Organization Registry) |
| `name` | VARCHAR(500) | NO | INDEX | Nombre oficial |
| `country` | VARCHAR(100) | SÍ | - | País |
| `type` | VARCHAR(50) | SÍ | - | Tipo (university, company, nonprofit, ...) |
| `created_at` | TIMESTAMP | NO | - | |

---

## TABLAS POR FUENTE

Cada fuente tiene una tabla con campos tipados y validados. Todas vinculan a `canonical_publications` y guardan el estado de reconciliación.

### `openalex_records`

| Campo | Tipo | Nulo | Nota |
|-------|------|------|------|
| `id` | INTEGER | NO | PK |
| `openalex_work_id` | VARCHAR(100) | NO | ID único de OpenAlex (ej: W123456789) |
| `canonical_publication_id` | INTEGER | SÍ | FK a canonical_publications (NULL si no reconciliado) |
| `doi` | VARCHAR(100) | SÍ | DOI normalizado |
| `title` | TEXT | SÍ | |
| `publication_year` | INTEGER | SÍ | |
| `publication_date` | VARCHAR(50) | SÍ | |
| `publication_type` | VARCHAR(100) | SÍ | |
| `source_journal` | VARCHAR(500) | SÍ | |
| `issn` | VARCHAR(20) | SÍ | |
| `is_open_access` | BOOLEAN | SÍ | |
| `oa_status` | VARCHAR(50) | SÍ | "green", "gold", "closed" |
| `citation_count` | INTEGER | NO | Default: 0 |
| `authors_text` | TEXT | SÍ | Cadena de autores (normalizada) |
| `raw_data` | JSONB | SÍ | Respuesta JSON original de API |
| `status` | VARCHAR(50) | NO | pending\|reconciled\|error DEFAULT 'pending' |
| `match_type` | VARCHAR(50) | SÍ | doi_exact\|fuzzy\|manual_review\|new |
| `match_score` | FLOAT | SÍ | Score de coincidencia (0-100) |
| `extracted_at` | TIMESTAMP | NO | |
| `reconciled_at` | TIMESTAMP | SÍ | |

**Estructura similar para:** `scopus_records`, `wos_records`, `cvlac_records`, `datos_abiertos_records`

---

## TABLAS DE AUDITORÍA

### `reconciliation_log`

Bitácora de cada decisión de reconciliación (auditoría completa).

| Campo | Tipo | Nulo | Descripción |
|-------|------|------|-------------|
| `id` | INTEGER | NO | PK |
| `source_record_id` | INTEGER | NO | ID del registro fuente (FK a tabla correspondiente) |
| `source_name` | VARCHAR(50) | NO | openalex\|scopus\|wos\|cvlac\|datos_abiertos |
| `canonical_publication_id` | INTEGER | SÍ | Canon resultante (NULL si rechazado/error) |
| `match_type` | VARCHAR(50) | NO | doi_exact\|fuzzy\|manual_review\|new\|rejected\|error |
| `match_score` | FLOAT | NO | Score 0-100 |
| `decision_reason` | TEXT | SÍ | Explicación de la decisión |
| `matched_against_id` | INTEGER | SÍ | Canon contra el que se comparó (si fuzzy) |
| `algorithm_version` | VARCHAR(50) | NO | "v1.0", "v1.1", ... (para rastreabilidad) |
| `created_at` | TIMESTAMP | NO | |

---

## TIPOS DE DATOS

### Enumeraciones

**`publication_type`:**
```
journal-article, review-article, conference-paper, 
book-chapter, book, report, dataset, preprint, 
monograph, technical-report, working-paper
```

**`oa_status`:**
```
gold, green, hybrid, bronze, closed
```

**`match_type`:**
```
doi_exact, fuzzy, fuzzy_high_confidence, manual_review, 
new, rejected, error
```

**`status` (registros fuente):**
```
pending, reconciled, error, manual_review, rejected
```

**`language` (ISO 639-1):**
```
en, es, fr, pt, de, it, ja, zh, ru, ...
```

---

## Restricciones y Validaciones

### Check Constraints

```sql
-- publication_year debe estar en rango válido
CHECK (publication_year >= 1900 AND publication_year <= 2099)

-- title no puede estar vacío
CHECK (length(title) >= 5)

-- citation_count no puede ser negativo
CHECK (citation_count >= 0)

-- match_score debe estar entre 0 y 100
CHECK (match_score >= 0 AND match_score <= 100)
```

### Foreign Keys

```
publication_authors.publication_id → canonical_publications.id
publication_authors.author_id → authors.id
canonical_publications.journal_id → journals.id
authors.institution_id → institutions.id
*_records.canonical_publication_id → canonical_publications.id
reconciliation_log.canonical_publication_id → canonical_publications.id
```

---

## Ejemplo de Registro Completo

### Publicación Canónica

```json
{
  "id": 42,
  "doi": "10.1038/s41586-023-06234-9",
  "title": "Large language models enable zero-shot clustering of brain signals",
  "normalized_title": "large language models enable zero shot clustering of brain signals",
  "publication_year": 2023,
  "publication_type": "journal-article",
  "journal_id": 5,
  "source_journal": "Nature",
  "issn": "0028-0836",
  "is_open_access": false,
  "oa_status": "closed",
  "citation_count": 127,
  "institutional_authors_count": 2,
  "sources_count": 3,
  "field_provenance": {
    "title": "openalex",
    "doi": "openalex",
    "publication_year": "scopus",
    "authors": "scopus",
    "citation_count": "openalex"
  },
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-03-10T15:45:00Z"
}
```

### Registros Fuente

**OpenAlex Record:**
```json
{
  "id": 123,
  "openalex_work_id": "W4381729405",
  "canonical_publication_id": 42,
  "match_type": "doi_exact",
  "match_score": 100.0,
  "status": "reconciled"
}
```

**Scopus Record:**
```json
{
  "id": 456,
  "scopus_doc_id": "85151169425",
  "canonical_publication_id": 42,
  "match_type": "fuzzy",
  "match_score": 94.5,
  "status": "reconciled"
}
```

---

## Notas Importantes

1. **Campos NULL:** Muchos campos pueden ser NULL por incompletitud de fuentes. Acepta esto como normal.
2. **Provenance (JSONB):** Registra qué fuente aportó cada field para trazabilidad.
3. **Auditoría:** reconciliation_log es inmutable; usala para auditorías.
4. **Escalabilidad:** Índices en FTS y year para queries rápidas con millones de registros.

---

## Evolución Esperada

- **v1 (actual):** Estructura base con field_provenance
- **v2:** Temporal tables para tracking de cambios (versioning)
- **v3:** Sharding por año si escala a >10M publicaciones
