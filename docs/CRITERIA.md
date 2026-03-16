# Criterios de Inclusión y Exclusión de Registros Bibliográficos

> Versión 1.0 — 13 de marzo de 2026
> Documento maestro que define reglas de aceptación/rechazo en pipeline de reconciliación

---

## 1. Propósito

Este documento formaliza los criterios de calidad para determinar qué registros bibliográficos se incluyen en el conjunto final unificado y cuáles se excluyen o marcan para revisión manual.

---

## 2. Criterios de INCLUSIÓN (Registro Aceptado)

Un registro **DEBE CUMPLIR** todos los siguientes criterios para ser incluido:

### 2.1 Campos Obligatorios

| Campo | Requisito |
|-------|-----------|
| `title` | Presente y no vacío. Longitud mín: 5 caracteres |
| `publication_year` | Año válido (1900-2099) |
| `source_name` | Una de: `openalex`, `scopus`, `wos`, `cvlac`, `datos_abiertos` |

**⚠️ Regla:** Si alguno falta → status='pending_review' (no se rechaza, se marca para revisión)

### 2.2 Identificadores

**Al menos uno de:**
- `doi` (normalizado, válido)
- `pmid` (número válido)
- `pmcid` (número válido)

**ℹ️ Nota:** Registros sin identificador se reconcilian por fuzzy. Si no hay coincidencia fuzzy → nueva canonical.

### 2.3 Cobertura Temática

- ✅ Aceptado: Cualquier tipo de publicación (artículo, review, proceeding, etc.)
- ✅ Aceptado: Cualquier idioma
- ❌ Rechazado: Registros claramente no científicos (ej: "Teléfono de emergencia", "Horario de buses")

**Detección:** Si `title` contiene palabras de "lista negra" (TBD) → excluir

### 2.4 Cobertura Temporal

- ✅ Aceptado: Publicaciones entre 1900 y año actual (2026)
- ⚠️ Revisión: Publicaciones futuras (año > 2026) → marcar para revisión
- ❌ Rechazado: Año inválido o nulo (si además falta identificador)

### 2.5 Completitud de Metadatos

**Puntuación de completitud:** Cuenta campos completados

| Rango | Status |
|-------|--------|
| ≥ 70% campos | ✅ Aceptado |
| 50-69% campos | ⚠️ Pending Review |
| < 50% campos | ❌ Rechazado |

**Campos contados:** title, year, doi, authors, journal, issn, publication_type, language, oa_status

---

## 3. Criterios de EXCLUSIÓN (Registro Rechazado)

Un registro se **RECHAZA** si:

### 3.1 Defectos Críticos

| Condición | Acción | Motivo |
|-----------|--------|--------|
| Título vacío o < 5 caracteres | ❌ Excluir | Imposible hacer fuzzy matching |
| Titulo = Titulo: | ❌ Excluir (probable corrupción) | Datos inválidos |
| Año = 0 o NULL (sin DOI) | ❌ Excluir | Ambigüedad temporal |
| author_count = 0 AND no DOI | ⚠️ Revisión | Difícil de desambiguar |

### 3.2 Datos Corrompidos

| Patrón | Acción | Ejemplo |
|--------|--------|---------|
| Títulos > 500 caracteres | ❌ Excluir | Probable XML embebido |
| DOI con caracteres inválidos | ❌ Excluir | "10.xxxX/!!!!" |
| ISSN < 8 caracteres (sin normalizar) | ⚠️ Revisión | "123-45" |
| Email como título | ❌ Excluir | Probable scraping error |

### 3.3 Contenido No Científico (Lista Negra)

Excluir si título contiene:
- "404", "error", "not found"
- "confidencial", "borrador", "draft"
- Números de teléfono principales
- URLs como título
- Cadenass repetidas ("aaaaa", "xxx xxx")

---

## 4. Criterios de REVISIÓN MANUAL (Pending Review)

Un registro se marca **Para revisión manual** si:

### 4.1 Ambigüedad de Duplicados

- Fuzzy matching score entre 0.70 y 0.80
- Múltiples candidatos con scores altos
- Conflicto de DOI (mismo DOI en 2 canonical_publications)

### 4.2 Inconsistencias de Metadatos

- Año de publicación discrepa entre fuentes (diff > 2 años)
- Título muy diferente en Wikipedia vs OpenAlex
- Author count muy diferente (10 en Scopus vs 1 en OpenAlex)

### 4.3 Registros Huérfanos

- Registro con identificador único (DOI) pero sin fuente corroborante
- Publicado en revista "desconocida" (no en ISSN database)

---

## 5. Algoritmo de Decisión

```
REGISTRO NUEVO
├─ ¿Campos obligatorios completos? (title, year, source)
│  ├─ NO → status='rejected_incomplete'
│  └─ SÍ → continuar
├─ ¿Pasa validación de datos corrompidos?
│  ├─ NO → status='rejected_corrupted'
│  └─ SÍ → continuar
├─ ¿Contiene palabras de lista negra?
│  ├─ SÍ → status='rejected_non_scientific'
│  └─ NO → continuar
├─ Completitud de metadatos
│  ├─ >= 70% → status='accepted'
│  ├─ 50-69% → status='pending_review'
│  └─ < 50% → status='rejected_incomplete'
└─ FIN
```

---

## 6. Configuración en código

```python
# config.py - CriteriaConfig

@dataclass
class CriteriaConfig:
    """Criterios de inclusión/exclusión"""
    
    # Campos obligatorios
    min_title_length: int = 5
    max_title_length: int = 500
    min_year: int = 1900
    max_year: int = 2099
    
    # Completitud
    min_completeness_accepted: float = 0.70      # 70%
    min_completeness_review: float = 0.50        # 50%
    
    # Fuzzy matching (reconciliación)
    fuzzy_auto_accept: float = 0.85              # >= 85% = aceptar automático
    fuzzy_manual_review: float = 0.70            # 70-85% = revisar manualmente
    
    # Año
    year_tolerance: int = 2                      # Tolerancia para divergencias
    
    # Palabras prohibidas en título
    blacklist_keywords: list[str] = field(default_factory=lambda: [
        "404", "error", "not found", "confidencial", 
        "borrador", "draft", "untitled"
    ])
    
    # Listas de validación
    valid_sources: list[str] = field(default_factory=lambda: [
        "openalex", "scopus", "wos", "cvlac", "datos_abiertos"
    ])
    valid_publication_types: list[str] = field(default_factory=lambda: [
        "journal-article", "conference-proceeding", "book-chapter",
        "book", "report", "review", "dataset", "preprint"
    ])
```

---

## 7. Aplicación en Pipeline

### Fase: LIMPIEZA (scripts/clean_data.py)
1. Valida criterios de inclusión/exclusión
2. Marca registros para revisión
3. Genera reporte de rechazo

### Fase: RECONCILIACIÓN (reconciliation/engine.py)
1. Procesa `status='accepted'` automáticamente
2. Maneja `status='pending_review'` con fuzzy + manual review
3. Rechaza `status='rejected_*'`

### Fase: CONSULTA (api/)
- GET /publications → solo regresar `status IN ('accepted', 'reconciled')`
- GET /external-records?status=pending_review → registro pendiente
- GET /quality/rejected-records → auditoría de rechazos

---

## 8. Evolución del Criterio

Actualizar este documento cuando:
- Se descubran nuevos patrones de corrupción
- Se añadan nuevas fuentes
- Cambien los objetivos del análisis (ej: solo artículos, solo inglés)

**Versioning:** CRITERIA_v{N}.md cuando cambios mayores.

---

## 9. Referencias

- [DDD: Domain Rules](https://martinfowler.com/bliki/BoundedContext.html)
- [Data Quality Dimensions](https://en.wikipedia.org/wiki/Data_quality#Dimensions)
- Reconciliation thresholds: [`reconciliation/fuzzy_matcher.py`](../reconciliation/fuzzy_matcher.py)
