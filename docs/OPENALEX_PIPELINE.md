# Procesos de OpenAlex en el Pipeline

Documentación específica de los endpoints y utilidades relacionados con **OpenAlex** dentro del sistema. Separado del resto del pipeline por su complejidad y posibles cambios independientes.

---

## 1. `/api/pipeline/extract/openalex`
**Método:** POST  
**Descripción:** Extrae publicaciones desde OpenAlex usando la librería **PyAlex**, las ingesta en la tabla `openalex_records` y reconcilia automáticamente contra las publicaciones canónicas.  
**Body:**
```json
{
  "year_from": 2020,
  "year_to": 2025,
  "max_results": 1000
}
```
**Respuesta:**
- `extracted`: registros extraídos
- `inserted`: registros nuevos (no duplicados)
- `reconciliation`: resumen de la reconciliación

---

## 2. `/api/search/openalex`
**Método:** GET  
**Descripción:** Búsqueda directa en OpenAlex por título, autor, ISSN, etc. Usa PyAlex internamente (no hace llamadas HTTP directas).  
**Parámetros:** `q` (query de texto), filtros opcionales de año, tipo, etc.

---

## 3. Clase `OpenAlexExtractor` (`extractors/openalex.py`)

Extractor principal que consume la API de OpenAlex vía **PyAlex**.

| Parámetro | Descripción |
|---|---|
| `ror_id` | ROR de la institución (ej. `https://ror.org/xxxxx`) |
| `email` | Email para el polite pool de OpenAlex |
| `max_results` | Límite de resultados (default: 10 000) |

**Configuración PyAlex:**
```python
pyalex.config.email               = email
pyalex.config.max_retries         = 3
pyalex.config.retry_backoff_factor = 0.5
pyalex.config.retry_http_codes    = [429, 500, 503]
```

**Método principal:**
```python
records = extractor.extract(year_from=2020, year_to=2025)
```

**Búsqueda por DOI:**
```python
record = extractor.search_by_doi("10.1234/abcd")
```

---

## 4. Clase `OpenAlexEnricher` (`extractors/openalex.py`)

Enriquece un Excel propio (con columnas `Título`, `Año`, `doi`) con metadatos de OpenAlex.

**Estrategias:**
1. **Batch por DOI** — hasta 50 DOIs por request vía `Works().filter(doi=[...]).get()`
2. **Búsqueda fuzzy por título + año** — RapidFuzz ≥ 80 % de similitud, como fallback

**Columnas añadidas al Excel:**

| Columna | Descripción |
|---|---|
| `oa_encontrado` | `true`/`false` |
| `oa_metodo` | `doi_batch`, `title_fuzzy` o vacío |
| `oa_work_id` | ID OpenAlex (`W1234567`) |
| `oa_titulo` | Título en OpenAlex |
| `oa_año` | Año de publicación |
| `oa_doi` | DOI normalizado |
| `oa_tipo` | Tipo de publicación |
| `oa_revista` | Nombre de la revista/fuente |
| `oa_issn` | ISSN-L de la fuente |
| `oa_editorial` | Editorial |
| `oa_open_access` | `true`/`false` |
| `oa_status_oa` | Estado de acceso abierto |
| `oa_citas` | Número de citas |
| `oa_idioma` | Idioma de la publicación |
| `oa_url` | URL de acceso |
| `oa_autores` | Autores (separados por `;`) |

**Uso desde código:**
```python
from extractors.openalex import OpenAlexEnricher

enricher = OpenAlexEnricher(email="mi@correo.edu.co")
df = enricher.enrich_from_excel("mis_publicaciones.xlsx")
enricher.save_to_excel(df, "resultado.xlsx")
```

---

## 5. DOI lookup de rescate (pipeline interno)

Cuando una publicación tiene DOI pero no está en la tabla `openalex_records`, el pipeline busca automáticamente su revista/ISSN en OpenAlex:

```python
work = Works()[f"https://doi.org/{doi_key}"]
source = work.get("primary_location", {}).get("source", {})
oa_issn    = source.get("issn_l") or source.get("issn", [""])[0]
oa_journal = source.get("display_name")
```

Se ejecuta en [`api/routers/pipeline.py`](../api/routers/pipeline.py) y [`api/routers/_pipeline_helpers.py`](../api/routers/_pipeline_helpers.py).

---

## Notas

- **Todas las llamadas a OpenAlex** usan PyAlex (`pyalex >= 0.21`). No hay llamadas HTTP directas con `requests`.
- PyAlex gestiona internamente el rate-limiting, los reintentos y el polite pool (mediante `pyalex.config.email`).
- Para ver los demás endpoints del pipeline, ver [ENDPOINTS_PIPELINE.md](ENDPOINTS_PIPELINE.md).
