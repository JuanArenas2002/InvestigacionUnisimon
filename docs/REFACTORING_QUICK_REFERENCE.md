# Quick Reference: Refactoring Citaciones v2

**Fecha**: 18 de marzo de 2026  
**Estado**: ✅ PRODUCTIVO

---

## TEST RÁPIDO

```bash
# Terminal 1: Iniciar servidor
& ".\venv\Scripts\Activate.ps1"
python -m uvicorn api.main:app --reload

# Terminal 2: Hacer POST al nuevo endpoint
curl -X POST http://localhost:8000/api/authors/charts/v2/author-data \
  -H "Content-Type: application/json" \
  -d '{"author_id": 1}'
```

---

## FILES OVERVIEW

| Archivo | Líneas | Cambio | Descripción |
|---------|--------|--------|-------------|
| `api/services/data_provider.py` | 367 | NEW | Servicio multi-fuente, 6 funciones |
| `api/schemas/charts.py` | +180 | MOD | 6 nuevos schemas Pydantic |
| `api/routers/charts.py` | +130 | MOD | Nuevo endpoint v2 + import |
| `docs/REFACTORING_MULTIFUENTE_CITACIONES.md` | 450 | NEW | Doc completa |

---

## STRUCTURE

```
data_provider.py
├─ YearlyAggregation (dataclass)
├─ AuthorData (dataclass)
├─ _apply_year_filter(records, year_from, year_to)
├─ _extract_author_info(records, author_id)
├─ _aggregate_by_year(records)
├─ _build_publication_dataframe(records)
├─ _calculate_metrics(df, years, pubs, cites)
└─ fetch_author_data(db, author_id, year_from, year_to) ← MAIN
```

---

## ENDPOINT v2

**POST** `/api/authors/charts/v2/author-data`

**Request**:
```json
{
  "author_id": 1,
  "year_from": 2015,
  "year_to": 2025
}
```

**Response** (200 OK):
```json
{
  "success": true,
  "author_id": 1,
  "author_name": "Juan Arenas",
  "source_ids": {
    "scopus": "57193767797",
    "openalex": "A1234567890",
    "wos": "AAH-1234-2022",
    "cvlac": "00123456789"
  },
  "metrics": {
    "total_publications": 62,
    "total_citations": 850,
    "h_index": 15,
    "cpp": 13.7,
    "median_citations": 8.5,
    "percent_cited": 85.5
  },
  "yearly_data": [
    {"year": 2015, "publications": 5, "citations": 120, "cpp": 24.0},
    {"year": 2016, "publications": 6, "citations": 95, "cpp": 15.8}
  ],
  "source_distribution": {
    "scopus": 62,
    "openalex": 58,
    "wos": 45
  }
}
```

---

## VENTAJAS

- ✅ Multi-fuente unificada
- ✅ Sin API calls (BD caché)
- ✅ 6 métricas vs 3 anteriores
- ✅ Serie temporal
- ✅ IDs de múltiples fuentes
- ✅ Código modular (0 duplicación)
- ✅ 100% compatible (v1 sin cambios)

---

## v1 vs v2

| v1 | v2 |
|----|----|
| `/generate` | `/v2/author-data` |
| AU-ID Scopus | author_id local |
| API calls | BD caché |
| Solo Scopus | Multi-source |
| PNG | JSON |
| 3 métricas | 6 métricas + serie |

---

## IMPORTS NUEVOS

```python
# En charts.py router
from api.services.data_provider import fetch_author_data

# En schemas
from typing import Dict  # (agregado)

# En data_provider.py
from dataclasses import dataclass
from db.models import CanonicalPublication, PublicationAuthor, Author
```

---

## PRÓXIMOS PASOS

1. ✅ **DONE**: Endpoint v2 con datos desde BD
2. ⏳ **TODO**: Tests unitarios para `data_provider.py`
3. ⏳ **TODO**: Modo chart generation con v2 (sin ScopusExtractor)
4. ⏳ **TODO**: Endpoint mapping IDs (AU-ID → author_id)
5. ⏳ **TODO**: Reportes HTML/PDF con datos v2

---

## REFERENCES

- Documentación: `docs/REFACTORING_MULTIFUENTE_CITACIONES.md`
- Servicio: `api/services/data_provider.py`
- Endpoint: `api/routers/charts.py` (línea ~580)
- Schemas: `api/schemas/charts.py` (línea ~180+)
