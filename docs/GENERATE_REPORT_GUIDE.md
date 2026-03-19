# Guía de Uso: Endpoint `/v2/generate-report`

## Resumen

El nuevo endpoint **`POST /api/authors/charts/v2/generate-report`** genera un **informe bibliométrico completo** con:

- 📊 **PNG profesional**: gráfico limpio sin sección de notas
- 📄 **PDF detallado**: informe con KPIs, análisis positivos/negativos, y **notas completas sin restricciones de espacio**

## Problema Resuelto

**Antes:** Las notas en el PNG tenían superposición de emojis y texto debido al espacio limitado.

**Ahora:** 
- PNG queda **limpio y profesional** 
- PDF contiene **todo el análisis detallado** con espacio ilimitado para notas

## Endpoint

```
POST /api/authors/charts/v2/generate-report
```

## Parámetros

| Parámetro | Tipo | Requerido | Descripción |
|-----------|------|-----------|-------------|
| `author_id` | integer | ✅ Sí | ID del autor en tabla `authors` (BD local) |
| `year_from` | integer | ❌ No | Año inicial para filtrar (default: sin filtro) |
| `year_to` | integer | ❌ No | Año final para filtrar (default: sin filtro) |
| `institution_name` | string | ❌ No | Nombre para pies de página (default: "Universidad Simón Bolívar") |
| `campo` | string | ❌ No | Campo disciplinar para umbrales (default: "CIENCIAS_SALUD") |

**Campos disponibles:**
- `CIENCIAS_SALUD`
- `CIENCIAS_BASICAS`
- `INGENIERIA`
- `CIENCIAS_SOCIALES`
- `ARTES_HUMANIDADES`

## Ejemplos de Uso

### 1. Uso Mínimo

```bash
curl -X POST http://localhost:8000/api/authors/charts/v2/generate-report \
  -H "Content-Type: application/json" \
  -d '{
    "author_id": 1
  }'
```

### 2. Con Rango de Años

```bash
curl -X POST http://localhost:8000/api/authors/charts/v2/generate-report \
  -H "Content-Type: application/json" \
  -d '{
    "author_id": 42,
    "year_from": 2018,
    "year_to": 2024
  }'
```

### 3. Completo (con todos los parámetros)

```bash
curl -X POST http://localhost:8000/api/authors/charts/v2/generate-report \
  -H "Content-Type: application/json" \
  -d '{
    "author_id": 7,
    "year_from": 2015,
    "year_to": 2025,
    "institution_name": "Universidad Simón Bolívar",
    "campo": "CIENCIAS_BASICAS"
  }'
```

### 4. Con Python (Requests)

```python
import requests
import json

url = "http://localhost:8000/api/authors/charts/v2/generate-report"

payload = {
    "author_id": 1,
    "year_from": 2018,
    "year_to": 2024,
    "institution_name": "USB",
    "campo": "CIENCIAS_SALUD"
}

response = requests.post(url, json=payload)
result = response.json()

print(f"PNG: {result['filename']}")
print(f"Tamaño PNG: {result['file_size_mb']} MB")
print(f"Investigador: {result['investigator_name']}")
print(f"KPIs: {result['metrics']}")
```

## Respuesta

```json
{
  "success": true,
  "investigator_name": "Dr. Juan Pérez",
  "filename": "grafico_autor_1_20250317_145630.png",
  "file_path": "reports/charts/grafico_autor_1_20250317_145630.png",
  "file_size_mb": 0.25,
  "metrics": {
    "total_publications": 28,
    "total_citations": 156,
    "h_index": 7,
    "cpp": 5.57,
    "median_citations": 3,
    "percent_cited": 92.86
  },
  "year_range": "2012-2025"
}
```

## Archivos Generados

Cuando se ejecuta el endpoint, se crean **dos archivos**:

### PNG
- **Ubicación:** `reports/charts/grafico_autor_1_<timestamp>.png`
- **Contenido:** Gráfico profesional con:
  - Nombre del investigador
  - Tabla de KPIs (6 indicadores)
  - Gráfico de series temporales
  - Análisis automático (hallazgos positivos/negativos)
  - Footer con institución y fecha
- **Características:** Limpio, sin notas, optimizado para presentaciones

### PDF
- **Ubicación:** `reports/pdfs/informe_<slug>_<timestamp>.pdf`
- **Contenido:** Informe detallado con:
  - Encabezado (investigador, institución, fechas)
  - Tabla de KPIs formateada
  - ✓ Hallazgos Positivos
  - ⚠ Aspectos a Mejorar
  - 📌 **Notas Aclaratorias** (SIN RESTRICCIÓN DE ESPACIO)
  - Footer profesional
- **Características:** Impresión lista, con todo el espacio necesario

## Flujo de Generación

```
POST /v2/generate-report
        ↓
   Validar author_id
        ↓
   fetch_author_data() ← BD
        ↓
   generar_hallazgos() → (positivos, negativos, notas)
        ↓
   [PARALELO]
   ├─→ render_author_chart() → PNG (sin notas)
   └─→ generate_analysis_report() → PDF (con todas las notas)
        ↓
   Retornar respuesta JSON
```

## Descargar Archivos

Los archivos generados están en:

```
project-root/
├── reports/
│   ├── charts/
│   │   └── grafico_autor_1_20250317_145630.png
│   └── pdfs/
│       └── informe_dr_juan_perez_20250317_145630.pdf
```

Para descargar el PNG desde la API, usar:

```bash
GET /api/authors/charts/download/{filename}
```

*(Endpoint de descarga — ver `charts.py` para detalles)*

## Diferencias con `/v2/generate-chart`

| Aspecto | `/v2/generate-chart` | `/v2/generate-report` |
|---------|---------------------|----------------------|
| PNG | ✅ Sí | ✅ Sí |
| PDF | ❌ No | ✅ Sí |
| Notas en PNG | ✅ (intenta) | ❌ No (limpio) |
| Notas en PDF | ❌ N/A | ✅ Sí (completas) |
| Análisis automático | ❌ No | ✅ Sí |
| Respuesta | Solo PNG info | PNG + PDF info |

## Errores Comunes

### 404 - Autor no encontrado

```json
{
  "success": false,
  "error": "Autor con ID 999 no encontrado en la base de datos"
}
```

**Solución:** Verificar que el `author_id` existe en tabla `authors`:

```sql
SELECT id, name FROM authors WHERE id = 999;
```

### 400 - Campo disciplinar inválido

```json
{
  "success": false,
  "error": "Campo disciplinar 'INVALIDO' no es válido"
}
```

**Solución:** Usar uno de los campos válidos:
- `CIENCIAS_SALUD`
- `CIENCIAS_BASICAS`
- `INGENIERIA`
- `CIENCIAS_SOCIALES`
- `ARTES_HUMANIDADES`

### 500 - Error en reportlab

```
reportlab not installed
```

**Solución:**
```bash
pip install reportlab
```

## Configuración

### Directorios de Salida

Se crean automáticamente si no existen:

```python
# PNG
reports/charts/

# PDF
reports/pdfs/
```

### Estilos PDF

El PDF usa:
- **Fuente:** Helvetica (estándar)
- **Colores:** Azul (#2563EB) para títulos, gris oscuro (#374151) para texto
- **Tamaño** A4 (carta)
- **Márgenes:** 0.5" × 0.75"

## Performance

- **Tiempo total:** ~2-5 segundos (según complejidad de datos)
- **Tamaño PNG:** 200-400 KB
- **Tamaño PDF:** 100-200 KB
- **Operaciones BD:** 3-4 queries

## Notas Técnicas

### Sobre las Notas

Las **notas** son observaciones aclaratorias generadas automáticamente:

```python
notas = [
    "📌 Tendencia alcista en últimos 3 años",
    "📌 Concentración de publicaciones en 2023",
    "📌 Colaboraciones internacionales detectadas",
]
```

En el PNG: **Se omiten** para mantener profesionalismo
En el PDF: **Se incluyen todas** con espacio ilimitado

### Análisis Automático

El sistema genera automáticamente:

**Positivos:**
- H-index consolidado (≥ umbral del campo)
- Citaciones uniformes
- Publicaciones consistentes
- etc.

**Negativos:**
- Baja actividad reciente
- Pocas citaciones
- Publicaciones esporádicas
- etc.

## Integración con Otros Servicios

```python
# Usar directamente en código
from api.services.pdf_reporter import generate_analysis_report
from api.services.graph_renderer import render_author_chart

# Ya automatizado en el endpoint ✅
```

## FAQ

**P: ¿Por qué no aparecen las notas en el PNG?**
R: Por diseño. El PNG es limpio para presentaciones ejecutivas. El PDF es para el análisis detallado.

**P: ¿Puedo descargar ambos al mismo tiempo?**
R: Sí, se generan automáticamente. El endpoint devuelve info del PNG. El PDF se genera en paralelo.

**P: ¿Se pueden modificar los colores/estilos del PDF?**
R: Sí, editando `api/services/pdf_reporter.py` en las secciones de `ParagraphStyle`.

**P: ¿Hay límite de tamaño de notas?**
R: No. El PDF automáticamente se expande en páginas según sea necesario.

## Siguiente: Descarga de Archivos

Ver [/api/authors/charts/download/{filename}](#) para descargar los archivos generados.

---

**Última actualización:** 2025-03-17
**Versión API:** 2.0
**Estado:** ✅ Producción
