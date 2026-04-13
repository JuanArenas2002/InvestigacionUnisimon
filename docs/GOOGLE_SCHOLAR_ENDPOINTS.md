# 🎓 Google Scholar - API Endpoints

## 📚 Documentación de Endpoints para Extracción de Google Scholar

---

## **Endpoint Principal: POST `/ingest`**

### **Descripción**
Ejecuta el pipeline ETL (Extract-Transform-Load) usando Google Scholar como fuente de datos.

### **URL**
```
POST http://localhost:8000/ingest
```

### **Request Body (JSON)**

```json
{
  "sources": ["google_scholar"],
  "scholar_ids": ["V94aovUAAAAJ"],
  "year_from": 2020,
  "year_to": 2025,
  "max_results": 50,
  "dry_run": false
}
```

### **Parámetros**

| Parámetro | Tipo | Requerido | Descripción |
|-----------|------|----------|-------------|
| `sources` | `string[]` | Sí | Lista de fuentes. Para Google Scholar: `["google_scholar"]` |
| `scholar_ids` | `string[]` | **Sí** | IDs de perfiles Google Scholar (ej: `["V94aovUAAAAJ"]`) |
| `year_from` | `integer` | No | Año inicial (inclusive). Default: sin límite |
| `year_to` | `integer` | No | Año final (inclusive). Default: sin límite |
| `max_results` | `integer` | No | Máximo de registros a extraer. Default: 100 |
| `dry_run` | `boolean` | No | Si `true`, no guarda en BD. Default: `false` |

### **Response (200 OK)**

```json
{
  "status": "ok",
  "selected_sources": ["google_scholar"],
  "stages": {
    "collect": 10,
    "deduplicate": 10,
    "normalize": 10,
    "match": 10,
    "enrich": 10
  },
  "persistence": {
    "authors_saved": 25,
    "source_saved": 10,
    "canonical_upserted": 8,
    "dry_run": false
  },
  "by_source": {
    "google_scholar": 10
  },
  "errors": {}
}
```

### **Response Fields**

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `status` | `string` | Estado de la ejecución: `"ok"` |
| `selected_sources` | `string[]` | Fuentes procesadas |
| `stages.collect` | `int` | Registros colectados |
| `stages.deduplicate` | `int` | Registros únicos |
| `stages.normalize` | `int` | Registros normalizados |
| `stages.match` | `int` | Registros con matching |
| `stages.enrich` | `int` | Registros enriquecidos |
| `persistence.authors_saved` | `int` | Autores guardados en BD |
| `persistence.source_saved` | `int` | Registros fuente guardados |
| `persistence.canonical_upserted` | `int` | Publicaciones canónicas creadas |
| `by_source` | `object` | Conteo por fuente |
| `errors` | `object` | Errores durante ejecución |

---

## 📝 **Ejemplos de Uso**

### **1. cURL - Extracción Simple**

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["google_scholar"],
    "scholar_ids": ["V94aovUAAAAJ"],
    "max_results": 10,
    "dry_run": true
  }'
```

### **2. cURL - Con Filtros de Año**

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["google_scholar"],
    "scholar_ids": ["V94aovUAAAAJ"],
    "year_from": 2022,
    "year_to": 2024,
    "max_results": 50
  }'
```

### **3. cURL - Múltiples Scholar IDs**

```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["google_scholar"],
    "scholar_ids": ["V94aovUAAAAJ", "_xxTOIEAAAAJ", "jc0B6ZUAAAAJ"],
    "max_results": 100
  }'
```

### **4. Python - Con requests**

```python
import requests
import json

url = "http://localhost:8000/ingest"

payload = {
    "sources": ["google_scholar"],
    "scholar_ids": ["V94aovUAAAAJ"],
    "year_from": 2020,
    "year_to": 2025,
    "max_results": 50,
    "dry_run": False
}

headers = {"Content-Type": "application/json"}

response = requests.post(url, json=payload, headers=headers)
result = response.json()

print(f"Status: {result['status']}")
print(f"Recolectados: {result['stages']['collect']}")
print(f"Normalizados: {result['stages']['normalize']}")
print(f"Canonicalizados: {result['persistence']['canonical_upserted']}")
print(f"Errores: {result['errors']}")
```

### **5. Python - Script Completo**

```python
#!/usr/bin/env python
"""Extracción de Google Scholar vía API"""

import requests
import json
import time

BASE_URL = "http://localhost:8000/ingest"

def extract_google_scholar(scholar_ids, year_from=None, year_to=None, max_results=50):
    """
    Extrae publicaciones de Google Scholar via API.
    
    Args:
        scholar_ids: Lista de Scholar IDs (ej: ["V94aovUAAAAJ"])
        year_from: Año inicial (optional)
        year_to: Año final (optional)
        max_results: Máximo de registros
    
    Returns:
        dict con resultado de la extracción
    """
    
    payload = {
        "sources": ["google_scholar"],
        "scholar_ids": scholar_ids,
        "max_results": max_results,
    }
    
    if year_from:
        payload["year_from"] = year_from
    if year_to:
        payload["year_to"] = year_to
    
    print(f"\n🔍 Extrayendo de Scholar IDs: {scholar_ids}")
    print(f"   Parámetros: {payload}\n")
    
    try:
        start = time.time()
        response = requests.post(BASE_URL, json=payload, timeout=300)
        elapsed = time.time() - start
        
        if response.status_code != 200:
            print(f"❌ Error HTTP {response.status_code}")
            print(response.text)
            return None
        
        result = response.json()
        
        print(f"✅ Extracción completada en {elapsed:.2f}s")
        print(f"\n📊 Resultados:")
        print(f"   Recolectados: {result['stages']['collect']}")
        print(f"   Deduplicados: {result['stages']['deduplicate']}")
        print(f"   Normalizados: {result['stages']['normalize']}")
        print(f"   Coincidencias: {result['stages']['match']}")
        print(f"   Enriquecidos: {result['stages']['enrich']}")
        
        print(f"\n💾 Persistencia:")
        print(f"   Autores guardados: {result['persistence']['authors_saved']}")
        print(f"   Registros fuente: {result['persistence']['source_saved']}")
        print(f"   Canónicas creadas: {result['persistence']['canonical_upserted']}")
        
        if result['errors']:
            print(f"\n⚠️  Errores:")
            for source, error in result['errors'].items():
                print(f"   {source}: {error}")
        
        return result
        
    except requests.exceptions.ConnectionError:
        print("❌ No se pudo conectar a la API")
        print("   ¿Está corriendo: uvicorn api.main:app --reload?")
        return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    # Ejemplo 1: Un scholar
    result1 = extract_google_scholar(
        scholar_ids=["V94aovUAAAAJ"],
        year_from=2020,
        max_results=10
    )
    
    # Ejemplo 2: Múltiples scholars
    result2 = extract_google_scholar(
        scholar_ids=["V94aovUAAAAJ", "_xxTOIEAAAAJ"],
        max_results=20
    )
```

### **6. JavaScript/Fetch**

```javascript
const BASE_URL = "http://localhost:8000/ingest";

async function extractGoogleScholar(scholarIds, options = {}) {
  const payload = {
    sources: ["google_scholar"],
    scholar_ids: scholarIds,
    year_from: options.yearFrom || null,
    year_to: options.yearTo || null,
    max_results: options.maxResults || 50,
    dry_run: options.dryRun || false,
  };

  try {
    const response = await fetch(BASE_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const result = await response.json();

    console.log("✅ Extracción completada:");
    console.log(`   Recolectados: ${result.stages.collect}`);
    console.log(`   Normalizados: ${result.stages.normalize}`);
    console.log(`   Canónicas: ${result.persistence.canonical_upserted}`);

    return result;
  } catch (error) {
    console.error("❌ Error:", error);
  }
}

// Uso
fetchGoogleScholar(["V94aovUAAAAJ"], {
  yearFrom: 2020,
  yearTo: 2025,
  maxResults: 50,
});
```

### **7. Postman Collection (JSON)**

```json
{
  "info": {
    "name": "Google Scholar Extraction",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "item": [
    {
      "name": "Extract Single Scholar",
      "request": {
        "method": "POST",
        "header": [
          {
            "key": "Content-Type",
            "value": "application/json"
          }
        ],
        "body": {
          "mode": "raw",
          "raw": "{\n  \"sources\": [\"google_scholar\"],\n  \"scholar_ids\": [\"V94aovUAAAAJ\"],\n  \"year_from\": 2020,\n  \"max_results\": 50\n}"
        },
        "url": {
          "raw": "http://localhost:8000/ingest",
          "protocol": "http",
          "host": ["localhost"],
          "port": "8000",
          "path": ["ingest"]
        }
      }
    },
    {
      "name": "Extract Multiple Scholars",
      "request": {
        "method": "POST",
        "header": [
          {
            "key": "Content-Type",
            "value": "application/json"
          }
        ],
        "body": {
          "mode": "raw",
          "raw": "{\n  \"sources\": [\"google_scholar\"],\n  \"scholar_ids\": [\"V94aovUAAAAJ\", \"_xxTOIEAAAAJ\"],\n  \"year_from\": 2022,\n  \"max_results\": 100\n}"
        },
        "url": {
          "raw": "http://localhost:8000/ingest",
          "protocol": "http",
          "host": ["localhost"],
          "port": "8000",
          "path": ["ingest"]
        }
      }
    }
  ]
}
```

---

## 🔍 **Cómo Obtener Scholar IDs**

### **Método 1: Desde Google Scholar**
1. Ve a [https://scholar.google.com/](https://scholar.google.com/)
2. Busca un autor o tu perfil
3. Abre el perfil público
4. La URL será: `https://scholar.google.com/citations?user=**V94aovUAAAAJ**`
5. El Scholar ID es: `V94aovUAAAAJ`

### **Método 2: Vía API Scholar Search**
```python
from scholarly import scholarly

# Buscar autor
search_query = scholarly.search_author("Juan Arenas")
author = scholarly.fill(next(search_query))

scholar_id = author["scholar_id"]
print(f"Scholar ID: {scholar_id}")
```

### **Método 3: Base de Datos Local**
Si tienes una tabla de `researchers`:
```sql
SELECT scholar_id FROM researchers WHERE scholar_id IS NOT NULL;
```

---

## ⚠️ **Manejo de Errores**

### **Error 400: Scholar ID Inválido**
```json
{
  "detail": "Scholar ID no encontrado o perfil privado"
}
```

**Solución:**
- Verifica que el Scholar ID sea correcto
- Asegúrate que el perfil sea público
- Prueba con: `python quick_test_scholar_id.py`

### **Error 429: Rate Limiting**
```json
{
  "detail": "Google Scholar aplicó rate-limiting (demasiadas solicitudes)"
}
```

**Solución:**
- Espera 1-2 horas
- Usa `max_results` menor
- Implementa proxy (ver documentación avanzada)

### **Error 500: Base de Datos**
```json
{
  "detail": "Error guardando registros en BD"
}
```

**Solución:**
- Usa `"dry_run": true` para debugging
- Verifica conexión a PostgreSQL
- Revisa los logs de la API

---

## 🚀 **Casos de Uso Avanzados**

### **Extracción Programada (Cron)**

```python
# scheduler.py
import schedule
import time
import requests

def scheduled_extraction():
    """Ejecuta extracción cada semana"""
    scholar_ids = [
        "V94aovUAAAAJ",
        "_xxTOIEAAAAJ",
        "jc0B6ZUAAAAJ"
    ]
    
    payload = {
        "sources": ["google_scholar"],
        "scholar_ids": scholar_ids,
        "max_results": 100
    }
    
    response = requests.post("http://localhost:8000/ingest", json=payload)
    print(f"Extracción completada: {response.status_code}")

# Ejecutar cada lunes a las 9 AM
schedule.every().monday.at("09:00").do(scheduled_extraction)

while True:
    schedule.run_pending()
    time.sleep(60)
```

### **Extracción por Institución**

```python
# Obtener todos los Scholar IDs de una institución
scholar_ids = [
    "V94aovUAAAAJ",  # Researcher 1
    "jc0B6ZUAAAAJ",  # Researcher 2
    "_xxTOIEAAAAJ",  # Researcher 3
]

# Extraer juntos
payload = {
    "sources": ["google_scholar"],
    "scholar_ids": scholar_ids,
    "year_from": 2020,
    "max_results": 500
}

response = requests.post("http://localhost:8000/ingest", json=payload)
```

### **Monitoreo de Progreso**

```python
# Usar dry_run=true primero para estimar tiempo
dry_run = {
    "sources": ["google_scholar"],
    "scholar_ids": ["V94aovUAAAAJ"],
    "max_results": 100,
    "dry_run": True
}

response = requests.post("http://localhost:8000/ingest", json=dry_run)
dry_result = response.json()

print(f"Registros a procesar: {dry_result['stages']['collect']}")
print(f"Tiempo estimado: ~{dry_result['stages']['collect'] * 0.5}s")

# Luego ejecutar real
dry_run["dry_run"] = False
real_response = requests.post("http://localhost:8000/ingest", json=dry_run)
```

---

## 📊 **Respuestas Esperadas**

### **Extracción Exitosa (1 Scholar)**
```json
{
  "status": "ok",
  "selected_sources": ["google_scholar"],
  "stages": {
    "collect": 15,
    "deduplicate": 15,
    "normalize": 15,
    "match": 12,
    "enrich": 12
  },
  "persistence": {
    "authors_saved": 45,
    "source_saved": 15,
    "canonical_upserted": 10,
    "dry_run": false
  },
  "by_source": {
    "google_scholar": 15
  },
  "errors": {}
}
```

---

## 🔧 **Verificación Rápida**

```bash
# 1. Verificar que el API está corriendo
curl http://localhost:8000/docs

# 2. Verificar Scholar ID válido
python quick_test_scholar_id.py

# 3. Prueba de extracción vía API
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"sources":["google_scholar"],"scholar_ids":["V94aovUAAAAJ"],"dry_run":true}'

# 4. Ver logs de la API
# (En la terminal donde corre uvicorn)
```

---

## 📚 **Referencias**

- [Documentación de Endpoints: /docs](http://localhost:8000/docs)
- [Google Scholar Extractor Testing](../GOOGLE_SCHOLAR_TESTING.md)
- [Arquitectura: GOOGLE_SCHOLAR_ARCHITECTURE.py](../GOOGLE_SCHOLAR_ARCHITECTURE.py)
- [Google Scholar](https://scholar.google.com/)

