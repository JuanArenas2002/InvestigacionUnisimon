# 🧪 Testing Google Scholar Endpoints - Guía Completa

## ✅ Quick Start

### 1. Iniciar API
```bash
cd c:\Users\juan.arenas1\Desktop\CONVOCATORIA
uvicorn api.main:app --reload --port 8000
```

**Esperado:** `Uvicorn running on http://127.0.0.1:8000`

---

## 📖 Documentación Interactiva

Una vez la API está corriendo:

- **Swagger UI:** http://localhost:8000/docs
  - Interfaz interactiva para probar endpoints
  - "Try it out" en cada endpoint

- **ReDoc:** http://localhost:8000/redoc
  - Documentación legible

- **OpenAPI Schema:** http://localhost:8000/openapi.json
  - Esquema JSON para integración

---

## 🔍 Endpoint de Información

### GET `/api/scholar/test`

**Propósito:** Ver información y ejemplos de cómo usar Google Scholar

```bash
curl -X GET "http://localhost:8000/api/scholar/test" -H "accept: application/json"
```

**Respuesta:**
```json
{
  "status": "ready",
  "service": "Google Scholar Extractor",
  "version": "1.0.0",
  "modo": "Web scraping con scholarly library",
  "uso": {
    "endpoint": "/api/hex/ingest",
    "metodo": "POST",
    "parametros": { ... }
  },
  "scholar_ids_ejemplo": {
    "Gustavo_Aroca_Martinez": "V94aovUAAAAJ",
    "Simon_Bauer": "jzXp-fUAAAAJ"
  }
}
```

---

## 📥 Endpoint de Extracción

### POST `/api/scholar/extract`

Extrae publicaciones de Google Scholar y las guarda (opcionalmente) en BD.

#### **Parámetros de Query:**

```
- scholar_ids: List[str]      → Scholar Profile IDs (URL encoded JSON)
- year_from: int = 2020       → Año inicial
- year_to: int = 2024         → Año final
- max_results: int = 50       → Máximo de resultados por perfil
- dry_run: bool = true        → true=sin guardar, false=guardar en BD
```

---

## 🎯 Ejemplos de Uso

### A) Con cURL (Query Parameters)

#### Extracción simple (dry_run=true)
```bash
curl -X POST "http://localhost:8000/api/scholar/extract?scholar_ids=%5B%22V94aovUAAAAJ%22%5D&year_from=2020&max_results=50&dry_run=true" \
  -H "accept: application/json"
```

#### Con persistencia (dry_run=false)
```bash
curl -X POST "http://localhost:8000/api/scholar/extract?scholar_ids=%5B%22V94aovUAAAAJ%22%5D&year_from=2020&max_results=50&dry_run=false" \
  -H "accept: application/json"
```

#### Múltiples perfiles
```bash
curl -X POST "http://localhost:8000/api/scholar/extract?scholar_ids=%5B%22V94aovUAAAAJ%22%2C%22jzXp-fUAAAAJ%22%5D&year_from=2019&year_to=2024&max_results=100&dry_run=false" \
  -H "accept: application/json"
```

---

### B) Con Python (Requests)

#### Test sin guardar
```python
import requests

response = requests.post(
    "http://localhost:8000/api/scholar/extract",
    params={
        "scholar_ids": ["V94aovUAAAAJ"],
        "year_from": 2020,
        "max_results": 50,
        "dry_run": True
    }
)

print(response.json())
```

#### Con persistencia
```python
response = requests.post(
    "http://localhost:8000/api/scholar/extract",
    params={
        "scholar_ids": ["V94aovUAAAAJ"],
        "year_from": 2020,
        "max_results": 50,
        "dry_run": False  # ← Guardar en BD
    }
)

data = response.json()
print(f"Extraídos: {data['extraidos']}")
print(f"Guardados: {data['guardados']}")
```

#### Script completo de testing
```python
import requests
import json

BASE_URL = "http://localhost:8000"

def test_google_scholar():
    """Test completo de Google Scholar"""
    
    print("🧪 Testing Google Scholar Extraction\n")
    
    # 1. Info del servicio
    print("1️⃣  Obtener información del servicio...")
    resp = requests.get(f"{BASE_URL}/api/scholar/test")
    print(f"   ✅ Status: {resp.status_code}")
    print(f"   Service: {resp.json()['service']}\n")
    
    # 2. Extracción dry-run
    print("2️⃣  Extracción (dry-run, sin guardar)...")
    resp = requests.post(
        f"{BASE_URL}/api/scholar/extract",
        params={
            "scholar_ids": ["V94aovUAAAAJ"],
            "year_from": 2020,
            "max_results": 10,
            "dry_run": True
        }
    )
    print(f"   ✅ Status: {resp.status_code}")
    data = resp.json()
    print(f"   Extraídos: {data['extraidos']} registros\n")
    
    # 3. Extracción con persistencia
    print("3️⃣  Extracción (persistiendo en BD)...")
    resp = requests.post(
        f"{BASE_URL}/api/scholar/extract",
        params={
            "scholar_ids": ["V94aovUAAAAJ"],
            "year_from": 2020,
            "max_results": 10,
            "dry_run": False  # ← Guardar
        }
    )
    print(f"   ✅ Status: {resp.status_code}")
    data = resp.json()
    print(f"   Extraídos: {data['extraidos']}")
    print(f"   Guardados en BD: {data['guardados']}\n")
    
    # 4. Verificar en BD
    print("4️⃣  Verificar datos en BD...")
    print("   SQL: SELECT COUNT(*) FROM google_Scholar_records;")
    print("   (Usa psql o herramienta preferida)\n")
    
    print("✅ Testing completado!")

if __name__ == "__main__":
    test_google_scholar()
```

---

### C) Con Postman

1. **Abrir Postman**

2. **Crear nueva request:**
   - Tipo: `POST`
   - URL: `http://localhost:8000/api/scholar/extract`

3. **Tab "Params":**
   ```
   Key                 Value
   scholar_ids         ["V94aovUAAAAJ"]
   year_from           2020
   max_results         50
   dry_run             true
   ```

4. **Enviar (Send)**

5. **Ver respuesta en el panel inferior**

**Nota:** Para enviar arrays JSON en Postman:
- Parámetro: `scholar_ids`
- Valor: `["V94aovUAAAAJ"]` (con comillas y brackets)

---

## 🔧 Endpoint Principal (Alternativa)

### POST `/api/hex/ingest`

Endpoint hexagonal que soporta **todas** las fuentes incluyendo Google Scholar.

```bash
curl -X POST "http://localhost:8000/api/hex/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["google_Scholar"],
    "scholar_ids": ["V94aovUAAAAJ"],
    "year_from": 2020,
    "max_results": 50,
    "dry_run": false
  }'
```

**Con Python:**
```python
import requests

response = requests.post(
    "http://localhost:8000/api/hex/ingest",
    json={
        "sources": ["google_Scholar"],
        "scholar_ids": ["V94aovUAAAAJ"],
        "year_from": 2020,
        "max_results": 50,
        "dry_run": False
    }
)

print(response.json())
```

---

## 📊 Verificar Resultados en BD

### Una vez guardados los datos:

```sql
-- Ver registros insertados
SELECT COUNT(*) FROM google_Scholar_records;

-- Ver último registro
SELECT * FROM google_Scholar_records ORDER BY created_at DESC LIMIT 1;

-- Ver por perfil
SELECT scholar_profile_id, COUNT(*) as cantidad
FROM google_Scholar_records
GROUP BY scholar_profile_id;

-- Ver por estado
SELECT status, COUNT(*) FROM google_Scholar_records GROUP BY status;

-- Ver más citados
SELECT title, citation_count
FROM google_Scholar_records
ORDER BY citation_count DESC
LIMIT 10;
```

---

## 🧠 Flujo Completo

```
1. Extracción (Google Scholar)
   ↓
   POST /api/scholar/extract
   ↓
2. Persistencia automática
   ↓
   INSERT INTO google_Scholar_records
   ↓
3. Status = "pending"
   ↓
4. Reconciliación (opcional)
   ↓
   POST /api/pipeline/reconcile-all
   ↓
5. Vinculación a canónicas
   ↓
   canonical_publications
```

---

## 🔍 Scholar Profile IDs - Dónde encontrarlos

### Método 1: URL de Google Scholar
Ir a https://scholar.google.com y buscar el investigador:

```
https://scholar.google.com/citations?user=V94aovUAAAAJ&hl=es
                                             ↑↑↑↑↑↑↑↑↑↑↑↑↑
                                        Scholar ID aquí
```

### Método 2: Copiar desde perfil
En la página del perfil → Ver URL → Copiar parte después de `user=`

### Ejemplos comunes:
- `V94aovUAAAAJ` - Gustavo Aroca Martinez
- `jzXp-fUAAAAJ` - Simón Bauer
- Reemplaza con tus investigadores

---

## ⚠️ Errores Comunes

### Error: "Scholar library not installed"
**Solución:**
```bash
pip install scholarly
```

### Error: "Connection refused"
**Solución:** Asegurate que la API está corriendo:
```bash
# En otra terminal:
uvicorn api.main:app --reload
```

### Error: "No se pudo conectar a BD"
**Solución:** Verificar que PostgreSQL está corriendo:
```bash
# Windows:
services.msc → buscar PostgreSQL → iniciar

# Linux:
sudo systemctl start postgresql
```

### Error: "Invalid Scholar ID"
**Solución:** Verificar que el ID sea válido:
- Ir a https://scholar.google.com/citations?user=V94aovUAAAAJ
- Cambiar V94aovUAAAAJ por el ID correcto

---

## 📈 Próximos Pasos Después del Testing

1. **Validar datos extraídos:**
   ```bash
   psql -U usuario -d convocatoria \
     -c "SELECT * FROM google_Scholar_records LIMIT 5;"
   ```

2. **Ejecutar reconciliación:**
   ```bash
   curl -X POST "http://localhost:8000/api/pipeline/reconcile-all"
   ```

3. **Ver coincidencias canónicas:**
   ```bash
   curl -X GET "http://localhost:8000/api/publications?source=google_Scholar"
   ```

4. **Analizar cobertura:**
   ```bash
   curl -X GET "http://localhost:8000/api/stats/sources"
   ```

---

## 📚 Documentación Relacionada

- [GOOGLE_SCHOLAR_ENDPOINTS.md](GOOGLE_SCHOLAR_ENDPOINTS.md) - Todos los endpoints
- [GOOGLE_SCHOLAR_DATABASE_MODEL.md](GOOGLE_SCHOLAR_DATABASE_MODEL.md) - Estructura de BD
- [GOOGLE_SCHOLAR_TESTING.md](GOOGLE_SCHOLAR_TESTING.md) - Tests avanzados

---

## 🎓 Schemas de Ejemplo

### Request: Extracción múltiple
```json
{
  "scholar_ids": ["V94aovUAAAAJ", "jzXp-fUAAAAJ"],
  "year_from": 2019,
  "year_to": 2024,
  "max_results": 100,
  "dry_run": false
}
```

### Response: Éxito
```json
{
  "status": "success",
  "dry_run": false,
  "scholar_ids": ["V94aovUAAAAJ"],
  "extraidos": 8,
  "guardados": 8,
  "registros": [
    {
      "source_name": "google_Scholar",
      "title": "Safety and Efficacy...",
      "authors": [...],
      "publication_year": 2021,
      "citation_count": 250
    }
  ],
  "proximos_pasos": [...]
}
```

---

## 🚀 ¡Listo para Testar!

Abre tu navegador en:
**http://localhost:8000/docs**

¡Prueba los endpoints de Google Scholar directamente desde Swagger UI! 🎉

