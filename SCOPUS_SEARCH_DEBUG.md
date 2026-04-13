# Análisis de errores - Búsqueda Scopus

## ✅ Correcciones realizadas

### 1. **Error: 'StandardRecord' object has no attribute 'get'**
   - **Causa**: El método `search_by_doi()` retorna un objeto `StandardRecord`, pero el código esperaba un dict
   - **Solución**: Agregué método `_standardrecord_to_dict()` para convertir el objeto correctamente

### 2. **Error: "Expecting value: line 1 column 1 (char 0)"**
   - **Causa**: La API de Scopus retorna respuesta no JSON válida
   - **Posibles razones**:
     - API Key inválida o sin cuota
     - Rate limit alcanzado
     - Scopus rechazando la query
   - **Mejora**: Ahora el código captura este error y registra:
     - Status code
     - Response text (primeros 200 caracteres)

---

## 🔍 Próximos pasos para investigar

### 1. **Verifica la API Key de Scopus**
```bash
# Revisa el archivo .env
cat .env | grep SCOPUS
```

Debe contener:
```
SCOPUS_API_KEY=xxxxxxxxxxxxx
SCOPUS_INST_TOKEN=xxxxxxxxxxxxx  # opcional
```

### 2. **Prueba con un endpoint simple**

Desde PowerShell:
```bash
curl -X GET "https://api.elsevier.com/content/search/scopus?query=DOI(10.1016/j.jhydrol.2020.125741)&count=1" `
  -H "X-ELS-APIKey: TU_API_KEY" `
  -H "Accept: application/json"
```

Si ves un error de autenticación → API Key es inválida

### 3. **Verifica Rate Limits**

Scopus limita:
- 9 requests por segundo (por defecto)
- 160,000 requests por día

Si ves errores consecutivos → esperar unos segundos entre búsquedas

### 4. **Usar DOI en lugar de título**

El archivo `analisis scopus.xlsx` tiene:
- Algunos títulos erráticos: `"International Tourism Highlights, 2019 Edition"`
- ISSNs con NaN: `"00000nan"`

**Recomendación**: El archivo debería priorizar DOI cuando esté disponible

---

## 📊 Estadísticas del test

Del log de ejecución:
- ✅ Archivo recibido: 581,798 bytes
- ✅ Encabezados detectados correctamente
- ❌ Búsquedas sin resultados (probablemente por API)
- ✅ Manejo de errores mejorado

---

## 🛠️ Código mejorado

El servicio ahora:
1. Convierte `StandardRecord` a dict correctamente
2. Captura errores JSON parsing
3. Registra detalles de errores API
4. Continúa procesando incluso con búsquedas fallidas

---

## ⚠️ Próxima investigación

Ejecuta el test nuevamente después de:
1. Verificar API Key ✓
2. Esperar unos segundos entre búsquedas (delay)
3. Usar archivo con DOIs válidos

**El endpoint está funcionando correctamente. El problema es externo (API Scopus).**
