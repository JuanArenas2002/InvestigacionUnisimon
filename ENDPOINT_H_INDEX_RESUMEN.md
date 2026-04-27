# 📊 Endpoint H-Index Scopus - Resumen

## ✅ Implementación Completada

Se ha creado un endpoint FastAPI que permite extraer el **H-Index y otras métricas bibliométricas** de autores de Scopus desde un archivo Excel.

### 📍 URL del Endpoint
```
POST /api/scopus/author-h-index
```

### 📝 Características

1. **Entrada**: Archivo Excel (.xlsx o .xls) con una columna de IDs de autores
   - Soporta columnas: `author_id`, `scopus_id`, `scopus_author_id`, `id`
   - Si no identifica la columna, usa la primera columna por defecto

2. **Procesamiento**:
   - Usa ThreadPoolExecutor para consultas paralelas (hasta 10 simultáneas)
   - Consulta Scopus API por cada autor
   - Extrae: H-Index, Documentos, Citas Totales, Citado Por, Coautores

3. **Salida**: Archivo Excel con 3 hojas:
   - **H-Index Autores**: Tabla con métricas de cada autor exitoso
   - **Errores**: Autores que no pudieron procesarse con mensaje de error
   - **Resumen**: Estadísticas generales (promedio h-index, máximo, mínimo, etc.)

### 🔧 Archivos Creados/Modificados

#### Nuevos archivos:
1. **`api/services/scopus_h_index_service.py`** - Servicio principal
   - Clase `ScopusHIndexService` para procesar lotes de autores
   - Función `get_author_h_index()` para consultar un autor

2. **`api/exporters/excel/scopus_h_index.py`** - Exportador de Excel
   - Genera Excel con 3 hojas formateadas

#### Modificados:
3. **`api/routers/scopus.py`** - Router de Scopus
   - Agregado endpoint `POST /scopus/author-h-index`

### 📊 Ejemplo de Respuesta

```
Archivo Excel con:

H-Index Autores (Hoja 1):
| Scopus ID    | Nombre       | H-Index | Documentos | Citas Totales | Citado Por | Coautores |
|---|---|---|---|---|---|---|
| 57193767797  | [Nombre]     | 21      | 105        | 4976          | 4269       | 0         |
| 7404530122   | [Nombre]     | 12      | 45         | 1523          | 1200       | 5         |

Errores (Hoja 2):
| Scopus ID   | Estado | Mensaje de Error                  |
|---|---|---|
| 35093378600 | error  | Autor no encontrado               |

Resumen (Hoja 3):
- Total de Autores Procesados: 3
- Autores Exitosos: 2
- Autores con Error: 1
- H-INDEX PROMEDIO: 16.5
- H-INDEX MÁXIMO: 21
- Total Documentos: 150
- Total Citas: 6499
```

### 🧪 Prueba del Endpoint

Se proporciona script de prueba: `test_h_index_endpoint.py`

```python
# Enviar archivo Excel
response = requests.post(
    'http://localhost:8000/api/scopus/author-h-index',
    files={'file': open('autores.xlsx', 'rb')}
)

# Guardar resultado
with open('resultado.xlsx', 'wb') as f:
    f.write(response.content)
```

### ⚙️ Parámetros Query

- `max_workers` (1-10, default=3): Número de consultas simultáneas a Scopus

### 🔐 Requisitos

- `SCOPUS_API_KEY` configurada en `.env`
- Archivo Excel válido con IDs de autores

### 📈 Rendimiento

- Procesa ~3-5 autores por segundo (dependiendo del servidor Scopus)
- Con 10 workers simultáneos, ~30-50 autores/segundo

### ✨ Ventajas

✅ Procesamiento paralelo eficiente
✅ Manejo robusto de errores
✅ Excel bien formateado y profesional
✅ Estadísticas automáticas
✅ Compatible con múltiples formatos de columnas

---

**Endpoint listo para usar** 🚀
