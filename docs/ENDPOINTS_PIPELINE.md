# Documentación de Endpoints del Pipeline

Esta documentación describe cada endpoint disponible en el router `/api/pipeline` del sistema de reconciliación bibliográfica, tras la última refactorización.

> **OpenAlex:** los endpoints y clases específicas de OpenAlex están documentados en [OPENALEX_PIPELINE.md](OPENALEX_PIPELINE.md).

---

## 1. `/api/pipeline/extract/scopus`
**Método:** POST  
**Descripción:** Extrae publicaciones desde Scopus (Elsevier API), las ingesta en `scopus_records` y reconcilia automáticamente. Usa el `SCOPUS_AFFILIATION_ID` de `.env` si no se envía en el body.  
**Body:**
```json
{
  "year_from": 2020,
  "year_to": 2025,
  "max_results": 1000
}
```
**Respuesta:** Igual a OpenAlex.

---

## 2. `/api/pipeline/extract/wos`
**Método:** POST  
**Descripción:** Extrae publicaciones desde Web of Science (Clarivate API), ingesta en `wos_records` y reconcilia automáticamente.  
**Body:** Similar a OpenAlex/Scopus.

---

## 3. `/api/pipeline/extract/cvlac`
**Método:** POST  
**Descripción:** Extrae publicaciones desde CvLAC (scraping Minciencias), ingesta en `cvlac_records` y reconcilia automáticamente.  
**Body:** Puede requerir códigos de CvLAC.

---

## 4. `/api/pipeline/extract/datos_abiertos`
**Método:** POST  
**Descripción:** Extrae publicaciones desde Datos Abiertos Colombia, ingesta en `datos_abiertos_records` y reconcilia automáticamente.  
**Body:** Similar a OpenAlex.

---

## 5. `/api/pipeline/load-json`
**Método:** POST  
**Descripción:** Carga un archivo JSON previamente descargado, detecta la fuente, ingesta los registros y reconcilia automáticamente.  
**Body:**
```json
{
  "filename": "openalex_publications_20260210_115654.json",
  "source": "openalex" // opcional, auto-detecta si no se indica
}
```

---

## 6. `/api/pipeline/reconcile`
**Método:** POST  
**Descripción:** Ejecuta la reconciliación manual de un batch de registros pendientes (de todas las fuentes).  
**Body:**
```json
{
  "batch_size": 500 // opcional, default 500
}
```
**Respuesta:** Resumen de la reconciliación del batch.

---

## 7. `/api/pipeline/reconcile-all`
**Método:** POST  
**Descripción:** Ejecuta la reconciliación manual de **todos** los registros pendientes (de todas las fuentes), procesando en lotes hasta terminar.  
**Body:** vacío
**Respuesta:** Resumen acumulado de la reconciliación.

---

## 8. `/api/pipeline/crossref-scopus`
**Método:** POST  
**Descripción:** Cruza las publicaciones canónicas con Scopus por DOI, enriquece campos faltantes y actualiza autores. Trabaja por lotes.  
**Body:**
```json
{
  "batch_size": 50 // opcional, default 50, max 200
}
```
**Respuesta:** Resumen del enriquecimiento cruzado.

---

## Notas generales
- **Todos los endpoints de extracción** (extract/*) hacen ingesta y reconciliación automática.
- **La reconciliación manual** (`/reconcile`, `/reconcile-all`) es útil si cargas varias fuentes y quieres controlar cuándo reconciliar.
- **No se eliminan registros fuente**: la trazabilidad es total.
- **Control de duplicados**: la ingesta previene registros repetidos en cada tabla fuente.

---

Cualquier endpoint que no esté en esta lista ha sido eliminado por ser redundante o innecesario.
