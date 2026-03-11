# Documentación de Endpoints de Todo el Sistema

Este documento describe todos los endpoints disponibles en el sistema, agrupados por módulo, con su propósito y uso principal.

---



## 1. `/pipeline` (Extracción, reconciliación y administración)

> Los procesos de OpenAlex (extracción, enriquecimiento, búsqueda) están documentados en [OPENALEX_PIPELINE.md](OPENALEX_PIPELINE.md).

- **POST `/extract/scopus`**: Extrae registros de Scopus usando el ID institucional configurado y los almacena.
- **POST `/load-json`**: Carga registros desde un archivo JSON local a la base de datos.
- **POST `/reconcile`**: Ejecuta la reconciliación solo sobre registros pendientes.
- **POST `/reconcile-all`**: Ejecuta la reconciliación sobre todos los registros de fuentes.
- **POST `/crossref-scopus-enrichment`**: Enriquecimiento cruzado entre Crossref y Scopus (si está implementado).
- **POST `/search-doi-in-sources`**: Busca un DOI en todas las fuentes externas (OpenAlex, Scopus, WoS, CvLAC, Datos Abiertos) y devuelve el registro encontrado por fuente.
	- **Body:** `{ "doi": "10.1234/abcd.5678" }`
	- **Respuesta:** Lista de resultados por fuente, cada uno con el registro encontrado (o `null` si no existe).
- **DELETE `/truncate-all`**: Elimina todos los registros y deja la base de datos limpia.
- **POST `/init-db`**: Inicializa la base de datos y crea todas las tablas necesarias.

---

## 2. `/authors` (Gestión y consulta de autores)

- **GET `/stats`**: Estadísticas globales de autores.
- **GET `/`**: Lista paginada de autores.
- **GET `/ids-coverage`**: Cobertura de identificadores (ORCID, Scopus, etc).
- **GET `/without-orcid`**: Lista de autores sin ORCID.
- **GET `/{author_id}`**: Detalle de un autor específico.
- **GET `/{author_id}/publications`**: Publicaciones asociadas a un autor.
- **GET `/{author_id}/coauthors`**: Coautores de un investigador.

---

## 3. `/catalogs` (Catálogos de revistas e instituciones)

- **GET `/journals`**: Lista paginada de revistas.
- **GET `/journals/{journal_id}`**: Detalle de una revista.
- **POST `/journals`**: Crear una nueva revista.
- **GET `/institutions`**: Lista paginada de instituciones.
- **GET `/institutions/{institution_id}`**: Detalle de una institución.
- **POST `/institutions`**: Crear una nueva institución.

---

## 4. `/external-records` (Registros de fuentes externas)

- **GET `/`**: Lista paginada de registros de fuentes externas.
- **GET `/by-source-status`**: Conteo de registros por fuente y estado.
- **GET `/match-types`**: Distribución de tipos de coincidencia en reconciliación.
- **GET `/manual-review`**: Registros que requieren revisión manual.
- **GET `/reconciliation-log`**: Log de operaciones de reconciliación.

---

## 5. `/publications` (Gestión y consulta de publicaciones)

- **GET `/`**: Lista paginada de publicaciones.
- **GET `/exists`**: Verifica si una publicación existe por identificador.
- **GET `/by-year`**: Distribución de publicaciones por año.
- **GET `/field-coverage`**: Cobertura de campos temáticos.
- **GET `/types`**: Tipos de publicación disponibles.
- **GET `/{pub_id}`**: Detalle de una publicación específica.

---

## 6. `/scopus` (Operaciones específicas de Scopus)

- **GET**: Endpoints para consulta y enriquecimiento de datos Scopus (ver detalles en código, suelen ser internos o de soporte).

---

## 7. `/search` (Búsqueda avanzada)

- **GET `/openalex`**: Búsqueda directa en OpenAlex — ver [OPENALEX_PIPELINE.md](OPENALEX_PIPELINE.md).

---

## 8. `/stats` (Estadísticas y salud del sistema)

- **GET `/health`**: Estado de salud del sistema (para monitoreo).
- **GET `/system`**: KPIs generales del sistema.
- **GET `/overview`**: Estadísticas generales de publicaciones y autores.
- **GET `/quality`**: Resumen de problemas de calidad de datos.
- **GET `/json-files`**: Lista de archivos JSON disponibles para carga.

---

### Notas generales
- Todos los endpoints usan autenticación y validación según configuración del sistema.
- Los endpoints de extracción y reconciliación están pensados para ser ejecutados por operadores o procesos automáticos.
- Los endpoints de consulta (`GET`) están optimizados para dashboards, reportes y análisis.

---

Para detalles de parámetros, ejemplos de uso y respuestas, consulta la documentación OpenAPI/Swagger generada automáticamente por FastAPI en `/docs` cuando el servidor esté en ejecución.
