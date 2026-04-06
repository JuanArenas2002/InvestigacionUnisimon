"""
Capa de aplicación del módulo Serial Title.

Orquesta las estrategias de búsqueda de cobertura de revistas:
  - coverage_service: lookup masivo paralelo, fallback chains (EID → ISSN → DOI → título),
    enriquecimiento de listas de publicaciones con datos de cobertura.

No contiene lógica de parseo de JSON ni detalles de sesión HTTP.
"""
