"""
Capa de dominio del extractor Scopus.

Contiene la lógica pura de negocio:
  - query_builder: construcción de queries avanzadas con operadores de campo de Scopus.
  - record_parser: parseo de entradas XML/JSON de Scopus → campos de StandardRecord,
                   incluyendo clasificación de estado Open Access.

No tiene dependencias de HTTP, disco ni frameworks externos.
"""
