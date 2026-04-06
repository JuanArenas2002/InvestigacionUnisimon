"""
Capa de dominio del extractor CVLAC.

Contiene la lógica pura de negocio:
  - html_parser: extracción de datos de la estructura HTML de CVLAC.
  - record_parser: transformación de dicts crudos a campos de StandardRecord.

No tiene dependencias de HTTP, disco ni frameworks externos.
Toda la lógica aquí es determinista y testeable sin red.
"""
