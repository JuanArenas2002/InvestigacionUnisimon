"""
Capa de dominio del extractor Datos Abiertos Colombia.

Contiene la lógica pura de negocio:
  - query_builder: construcción de cláusulas SoQL ($where) para filtrar datasets.
  - record_parser: transformación flexible de registros crudos a campos de StandardRecord.

No tiene dependencias de HTTP, disco ni frameworks externos.
"""
