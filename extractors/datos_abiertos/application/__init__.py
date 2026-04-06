"""
Capa de aplicación del extractor Datos Abiertos Colombia.

Orquesta la extracción paginada de datasets SODA: construye la URL,
aplica filtros SoQL, y gestiona la paginación por offset hasta agotar
los resultados o alcanzar el límite pedido.
"""
