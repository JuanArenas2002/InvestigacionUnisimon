"""
Auto-descubrimiento de fuentes de datos bibliográficas.

Este paquete contiene un módulo por cada fuente (openalex.py, scopus.py, etc.).
Cada módulo define su modelo SQLAlchemy y lo registra en SOURCE_REGISTRY.

El __init__.py usa pkgutil para importar automáticamente todos los módulos,
de modo que cualquier nuevo archivo fuente se describe automáticamente sin
necesidad de modificar este archivo.
"""

import pkgutil
import importlib

# Auto-importar todos los módulos en este paquete
for importer, modname, ispkg in pkgutil.iter_modules(__path__):
    importlib.import_module(f"{__name__}.{modname}")
