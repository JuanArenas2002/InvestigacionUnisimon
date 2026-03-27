"""
Paquete de plugins de fuentes bibliográficas.

Auto-descubre e importa todos los módulos fuente (*.py) en este directorio.
Cada módulo debe llamar SOURCE_REGISTRY.register(...) al final para registrarse.

Para agregar una nueva fuente:
  1. Crea sources/nueva_fuente.py siguiendo el patrón de los existentes.
  2. Ejecuta la migración SQL correspondiente.
  No toques ningún otro archivo.
"""

import importlib
import pkgutil
import os

_package_dir = os.path.dirname(__file__)

for _finder, _module_name, _ispkg in pkgutil.iter_modules([_package_dir]):
    importlib.import_module(f"sources.{_module_name}")
