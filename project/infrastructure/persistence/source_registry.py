"""
Registro central de fuentes de datos bibliográficas.

ARQUITECTURA DE EXTENSIBILIDAD
================================
Para agregar una nueva fuente (ej: "pubmed") sin tocar ningún archivo existente:

  1. Crea el archivo  sources/pubmed.py  con:
       - Clase SQLAlchemy que herede de SourceRecordMixin + Base
       - Función constructora de kwargs específicos
       - Una llamada  SOURCE_REGISTRY.register(...)  al final del módulo

  2. Ejecuta la migración SQL para crear la tabla y, si aplica,
     agregar la clave "pubmed" a los registros JSONB de external_ids.

  Eso es todo. El motor de reconciliación, el API y los routers
  lo detectan automáticamente sin ningún cambio adicional.

NOTA sobre author_id_key:
  Antes cada fuente tenía una columna propia en la tabla authors
  (openalex_id, scopus_id, …). Ahora authors.external_ids es un JSONB
  con forma {"openalex": "A123", "scopus": "456", ...}.
  author_id_key es la clave que usa esa fuente dentro del diccionario.
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Type


# =============================================================
# DEFINICIÓN DE FUENTE
# =============================================================

@dataclass
class SourceDefinition:
    """
    Descriptor completo de una fuente de datos.

    Attributes:
        name:                  Nombre canónico ("openalex", "scopus", …)
        model_class:           Clase SQLAlchemy del registro de esa fuente.
        id_attr:               Atributo del modelo que contiene el ID propio
                               de la fuente (ej: "openalex_work_id").
        author_id_key:         Clave en authors.external_ids para el ID de
                               autor de esta fuente (ej: "openalex").
                               None si la fuente no tiene ID de autor.
        build_specific_kwargs: Función (record, raw, kwargs) → None que
                               añade al dict kwargs los campos específicos
                               de la fuente (modifica en-place).
    """
    name: str
    model_class: Type
    id_attr: str
    author_id_key: Optional[str]
    build_specific_kwargs: Callable


# =============================================================
# REGISTRO
# =============================================================

class SourceRegistry:
    """
    Registro en memoria de todas las fuentes activas.

    Se llena durante el arranque mediante llamadas a register() desde
    cada módulo sources/*.py. Ningún otro módulo necesita conocer las
    fuentes individualmente.
    """

    def __init__(self):
        self._sources: Dict[str, SourceDefinition] = {}

    # ── Registro ─────────────────────────────────────────────

    def register(self, definition: SourceDefinition) -> None:
        """Registra una fuente. Idempotente: re-registro sobreescribe."""
        self._sources[definition.name] = definition

    # ── Consulta individual ──────────────────────────────────

    def get(self, name: str) -> SourceDefinition:
        """Retorna la definición de la fuente o lanza ValueError si no existe."""
        if name not in self._sources:
            registered = list(self._sources)
            raise ValueError(
                f"Fuente no registrada: '{name}'. "
                f"Fuentes disponibles: {registered}"
            )
        return self._sources[name]

    def get_or_none(self, name: str) -> Optional[SourceDefinition]:
        return self._sources.get(name)

    # ── Vistas derivadas (usadas por engine, routers, etc.) ──

    def all(self) -> List[SourceDefinition]:
        return list(self._sources.values())

    @property
    def names(self) -> List[str]:
        """Lista de nombres registrados."""
        return list(self._sources.keys())

    @property
    def models(self) -> Dict[str, Type]:
        """Dict {nombre: ModelClass}."""
        return {s.name: s.model_class for s in self._sources.values()}

    @property
    def id_attrs(self) -> Dict[str, str]:
        """Dict {nombre: id_attr}."""
        return {s.name: s.id_attr for s in self._sources.values()}

    @property
    def author_id_keys(self) -> Dict[str, str]:
        """Dict {nombre: author_id_key} para fuentes que tienen ID de autor."""
        return {
            s.name: s.author_id_key
            for s in self._sources.values()
            if s.author_id_key
        }

    @property
    def source_id_mapping(self) -> Dict[str, str]:
        """
        Dict {NombreClase: id_attr} compatible con el SOURCE_ID_MAPPING
        que usaba authors.py.
        """
        return {
            s.model_class.__name__: s.id_attr
            for s in self._sources.values()
        }

    def __contains__(self, name: str) -> bool:
        return name in self._sources

    def __repr__(self) -> str:
        return f"<SourceRegistry sources={self.names}>"


# =============================================================
# INSTANCIA GLOBAL
# =============================================================

SOURCE_REGISTRY = SourceRegistry()
