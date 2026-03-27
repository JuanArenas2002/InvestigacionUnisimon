"""
Registro central de fuentes de datos bibliográficas.

ARQUITECTURA DE EXTENSIBILIDAD
================================
Para agregar una nueva fuente (ej: "pubmed") sin tocar ningún archivo existente:

  1. Crea el modelo SQLAlchemy en db/models.py (o un archivo aparte)
     que herede de SourceRecordMixin + Base.

  2. Crea la función constructora de campos:
         def _build_pubmed_kwargs(record: StandardRecord, raw: dict) -> dict:
             return {
                 "pubmed_uid": record.source_id,
                 "abstract": raw.get("AbstractText"),
                 ...
             }

  3. Registra la fuente — una sola línea:
         SOURCE_REGISTRY.register(SourceDefinition(
             name="pubmed",
             model_class=PubmedRecord,
             id_attr="pubmed_uid",            # columna PK de la fuente en su tabla
             author_id_attr="pubmed_id",      # columna en la tabla authors (o None)
             build_specific_kwargs=_build_pubmed_kwargs,
         ))

  Eso es todo. El motor de reconciliación, el API y los routers de autores
  lo detectan automáticamente sin ningún cambio adicional.

NOTA sobre la base de datos:
  Cada nueva fuente requiere una migración SQL para crear su tabla
  y, si aplica, agregar la columna de autor (ej: authors.pubmed_id).
  Esto no puede evitarse — es inherente a la estructura relacional.
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
        name:                  Nombre canónico de la fuente ("openalex", "scopus", …)
        model_class:           Clase SQLAlchemy del registro de esa fuente.
        id_attr:               Nombre del atributo del modelo que contiene el ID propio
                               de la fuente (ej: "openalex_work_id").
        author_id_attr:        Nombre del atributo en el modelo Author que guarda el ID
                               de esta fuente (ej: "openalex_id"). None si no aplica.
        build_specific_kwargs: Función (record, raw) → dict con los kwargs específicos
                               de la fuente para construir el SourceRecord.
                               Recibe:
                                 - record: StandardRecord del extractor
                                 - raw:    dict con raw_data original
                               Devuelve solo los campos que NO están en SourceRecordMixin.
    """
    name: str
    model_class: Type
    id_attr: str
    author_id_attr: Optional[str]
    build_specific_kwargs: Callable


# =============================================================
# REGISTRO
# =============================================================

class SourceRegistry:
    """
    Registro en memoria de todas las fuentes activas.

    Se llena durante el arranque del módulo db/models.py mediante llamadas
    a SOURCE_REGISTRY.register(). Ningún otro módulo necesita conocer las
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
        """Lista de nombres registrados. Reemplaza KNOWN_SOURCES hardcodeado."""
        return list(self._sources.keys())

    @property
    def models(self) -> Dict[str, Type]:
        """Dict {nombre: ModelClass}. Reemplaza SOURCE_MODELS hardcodeado."""
        return {s.name: s.model_class for s in self._sources.values()}

    @property
    def id_attrs(self) -> Dict[str, str]:
        """Dict {nombre: id_attr}. Reemplaza _SOURCE_ID_ATTR hardcodeado."""
        return {s.name: s.id_attr for s in self._sources.values()}

    @property
    def author_id_attrs(self) -> Dict[str, str]:
        """Dict {nombre: author_id_attr} para los que tienen columna en Author."""
        return {
            s.name: s.author_id_attr
            for s in self._sources.values()
            if s.author_id_attr
        }

    @property
    def source_id_mapping(self) -> Dict[str, str]:
        """
        Dict {NombreClase: id_attr} compatible con el SOURCE_ID_MAPPING
        que usaba authors.py. Permite migración gradual.
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
