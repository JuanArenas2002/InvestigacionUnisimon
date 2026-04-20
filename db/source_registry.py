# Backward-compatibility shim. New code: use project.infrastructure.persistence.source_registry
from project.infrastructure.persistence.source_registry import *  # noqa: F401, F403
from project.infrastructure.persistence.source_registry import SourceDefinition, SourceRegistry, SOURCE_REGISTRY  # noqa: F401
