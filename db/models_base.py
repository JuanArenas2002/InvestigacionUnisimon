# Backward-compatibility shim. New code: use project.infrastructure.persistence.models_base
from project.infrastructure.persistence.models_base import *  # noqa: F401, F403
from project.infrastructure.persistence.models_base import Base, SourceRecordMixin  # noqa: F401
