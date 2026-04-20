# Backward-compatibility shim. New code: use project.domain.ports.source_port
from project.domain.ports.source_port import SourcePort

__all__ = ["SourcePort"]
