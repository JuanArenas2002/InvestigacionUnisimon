# Backward-compatibility shim. New code: use project.domain.ports.repository_port
from project.domain.ports.repository_port import RepositoryPort

__all__ = ["RepositoryPort"]
