import importlib
import inspect
import pkgutil
from types import ModuleType
from typing import Dict, Iterable, List, Type

from project.domain.ports.source_port import SourcePort


class SourceRegistry:
    """Registro dinamico de adapters de fuentes (plugin system)."""

    def __init__(self) -> None:
        self._adapters: Dict[str, Type[SourcePort]] = {}

    def autodiscover(self, package_name: str = "project.infrastructure.sources") -> "SourceRegistry":
        package = importlib.import_module(package_name)
        for _, module_name, _ in pkgutil.iter_modules(package.__path__):
            module = importlib.import_module(f"{package_name}.{module_name}")
            self._register_from_module(module)
        return self

    def _register_from_module(self, module: ModuleType) -> None:
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if cls is SourcePort:
                continue
            if not issubclass(cls, SourcePort):
                continue
            source_name = getattr(cls, "SOURCE_NAME", "").strip().lower()
            if not source_name:
                continue
            self._adapters[source_name] = cls

    def create(self, source_name: str) -> SourcePort:
        key = source_name.strip().lower()
        if key not in self._adapters:
            raise ValueError(f"Fuente no registrada: {source_name}. Disponibles: {sorted(self._adapters)}")
        return self._adapters[key]()

    def create_many(self, source_names: Iterable[str]) -> List[SourcePort]:
        return [self.create(source_name) for source_name in source_names]

    @property
    def source_names(self) -> List[str]:
        return sorted(self._adapters.keys())
