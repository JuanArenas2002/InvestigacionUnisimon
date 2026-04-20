"""
Tests del SourceRegistry (sistema de plugins).

Demuestra:
- Registro manual de adapters
- Autodiscovery
- Creacion de instancias
- Manejo de fuentes no registradas

Ejecutar:
    pytest tests/project/test_registry.py -v
"""

from typing import List, Optional

import pytest

from project.domain.models.publication import Publication
from project.domain.ports.source_port import SourcePort
from project.registry.source_registry import SourceRegistry


# ──────────────────────────────────────────────────────────────────────────────
# STUBS de adapters para tests
# ──────────────────────────────────────────────────────────────────────────────


class StubSourceA(SourcePort):
    SOURCE_NAME = "stub_a"

    @property
    def source_name(self) -> str:
        return self.SOURCE_NAME

    def fetch_records(self, **kwargs) -> List[Publication]:
        return []


class StubSourceB(SourcePort):
    SOURCE_NAME = "stub_b"

    @property
    def source_name(self) -> str:
        return self.SOURCE_NAME

    def fetch_records(self, **kwargs) -> List[Publication]:
        return []


class StubSourceNoName(SourcePort):
    """Adapter sin SOURCE_NAME — no debe registrarse."""

    SOURCE_NAME = ""

    @property
    def source_name(self) -> str:
        return ""

    def fetch_records(self, **kwargs) -> List[Publication]:
        return []


# ──────────────────────────────────────────────────────────────────────────────
# TESTS
# ──────────────────────────────────────────────────────────────────────────────


class TestSourceRegistry:
    def setup_method(self):
        self.registry = SourceRegistry()

    def test_register_single_adapter(self):
        self.registry._register_from_module(
            type("FakeModule", (), {"StubSourceA": StubSourceA})
        )
        assert "stub_a" in self.registry.source_names

    def test_create_returns_correct_type(self):
        self.registry._register_from_module(
            type("FakeModule", (), {"StubSourceA": StubSourceA})
        )
        instance = self.registry.create("stub_a")
        assert isinstance(instance, StubSourceA)

    def test_create_many_returns_list(self):
        module = type("FakeModule", (), {"StubSourceA": StubSourceA, "StubSourceB": StubSourceB})
        self.registry._register_from_module(module)
        instances = self.registry.create_many(["stub_a", "stub_b"])
        assert len(instances) == 2
        assert all(isinstance(i, SourcePort) for i in instances)

    def test_unknown_source_raises_value_error(self):
        with pytest.raises(ValueError, match="no registrada"):
            self.registry.create("no_existe")

    def test_source_names_sorted(self):
        module = type("FakeModule", (), {"StubSourceB": StubSourceB, "StubSourceA": StubSourceA})
        self.registry._register_from_module(module)
        names = self.registry.source_names
        assert names == sorted(names)

    def test_adapter_without_source_name_ignored(self):
        module = type("FakeModule", (), {"StubSourceNoName": StubSourceNoName})
        self.registry._register_from_module(module)
        assert "" not in self.registry.source_names

    def test_base_source_port_not_registered(self):
        module = type("FakeModule", (), {"SourcePort": SourcePort})
        self.registry._register_from_module(module)
        assert "SourcePort" not in str(self.registry.source_names)

    def test_case_insensitive_create(self):
        module = type("FakeModule", (), {"StubSourceA": StubSourceA})
        self.registry._register_from_module(module)
        # Busca con mayusculas
        instance = self.registry.create("STUB_A")
        assert isinstance(instance, StubSourceA)


class TestSourceRegistryAutodiscovery:
    def test_autodiscover_loads_all_adapters(self):
        """Autodiscovery debe encontrar los 5 adapters en infrastructure/sources."""
        registry = SourceRegistry().autodiscover("project.infrastructure.sources")
        expected = {"scopus", "openalex", "wos", "cvlac", "datos_abiertos"}
        registered = set(registry.source_names)
        assert expected.issubset(registered), (
            f"Faltan adapters: {expected - registered}"
        )

    def test_autodiscover_returns_self(self):
        """autodiscover debe retornar el mismo registry (para encadenamiento)."""
        registry = SourceRegistry()
        result = registry.autodiscover("project.infrastructure.sources")
        assert result is registry
