"""
Tests de los endpoints FastAPI de la arquitectura hexagonal.
Usa TestClient de Starlette — no levanta servidor real.

Ejecutar:
    pytest tests/project/test_fastapi_routes.py -v
"""

from typing import Dict, List, Optional
from unittest.mock import patch, MagicMock

import pytest

from project.domain.models.author import Author
from project.domain.models.publication import Publication


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────


def make_pipeline_result_dict(
    collected: int = 5,
    deduplicated: int = 4,
    normalized: int = 4,
    matched: int = 4,
    enriched: int = 4,
    authors_saved: int = 8,
    source_saved: int = 4,
    canonical_upserted: int = 4,
    by_source: dict | None = None,
    errors: dict | None = None,
):
    from project.application.ingest_pipeline import PipelineResult
    return PipelineResult(
        collected=collected,
        deduplicated=deduplicated,
        normalized=normalized,
        matched=matched,
        enriched=enriched,
        authors_saved=authors_saved,
        source_saved=source_saved,
        canonical_upserted=canonical_upserted,
        by_source=by_source or {"mock": collected},
        errors=errors or {},
    )


# ──────────────────────────────────────────────────────────────────────────────
# TESTS: HEALTH y ROOT
# ──────────────────────────────────────────────────────────────────────────────


class TestHealthEndpoint:
    def test_health_returns_200(self):
        from fastapi.testclient import TestClient
        from project.app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/health")
        assert response.status_code == 200

    def test_health_returns_ok_status(self):
        from fastapi.testclient import TestClient
        from project.app.main import app
        client = TestClient(app, raise_server_exceptions=False)
        response = client.get("/health")
        data = response.json()
        assert data["status"] == "ok"


# ──────────────────────────────────────────────────────────────────────────────
# TESTS: INGEST ENDPOINT
# ──────────────────────────────────────────────────────────────────────────────


class TestIngestEndpoint:
    def test_ingest_with_invalid_source_returns_400(self):
        """Fuente no registrada debe retornar 400."""
        from fastapi.testclient import TestClient
        from project.app.main import app

        with patch("project.interfaces.api.routers.ingest.build_source_registry") as mock_reg:
            mock_registry = MagicMock()
            mock_registry.source_names = ["scopus", "openalex"]
            mock_reg.return_value = mock_registry

            client = TestClient(app)
            response = client.post("/ingest", json={"sources": ["fuente_inexistente"]})

        assert response.status_code == 400
        data = response.json()
        assert "invalid" in data["detail"]

    def test_ingest_dry_run_returns_ok(self):
        """dry_run=true no debe persistir pero si retornar resultado."""
        from fastapi.testclient import TestClient
        from project.app.main import app

        mock_result = make_pipeline_result_dict()

        with (
            patch("project.interfaces.api.routers.ingest.build_source_registry") as mock_reg,
            patch("project.interfaces.api.routers.ingest.build_pipeline") as mock_pipe,
        ):
            mock_registry = MagicMock()
            mock_registry.source_names = ["mock"]
            mock_reg.return_value = mock_registry

            mock_pipeline = MagicMock()
            mock_pipeline.run.return_value = mock_result
            mock_pipe.return_value = mock_pipeline

            client = TestClient(app)
            response = client.post("/ingest", json={
                "sources": ["mock"],
                "dry_run": True,
                "max_results": 10,
            })

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["persistence"]["dry_run"] is True

    def test_ingest_response_has_stages(self):
        """Respuesta debe incluir conteos de cada etapa del pipeline."""
        from fastapi.testclient import TestClient
        from project.app.main import app

        mock_result = make_pipeline_result_dict(
            collected=10, deduplicated=8, normalized=8, matched=8, enriched=8
        )

        with (
            patch("project.interfaces.api.routers.ingest.build_source_registry") as mock_reg,
            patch("project.interfaces.api.routers.ingest.build_pipeline") as mock_pipe,
        ):
            mock_registry = MagicMock()
            mock_registry.source_names = ["mock"]
            mock_reg.return_value = mock_registry

            mock_pipeline = MagicMock()
            mock_pipeline.run.return_value = mock_result
            mock_pipe.return_value = mock_pipeline

            client = TestClient(app)
            response = client.post("/ingest", json={"sources": ["mock"]})

        assert response.status_code == 200
        data = response.json()
        assert "stages" in data
        assert data["stages"]["collect"] == 10
        assert data["stages"]["deduplicate"] == 8

    def test_ingest_no_sources_uses_all_registered(self):
        """Sin `sources` en el request, usa todas las fuentes registradas."""
        from fastapi.testclient import TestClient
        from project.app.main import app

        mock_result = make_pipeline_result_dict()

        with (
            patch("project.interfaces.api.routers.ingest.build_source_registry") as mock_reg,
            patch("project.interfaces.api.routers.ingest.build_pipeline") as mock_pipe,
        ):
            mock_registry = MagicMock()
            mock_registry.source_names = ["scopus", "openalex", "wos"]
            mock_reg.return_value = mock_registry

            mock_pipeline = MagicMock()
            mock_pipeline.run.return_value = mock_result
            mock_pipe.return_value = mock_pipeline

            client = TestClient(app)
            response = client.post("/ingest", json={})

        assert response.status_code == 200
        # build_pipeline debe haberse llamado con todas las fuentes
        call_args = mock_pipe.call_args[0][0]
        assert set(call_args) == {"scopus", "openalex", "wos"}


# ──────────────────────────────────────────────────────────────────────────────
# TESTS: PUBLICATIONS ENDPOINT
# ──────────────────────────────────────────────────────────────────────────────


class TestPublicationsEndpoint:
    def test_get_publications_returns_200(self):
        from fastapi.testclient import TestClient
        from project.app.main import app

        with patch("project.interfaces.api.routers.publications.build_repository") as mock_repo_builder:
            mock_repo = MagicMock()
            mock_repo.list_publications.return_value = [
                {"id": 1, "title": "Paper 1", "publication_year": 2022},
                {"id": 2, "title": "Paper 2", "publication_year": 2023},
            ]
            mock_repo_builder.return_value = mock_repo

            client = TestClient(app)
            response = client.get("/publications")

        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["items"]) == 2

    def test_get_publications_pagination(self):
        from fastapi.testclient import TestClient
        from project.app.main import app

        with patch("project.interfaces.api.routers.publications.build_repository") as mock_repo_builder:
            mock_repo = MagicMock()
            mock_repo.list_publications.return_value = []
            mock_repo_builder.return_value = mock_repo

            client = TestClient(app)
            response = client.get("/publications?limit=25&offset=50")

        assert response.status_code == 200
        data = response.json()
        assert data["limit"] == 25
        assert data["offset"] == 50
        mock_repo.list_publications.assert_called_once_with(limit=25, offset=50)

    def test_get_publications_limit_out_of_range(self):
        from fastapi.testclient import TestClient
        from project.app.main import app

        with patch("project.interfaces.api.routers.publications.build_repository"):
            client = TestClient(app)
            response = client.get("/publications?limit=9999")  # max es 500

        assert response.status_code == 422  # Validation error
