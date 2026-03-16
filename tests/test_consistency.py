"""
Tests de Integridad y Consistencia de la Base de Datos.

DDD: Domain Tests — Valida reglas de negocio y constraints de BD

Ejecutar:
    pytest tests/test_consistency.py -v
    pytest tests/test_consistency.py::TestCanonicalPublications -v
"""

import pytest
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.models import (
    CanonicalPublication,
    Author,
    PublicationAuthor,
    Journal,
    Institution,
    OpenalexRecord,
    ScopusRecord,
    ReconciliationLog,
)
from db.session import get_session, create_all_tables
from config import RecordStatus, MatchType


class TestCanonicalPublications:
    """Pruebas de integridad de canonical_publications"""

    @pytest.fixture
    def db_session(self):
        """Crea sesión y tablas limpias para cada test"""
        session = get_session()
        create_all_tables()
        yield session
        # Cleanup
        session.query(CanonicalPublication).delete()
        session.query(Author).delete()
        session.query(Journal).delete()
        session.query(Institution).delete()
        session.commit()
        session.close()

    def test_canonical_pub_required_fields(self, db_session: Session):
        """Valida que campos obligatorios están enforce en BD"""
        pub = CanonicalPublication(
            title="Test Publication",
            # doi, pmid, pmcid son opcionales
        )
        db_session.add(pub)
        db_session.commit()
        
        assert pub.id is not None
        assert pub.title == "Test Publication"
        assert pub.created_at is not None

    def test_canonical_pub_title_not_null(self, db_session: Session):
        """Title no puede ser NULL"""
        pub = CanonicalPublication(title=None)
        db_session.add(pub)
        
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_canonical_pub_year_range(self, db_session: Session):
        """publication_year debe estar en rango 1900-2099"""
        # Válido
        pub = CanonicalPublication(
            title="Valid Year",
            publication_year=2023
        )
        db_session.add(pub)
        db_session.commit()
        assert pub.publication_year == 2023

    def test_canonical_pub_doi_unique(self, db_session: Session):
        """DOI debe ser UNIQUE (o NULL múltiples)"""
        doi = "10.1000/xyz"
        
        pub1 = CanonicalPublication(
            title="Publication 1",
            doi=doi
        )
        pub2 = CanonicalPublication(
            title="Publication 2",
            doi=doi
        )
        
        db_session.add(pub1)
        db_session.commit()
        
        db_session.add(pub2)
        
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_canonical_pub_null_dois_allowed(self, db_session: Session):
        """Múltiples publicaciones con doi=NULL está permitido"""
        pub1 = CanonicalPublication(title="Pub 1", doi=None)
        pub2 = CanonicalPublication(title="Pub 2", doi=None)
        
        db_session.add(pub1)
        db_session.add(pub2)
        db_session.commit()
        
        assert pub1.id != pub2.id
        assert pub1.doi is None
        assert pub2.doi is None

    def test_field_provenance_jsonb(self, db_session: Session):
        """field_provenance debe estar disponible como JSONB"""
        pub = CanonicalPublication(
            title="Test",
            field_provenance={
                "title": "openalex",
                "year": "scopus",
                "doi": "wos"
            }
        )
        db_session.add(pub)
        db_session.commit()
        
        retrieved = db_session.query(CanonicalPublication).first()
        assert retrieved.field_provenance["title"] == "openalex"


class TestAuthors:
    """Pruebas de integridad de tabla authors"""

    @pytest.fixture
    def db_session(self):
        session = get_session()
        create_all_tables()
        yield session
        session.query(Author).delete()
        session.commit()
        session.close()

    def test_author_name_required(self, db_session: Session):
        """name es campo obligatorio"""
        author = Author(name="Juan García")
        db_session.add(author)
        db_session.commit()
        
        assert author.id is not None

    def test_author_orcid_unique(self, db_session: Session):
        """ORCID debe ser UNIQUE"""
        orcid = "0000-0001-2345-6789"
        
        author1 = Author(name="Author 1", orcid=orcid)
        author2 = Author(name="Author 2", orcid=orcid)
        
        db_session.add(author1)
        db_session.commit()
        
        db_session.add(author2)
        
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_author_multiple_null_orcids(self, db_session: Session):
        """Múltiples autores sin ORCID (NULL) está permitido"""
        author1 = Author(name="Author 1", orcid=None)
        author2 = Author(name="Author 2", orcid=None)
        
        db_session.add(author1)
        db_session.add(author2)
        db_session.commit()
        
        assert author1.id != author2.id


class TestPublicationAuthors:
    """Pruebas de relación N:M publication_authors"""

    @pytest.fixture
    def db_session(self):
        session = get_session()
        create_all_tables()
        yield session
        session.query(PublicationAuthor).delete()
        session.query(Author).delete()
        session.query(CanonicalPublication).delete()
        session.commit()
        session.close()

    def test_publication_authors_order(self, db_session: Session):
        """author_position debe preservar orden en listado"""
        pub = CanonicalPublication(title="Multi-author paper")
        author1 = Author(name="First Author")
        author2 = Author(name="Second Author")
        
        db_session.add_all([pub, author1, author2])
        db_session.flush()
        
        pa1 = PublicationAuthor(
            publication_id=pub.id,
            author_id=author1.id,
            author_position=1
        )
        pa2 = PublicationAuthor(
            publication_id=pub.id,
            author_id=author2.id,
            author_position=2
        )
        
        db_session.add_all([pa1, pa2])
        db_session.commit()
        
        # Verificar
        pub_retrieved = db_session.query(CanonicalPublication).first()
        authors = sorted(pub_retrieved.authors, key=lambda x: x.author_position)
        
        assert authors[0].author_position == 1
        assert authors[1].author_position == 2

    def test_publication_authors_unique(self, db_session: Session):
        """Relación (pub_id, author_id) debe ser UNIQUE"""
        pub = CanonicalPublication(title="Test")
        author = Author(name="Test Author")
        
        db_session.add_all([pub, author])
        db_session.flush()
        
        pa1 = PublicationAuthor(
            publication_id=pub.id,
            author_id=author.id,
            author_position=1
        )
        pa2 = PublicationAuthor(
            publication_id=pub.id,
            author_id=author.id,
            author_position=1
        )
        
        db_session.add(pa1)
        db_session.commit()
        
        db_session.add(pa2)
        
        with pytest.raises(IntegrityError):
            db_session.commit()


class TestExternalRecords:
    """Pruebas de integridad de tablas por fuente"""

    @pytest.fixture
    def db_session(self):
        session = get_session()
        create_all_tables()
        yield session
        session.query(OpenalexRecord).delete()
        session.query(ScopusRecord).delete()
        session.query(CanonicalPublication).delete()
        session.commit()
        session.close()

    def test_openalex_record_required_fields(self, db_session: Session):
        """openalex_work_id es obligatorio"""
        record = OpenalexRecord(
            openalex_work_id="W123456789",
            title="Test",
            status="pending"
        )
        db_session.add(record)
        db_session.commit()
        
        assert record.id is not None

    def test_source_record_status_values(self, db_session: Session):
        """status debe ser uno de los valores válidos"""
        record = OpenalexRecord(
            openalex_work_id="W123",
            title="Test",
            status=RecordStatus.PENDING.value  # Usar enum
        )
        db_session.add(record)
        db_session.commit()
        
        assert record.status == "pending"

    def test_source_record_fk_canonical(self, db_session: Session):
        """canonical_publication_id debe referenciar tabla correcta"""
        pub = CanonicalPublication(title="Canon")
        record = OpenalexRecord(
            openalex_work_id="W123",
            title="Test",
            status="pending"
        )
        
        db_session.add(pub)
        db_session.flush()
        
        record.canonical_publication_id = pub.id
        db_session.add(record)
        db_session.commit()
        
        assert record.canonical_publication_id == pub.id


class TestReconciliationLog:
    """Pruebas de auditoría"""

    @pytest.fixture
    def db_session(self):
        session = get_session()
        create_all_tables()
        yield session
        session.query(ReconciliationLog).delete()
        session.query(OpenalexRecord).delete()
        session.query(CanonicalPublication).delete()
        session.commit()
        session.close()

    def test_reconciliation_log_immutable(self, db_session: Session):
        """Los logs no deben actualizarse (solo insert)"""
        record = OpenalexRecord(
            openalex_work_id="W123",
            title="Test",
            status="pending"
        )
        
        log = ReconciliationLog(
            source_record_id=1,
            source_name="openalex",
            match_type=MatchType.DOI_EXACT,
            match_score=100.0,
            decision_reason="Exact DOI match"
        )
        
        db_session.add(record)
        db_session.flush()
        
        log.source_record_id = record.id
        db_session.add(log)
        db_session.commit()
        
        assert log.id is not None
        assert log.created_at is not None

    def test_reconciliation_log_match_score_range(self, db_session: Session):
        """match_score debe estar entre 0 y 100"""
        log = ReconciliationLog(
            source_record_id=1,
            source_name="scopus",
            match_type=MatchType.FUZZY,
            match_score=85.5,
            decision_reason="Fuzzy match on title+authors"
        )
        
        db_session.add(log)
        db_session.commit()
        
        assert 0 <= log.match_score <= 100


class TestDataQualityMetrics:
    """Tests de calidad de datos agregados"""

    @pytest.fixture
    def db_session(self):
        session = get_session()
        create_all_tables()
        yield session
        
        # Limpiar después de tests
        session.query(PublicationAuthor).delete()
        session.query(OpenalexRecord).delete()
        session.query(ScopusRecord).delete()
        session.query(ReconciliationLog).delete()
        session.query(Author).delete()
        session.query(CanonicalPublication).delete()
        session.commit()
        session.close()

    def test_canonical_coverage_doi(self, db_session: Session):
        """Porcentaje de publicaciones con DOI"""
        for i in range(10):
            doi = f"10.1000/test{i}" if i < 7 else None
            pub = CanonicalPublication(
                title=f"Publication {i}",
                doi=doi
            )
            db_session.add(pub)
        
        db_session.commit()
        
        total = db_session.query(CanonicalPublication).count()
        with_doi = db_session.query(CanonicalPublication).filter(
            CanonicalPublication.doi.isnot(None)
        ).count()
        
        coverage = with_doi / total if total > 0 else 0
        
        assert total == 10
        assert with_doi == 7
        assert coverage == 0.7

    def test_reconciliation_match_distribution(self, db_session: Session):
        """Distribución de tipos de coincidencia"""
        pub = CanonicalPublication(title="Test")
        db_session.add(pub)
        db_session.flush()
        
        match_types = [
            MatchType.DOI_EXACT,
            MatchType.FUZZY,
            MatchType.FUZZY,
            MatchType.MANUAL_REVIEW,
        ]
        
        for match_type in match_types:
            log = ReconciliationLog(
                source_record_id=1,
                source_name="openalex",
                match_type=match_type,
                match_score=100.0 if match_type == MatchType.DOI_EXACT else 75.0,
                decision_reason="Test"
            )
            db_session.add(log)
        
        db_session.commit()
        
        # Contar por tipo
        doi_count = db_session.query(ReconciliationLog).filter(
            ReconciliationLog.match_type == MatchType.DOI_EXACT
        ).count()
        fuzzy_count = db_session.query(ReconciliationLog).filter(
            ReconciliationLog.match_type == MatchType.FUZZY
        ).count()
        
        assert doi_count == 1
        assert fuzzy_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
