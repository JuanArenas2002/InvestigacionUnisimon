from typing import Dict, List, Optional

from sqlalchemy import desc

from db.models import CanonicalPublication
from db.session import get_session
from db.source_registry import SOURCE_REGISTRY

from project.domain.models.publication import Publication
from project.ports.repository_port import RepositoryPort


class PostgresRepository(RepositoryPort):
    """Adapter PostgreSQL para guardar y consultar publicaciones."""

    def save_source_records(self, records_by_source: Dict[str, List[Publication]]) -> int:
        saved = 0
        session = get_session()
        try:
            for source_name, publications in records_by_source.items():
                source_definition = SOURCE_REGISTRY.get(source_name)
                model_cls = source_definition.model_class

                for publication in publications:
                    kwargs = self._build_common_kwargs(publication)
                    source_definition.build_specific_kwargs(publication, publication.raw_data or {}, kwargs)

                    instance = self._find_existing_source_record(
                        session=session,
                        model_cls=model_cls,
                        id_attr=source_definition.id_attr,
                        source_id=publication.source_id,
                    )
                    if instance is None:
                        instance = model_cls(**kwargs)
                        session.add(instance)
                    else:
                        for key, value in kwargs.items():
                            setattr(instance, key, value)
                    saved += 1

            session.commit()
            return saved
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_canonical_publications(self, publications: List[Publication]) -> int:
        session = get_session()
        upserted = 0
        try:
            for publication in publications:
                canonical = self._find_existing_canonical(session, publication)
                if canonical is None:
                    canonical = CanonicalPublication(
                        doi=publication.doi,
                        title=publication.title or "Sin titulo",
                        normalized_title=publication.normalized_title,
                        publication_year=publication.publication_year,
                        publication_date=publication.publication_date,
                        publication_type=publication.publication_type,
                        language=publication.language,
                        source_journal=publication.source_journal,
                        issn=publication.issn,
                        is_open_access=publication.is_open_access,
                        oa_status=publication.oa_status,
                        citation_count=publication.citation_count,
                        field_provenance={
                            "title": publication.source_name,
                            "doi": publication.source_name if publication.doi else None,
                        },
                    )
                    session.add(canonical)
                else:
                    canonical.title = publication.title or canonical.title
                    canonical.normalized_title = publication.normalized_title or canonical.normalized_title
                    canonical.publication_year = publication.publication_year or canonical.publication_year
                    canonical.citation_count = max(int(canonical.citation_count or 0), int(publication.citation_count or 0))
                    canonical.updated_at = canonical.updated_at
                upserted += 1

            session.commit()
            return upserted
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def list_publications(self, limit: int = 100, offset: int = 0) -> List[dict]:
        session = get_session()
        try:
            rows = (
                session.query(CanonicalPublication)
                .order_by(desc(CanonicalPublication.id))
                .offset(offset)
                .limit(limit)
                .all()
            )
            return [
                {
                    "id": row.id,
                    "doi": row.doi,
                    "title": row.title,
                    "publication_year": row.publication_year,
                    "publication_type": row.publication_type,
                    "citation_count": row.citation_count,
                    "source_journal": row.source_journal,
                    "estado_publicacion": row.estado_publicacion,
                }
                for row in rows
            ]
        finally:
            session.close()

    @staticmethod
    def _build_common_kwargs(publication: Publication) -> dict:
        return {
            "doi": publication.doi,
            "title": publication.title,
            "normalized_title": publication.normalized_title,
            "publication_year": publication.publication_year,
            "publication_date": publication.publication_date,
            "publication_type": publication.publication_type,
            "source_journal": publication.source_journal,
            "issn": publication.issn,
            "language": publication.language,
            "is_open_access": publication.is_open_access,
            "oa_status": publication.oa_status,
            "citation_count": publication.citation_count,
            "authors_text": publication.authors_text,
            "normalized_authors": publication.normalized_authors,
            "url": publication.url,
            "raw_data": publication.raw_data,
            "status": "pending",
            "match_type": publication.match_type,
            "match_score": publication.match_score,
        }

    @staticmethod
    def _find_existing_source_record(session, model_cls, id_attr: str, source_id: Optional[str]):
        if source_id:
            return session.query(model_cls).filter(getattr(model_cls, id_attr) == source_id).first()
        return None

    @staticmethod
    def _find_existing_canonical(session, publication: Publication):
        if publication.doi:
            row = session.query(CanonicalPublication).filter(CanonicalPublication.doi == publication.doi).first()
            if row is not None:
                return row

        if publication.normalized_title and publication.publication_year:
            return (
                session.query(CanonicalPublication)
                .filter(
                    CanonicalPublication.normalized_title == publication.normalized_title,
                    CanonicalPublication.publication_year == publication.publication_year,
                )
                .first()
            )
        return None
