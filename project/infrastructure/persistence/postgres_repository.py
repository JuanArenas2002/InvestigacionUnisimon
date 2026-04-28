import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import desc, func, or_, text

from project.infrastructure.persistence.models import Author, AuthorAuditLog, AuthorConflict, CanonicalPublication, PublicationAuthor
from project.infrastructure.persistence.session import get_session
from project.infrastructure.persistence.source_registry import SOURCE_REGISTRY
from shared.normalizers import normalize_author_name, normalize_text

from project.domain.models.publication import Publication
from project.domain.ports.repository_port import RepositoryPort  # PublicationRepositoryPort + AuthorRepositoryPort

logger = logging.getLogger(__name__)

# Umbral de similitud fuzzy (pg_trgm) para considerar dos nombres el mismo autor.
# 0.82 equivale a ~82% de trigramas en común — equilibrio entre precisión y recall.
FUZZY_SIMILARITY_THRESHOLD = 0.82


def _canonical_name(normalized: str) -> str:
    """
    Forma canonica de un nombre: tokens ordenados alfabeticamente.

    "juan garcia lopez"  → "garcia juan lopez"
    "garcia lopez juan"  → "garcia juan lopez"

    Garantiza que el mismo nombre en cualquier orden produzca
    la misma clave de busqueda, resolviendo el problema de nombres invertidos.
    """
    return " ".join(sorted(normalized.split()))


def _author_snapshot(author: Author) -> dict:
    """Genera un diccionario con los campos clave de un autor para el audit log."""
    return {
        "name": author.name,
        "normalized_name": author.normalized_name,
        "orcid": author.orcid,
        "external_ids": dict(author.external_ids or {}),
        "is_institutional": author.is_institutional,
        "verification_status": author.verification_status,
    }


def _log_author_change(
    session,
    author: Author,
    change_type: str,
    before: Optional[dict],
    source: str,
    field_changes: Optional[dict] = None,
) -> None:
    """Registra un cambio en el audit log sin interrumpir el flujo principal."""
    try:
        after = _author_snapshot(author)
        entry = AuthorAuditLog(
            author_id=author.id,
            change_type=change_type,
            before_data=before,
            after_data=after,
            field_changes=field_changes,
            source=source,
        )
        session.add(entry)
    except Exception:
        # El audit log nunca debe interrumpir el guardado de datos
        logger.warning("No se pudo registrar audit log para autor %s", getattr(author, "id", "?"))


def _log_conflict(
    session,
    author: Author,
    field_name: str,
    existing_value: str,
    new_value: str,
    existing_source: str,
    new_source: str,
) -> None:
    """Registra un conflicto entre fuentes para un campo de autor."""
    try:
        conflict = AuthorConflict(
            author_id=author.id,
            field_name=field_name,
            existing_value=str(existing_value) if existing_value is not None else None,
            new_value=str(new_value) if new_value is not None else None,
            existing_source=existing_source,
            new_source=new_source,
        )
        session.add(conflict)
    except Exception:
        logger.warning("No se pudo registrar conflicto para autor %s campo %s", getattr(author, "id", "?"), field_name)


class PostgresRepository(RepositoryPort):
    """Adapter PostgreSQL para guardar y consultar publicaciones."""

    # ─────────────────────────────────────────────────────────────────
    # save_authors — paso explicito en el pipeline
    # ─────────────────────────────────────────────────────────────────

    def save_authors(self, publications: List[Publication]) -> int:
        """
        Persiste todos los autores de una lista de publicaciones.

        Cascada de identificacion (ORCID primero):
          1. ORCID exacto
          2. ID externo por fuente (scopus_id, openalex_id, wos_id, cvlac_id)
          3. Nombre canonico (tokens ordenados → maneja nombres invertidos)

        Idempotente: upserta sin duplicar.
        """
        processed = 0
        session = get_session()
        try:
            for publication in publications:
                for author_payload in publication.authors or []:
                    result = self._upsert_author(
                        session, author_payload, publication.source_name
                    )
                    if result is not None:
                        processed += 1
            session.commit()
            return processed
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ─────────────────────────────────────────────────────────────────

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

                    # Alimentar autores desde la fase de extraccion para no depender
                    # de que la reconciliacion canonica haya corrido.
                    for author_payload in publication.authors or []:
                        self._upsert_author(session, author_payload, publication.source_name)

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
                    session.flush()
                else:
                    canonical.title = publication.title or canonical.title
                    canonical.normalized_title = publication.normalized_title or canonical.normalized_title
                    canonical.publication_year = publication.publication_year or canonical.publication_year
                    canonical.citation_count = max(int(canonical.citation_count or 0), int(publication.citation_count or 0))
                    canonical.updated_at = canonical.updated_at

                self._upsert_authors_and_links(session, canonical, publication)
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

    @staticmethod
    def _upsert_authors_and_links(session, canonical: CanonicalPublication, publication: Publication) -> None:
        if not publication.authors:
            return

        existing_relations: dict[int, PublicationAuthor] = {
            r.author_id: r
            for r in session.query(PublicationAuthor).filter(
                PublicationAuthor.publication_id == canonical.id
            ).all()
        }

        for position, author_payload in enumerate(publication.authors, start=1):
            author = PostgresRepository._upsert_author(session, author_payload, publication.source_name)
            if author is None:
                continue

            relation = existing_relations.get(author.id)
            if relation is None:
                relation = PublicationAuthor(
                    publication_id=canonical.id,
                    author_id=author.id,
                    is_institutional=bool(author_payload.is_institutional),
                    author_position=position,
                )
                session.add(relation)
                existing_relations[author.id] = relation
            else:
                relation.is_institutional = relation.is_institutional or bool(author_payload.is_institutional)
                if relation.author_position is None:
                    relation.author_position = position

    @staticmethod
    def _upsert_author(session, author_payload, source_name: str) -> Optional[Author]:
        clean_name = normalize_author_name(author_payload.name or "").strip()
        if not clean_name:
            return None

        normalized_name = normalize_text(clean_name)
        # Clave canonica: tokens ordenados → "Juan García" ≡ "García Juan"
        canonical = _canonical_name(normalized_name)

        external_ids = {
            k: str(v)
            for k, v in (author_payload.external_ids or {}).items()
            if v
        }

        author = None

        # ── 0. Cédula (identificador colombiano, columna directa con unique index) ──
        cedula = getattr(author_payload, "cedula", None)
        if cedula:
            author = session.query(Author).filter(
                Author.cedula == str(cedula).strip()
            ).first()

        # ── 1. ORCID (identificador unico, maxima prioridad) ──────────────
        if author is None and author_payload.orcid:
            author = session.query(Author).filter(
                Author.orcid == author_payload.orcid
            ).first()

        # ── 2. ID externo por fuente ──────────────────────────────────────
        if author is None:
            for ext_key, ext_value in external_ids.items():
                candidate = (
                    session.query(Author)
                    .filter(Author.external_ids[ext_key].astext == ext_value)
                    .first()
                )
                if candidate is not None:
                    author = candidate
                    break

        # ── 3. Nombre canonico exacto (maneja nombres invertidos) ─────────
        if author is None:
            author = (
                session.query(Author)
                .filter(
                    or_(
                        Author.normalized_name == canonical,
                        Author.normalized_name == normalized_name,
                    )
                )
                .first()
            )

        # ── 4. Similitud fuzzy por pg_trgm ────────────────────────────────
        # Solo si no encontramos nada exacto. Usa similarity() de pg_trgm para
        # detectar variantes del mismo nombre: abreviaciones, orden invertido
        # con guion, acentos inconsistentes, etc.
        # Política:
        #   - similitud >= umbral Y comparte al menos un external_id
        #     → mismo autor, fusionar (alta confianza)
        #   - similitud >= umbral Y sin external_ids compartidos
        #     → marcar como posible duplicado para revisión humana (no fusionar)
        if author is None and canonical:
            try:
                fuzzy_candidates = (
                    session.query(Author)
                    .filter(
                        func.similarity(Author.normalized_name, canonical)
                        >= FUZZY_SIMILARITY_THRESHOLD,
                        Author.normalized_name.isnot(None),
                    )
                    .order_by(
                        func.similarity(Author.normalized_name, canonical).desc()
                    )
                    .limit(5)
                    .all()
                )

                for candidate in fuzzy_candidates:
                    cand_ext = candidate.external_ids or {}
                    # Verificar si comparten algún ID externo
                    shared_keys = set(external_ids.keys()) & set(cand_ext.keys())
                    has_shared_id = any(
                        external_ids[k] == cand_ext[k] for k in shared_keys
                    )

                    if has_shared_id:
                        # Alta confianza: mismo autor
                        author = candidate
                        break
                    else:
                        # Baja confianza: marcar para revisión si aún no está marcado
                        if candidate.possible_duplicate_of is None:
                            # Se marcará al crear el nuevo autor (ver abajo)
                            pass
            except Exception:
                # pg_trgm no disponible o error de BD → continuar sin fuzzy
                logger.debug("Fuzzy match no disponible para '%s'", canonical)

        # ── Crear nuevo autor ─────────────────────────────────────────────
        if author is None:
            author = Author(
                name=clean_name,
                normalized_name=canonical,
                orcid=author_payload.orcid,
                cedula=str(cedula).strip() if cedula else None,
                external_ids=external_ids,
                is_institutional=bool(author_payload.is_institutional),
                verification_status="auto_detected",
                field_provenance={
                    source_name: {
                        "orcid": author_payload.orcid,
                        "external_ids": external_ids,
                        "metadata": author_payload.metadata or {},
                    }
                },
            )
            session.add(author)
            session.flush()

            # Si hay candidatos fuzzy sin ID compartido, señalar posible duplicado
            try:
                fuzzy_flag_candidates = (
                    session.query(Author)
                    .filter(
                        Author.id != author.id,
                        func.similarity(Author.normalized_name, canonical)
                        >= FUZZY_SIMILARITY_THRESHOLD,
                        Author.normalized_name.isnot(None),
                    )
                    .order_by(
                        func.similarity(Author.normalized_name, canonical).desc()
                    )
                    .limit(1)
                    .all()
                )
                if fuzzy_flag_candidates:
                    author.possible_duplicate_of = fuzzy_flag_candidates[0].id
                    author.verification_status = "needs_review"
            except Exception:
                pass

            _log_author_change(session, author, "created", None, source_name)
            return author

        # ── Enriquecer autor existente ────────────────────────────────────
        before = _author_snapshot(author)
        field_changes: dict = {}

        # Prefiere el nombre mas largo (mas informacion)
        if len(clean_name) > len(author.name or ""):
            field_changes["name"] = {"before": author.name, "after": clean_name}
            author.name = normalize_author_name(clean_name)

        # Migrar al formato canonico si el registro era del formato antiguo
        if author.normalized_name != canonical:
            author.normalized_name = canonical

        # ORCID: detectar conflicto si el autor ya tiene uno diferente
        if author_payload.orcid:
            if not author.orcid:
                field_changes["orcid"] = {"before": None, "after": author_payload.orcid}
                author.orcid = author_payload.orcid
            elif author.orcid != author_payload.orcid:
                # Conflicto: dos fuentes dan ORCIDs distintos — registrar sin sobreescribir
                existing_source = (author.field_provenance or {}).get("orcid", "unknown")
                _log_conflict(
                    session, author,
                    field_name="orcid",
                    existing_value=author.orcid,
                    new_value=author_payload.orcid,
                    existing_source=existing_source,
                    new_source=source_name,
                )

        # Cédula: rellenar si el autor existente no la tenía
        if cedula and not author.cedula:
            field_changes["cedula"] = {"before": None, "after": str(cedula).strip()}
            author.cedula = str(cedula).strip()

        author.is_institutional = author.is_institutional or bool(author_payload.is_institutional)

        # Fusionar external_ids detectando conflictos campo a campo
        current_ext = dict(author.external_ids or {})
        for ext_key, ext_value in external_ids.items():
            if ext_key not in current_ext:
                current_ext[ext_key] = ext_value
                field_changes[f"external_ids.{ext_key}"] = {"before": None, "after": ext_value}
            elif current_ext[ext_key] != ext_value:
                # Conflicto de ID externo
                existing_source = (author.field_provenance or {}).get(f"external_ids.{ext_key}", "unknown")
                _log_conflict(
                    session, author,
                    field_name=f"external_ids.{ext_key}",
                    existing_value=current_ext[ext_key],
                    new_value=ext_value,
                    existing_source=existing_source,
                    new_source=source_name,
                )
        author.external_ids = current_ext

        provenance = dict(author.field_provenance or {})
        provenance[source_name] = {
            "orcid": author_payload.orcid,
            "external_ids": external_ids,
            "metadata": author_payload.metadata or {},
        }
        author.field_provenance = provenance

        if field_changes:
            _log_author_change(session, author, "updated", before, source_name, field_changes)

        return author

    # ─────────────────────────────────────────────────────────────────
    # Edición controlada de perfil de autor
    # ─────────────────────────────────────────────────────────────────

    def get_author_by_id(self, author_id: int) -> Optional[dict]:
        session = get_session()
        try:
            author = session.query(Author).filter(Author.id == author_id).first()
            if not author:
                return None
            return {
                "id": author.id,
                "name": author.name,
                "normalized_name": author.normalized_name,
                "orcid": author.orcid,
                "cedula": author.cedula,
                "external_ids": dict(author.external_ids or {}),
                "is_institutional": author.is_institutional,
                "verification_status": author.verification_status,
            }
        finally:
            session.close()

    def get_author_name_options(self, author_id: int) -> list:
        """
        Para cada fuente vinculada, extrae el nombre del autor desde raw_data
        de sus publicaciones en esa fuente.
        """
        from db.models import SOURCE_MODELS, PublicationAuthor, CanonicalPublication

        session = get_session()
        try:
            author = session.query(Author).filter(Author.id == author_id).first()
            if not author:
                return []

            external_ids = dict(author.external_ids or {})
            options = []
            seen_names: set = set()

            # IDs de publicaciones canónicas del autor
            pub_ids = [
                pa.publication_id
                for pa in session.query(PublicationAuthor.publication_id)
                .filter(PublicationAuthor.author_id == author_id)
                .all()
            ]

            for source, source_model in SOURCE_MODELS.items():
                ext_id = external_ids.get(source)
                if not ext_id:
                    continue

                # Buscar registros de esta fuente vinculados a pubs del autor.
                # Para CvLAC filtramos además por cvlac_code para evitar leer
                # el nombre de un co-autor cuyo perfil también contiene la
                # misma publicación (raw_data._investigador pertenece al
                # investigador que generó el registro, no al autor buscado).
                query = session.query(source_model).filter(
                    source_model.canonical_publication_id.in_(pub_ids)
                )
                if source == "cvlac":
                    # cvlac_code almacena la cédula del investigador dueño del
                    # registro. Usar author.cedula (más confiable que ext_id,
                    # que puede contener cod_rh en vez de cédula).
                    cvlac_filter_val = author.cedula or ext_id
                    if cvlac_filter_val:
                        query = query.filter(
                            source_model.cvlac_code == cvlac_filter_val
                        )

                records = query.limit(10).all()

                for rec in records:
                    name = _extract_author_name_from_record(rec, source, ext_id)
                    if name and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        options.append({
                            "source": source,
                            "name": name,
                            "profile_url": _build_profile_url_for_source(source, ext_id),
                        })
                        break  # una opción por fuente es suficiente

            return options
        finally:
            session.close()

    def update_author_name(self, author_id: int, name: str, source: str) -> dict:
        session = get_session()
        try:
            author = session.query(Author).filter(Author.id == author_id).first()
            before = _author_snapshot(author)
            author.name = name
            author.normalized_name = normalize_text(normalize_author_name(name))
            field_changes = {
                "name": {"before": before["name"], "after": name},
                "normalized_name": {"before": before["normalized_name"], "after": author.normalized_name},
            }
            _log_author_change(session, author, "name_updated", before, source, field_changes)
            session.commit()
            return self.get_author_by_id(author_id)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def get_author_source_links(self, author_id: int) -> list:
        session = get_session()
        try:
            author = session.query(Author).filter(Author.id == author_id).first()
            if not author:
                return []

            external_ids = dict(author.external_ids or {})
            known_sources = ["cvlac", "openalex", "scopus", "wos", "google_scholar"]
            links = []
            for source in known_sources:
                ext_id = external_ids.get(source)
                links.append({
                    "source": source,
                    "external_id": ext_id,
                    "profile_url": _build_profile_url_for_source(source, ext_id) if ext_id else None,
                    "linked": bool(ext_id),
                })
            return links
        finally:
            session.close()

    def update_author_source_link(self, author_id: int, source: str, external_id: str) -> dict:
        session = get_session()
        try:
            author = session.query(Author).filter(Author.id == author_id).first()
            before = _author_snapshot(author)
            current = dict(author.external_ids or {})
            old_value = current.get(source)
            current[source] = external_id
            author.external_ids = current
            field_changes = {f"external_ids.{source}": {"before": old_value, "after": external_id}}
            _log_author_change(session, author, "source_link_updated", before, "manual", field_changes)
            session.commit()
            return {"author_id": author_id, "links": self.get_author_source_links(author_id)}
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def remove_author_source_link(self, author_id: int, source: str) -> dict:
        session = get_session()
        try:
            author = session.query(Author).filter(Author.id == author_id).first()
            before = _author_snapshot(author)
            current = dict(author.external_ids or {})
            old_value = current.pop(source, None)
            author.external_ids = current
            field_changes = {f"external_ids.{source}": {"before": old_value, "after": None}}
            _log_author_change(session, author, "source_link_removed", before, "manual", field_changes)
            session.commit()
            return {"author_id": author_id, "links": self.get_author_source_links(author_id)}
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def update_author_orcid(self, author_id: int, orcid: str) -> dict:
        session = get_session()
        try:
            author = session.query(Author).filter(Author.id == author_id).first()
            before = _author_snapshot(author)
            field_changes = {"orcid": {"before": author.orcid, "after": orcid}}
            author.orcid = orcid
            _log_author_change(session, author, "orcid_updated", before, "manual", field_changes)
            session.commit()
            return self.get_author_by_id(author_id)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def check_source_id_conflict(
        self, source: str, external_id: str, exclude_author_id: int
    ) -> Optional[int]:
        session = get_session()
        try:
            if source == "orcid":
                existing = (
                    session.query(Author.id)
                    .filter(Author.orcid == external_id, Author.id != exclude_author_id)
                    .first()
                )
            else:
                existing = (
                    session.query(Author.id)
                    .filter(
                        Author.external_ids[source].astext == external_id,
                        Author.id != exclude_author_id,
                    )
                    .first()
                )
            return existing[0] if existing else None
        finally:
            session.close()


# ── Helpers privados ─────────────────────────────────────────────────────────

_PROFILE_URL_TEMPLATES = {
    "cvlac": "https://scienti.minciencias.gov.co/cvlac/visualizador/generateCurriculoCvLac.do?cod_rh={id}",
    "openalex": "https://openalex.org/{id}",
    "scopus": "https://www.scopus.com/authid/detail.uri?authorId={id}",
    "google_scholar": "https://scholar.google.com/citations?user={id}",
    "orcid": "https://orcid.org/{id}",
}


def _build_profile_url_for_source(source: str, external_id: str) -> Optional[str]:
    template = _PROFILE_URL_TEMPLATES.get(source)
    return template.format(id=external_id) if template else None


def _extract_author_name_from_record(record, source: str, external_id: str) -> Optional[str]:
    """Extrae el nombre del autor específico desde el raw_data del registro."""
    raw = record.raw_data or {}

    if source == "cvlac":
        investigador = raw.get("_investigador") or {}
        return investigador.get("nombre") or None

    if source == "openalex":
        for authorship in raw.get("authorships", []):
            author_data = authorship.get("author") or {}
            # El ID en OpenAlex incluye la URL completa, comparar por sufijo
            oa_id = author_data.get("id", "")
            if external_id in oa_id or oa_id.endswith(external_id):
                return author_data.get("display_name") or None
        return None

    if source == "scopus":
        for author_data in raw.get("author", []):
            if str(author_data.get("authid", "")) == str(external_id):
                given = author_data.get("given-name", "")
                surname = author_data.get("surname", "")
                full = f"{given} {surname}".strip()
                return full or None
        return None

    if source == "wos":
        # WoS almacena autores en authors_text; usar como fallback
        return record.authors_text.split(";")[0].strip() if record.authors_text else None

    if source == "google_scholar":
        return raw.get("author_name") or raw.get("name") or None

    return None
