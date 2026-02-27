"""
Backfill: Inferir field_provenance para publicaciones existentes.

Para publicaciones con sources_count=1, la fuente de TODOS los campos
es el único external_record vinculado.

Para publicaciones con múltiples fuentes, se asigna cada campo
al external_record más antiguo que podría haberlo aportado.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from db.session import get_engine
from db.models import CanonicalPublication, get_all_source_records_for_canonical
from sqlalchemy.orm import Session

engine = get_engine()

with Session(engine) as session:
    # Publicaciones sin provenance o con provenance vacío
    pubs = (
        session.query(CanonicalPublication)
        .filter(
            (CanonicalPublication.field_provenance.is_(None)) |
            (CanonicalPublication.field_provenance == {})
        )
        .all()
    )
    print(f"Publicaciones sin provenance: {len(pubs)}")

    updated = 0
    for pub in pubs:
        # Obtener registros de todas las fuentes vinculados, ordenados por fecha
        ext_records = get_all_source_records_for_canonical(session, pub.id)

        if not ext_records:
            continue

        prov = {}

        # El primer record (más antiguo) creó la canónica → todos los campos iniciales son de él
        first = ext_records[0]
        src = first.source_name

        if pub.title:
            prov["title"] = src
        if pub.doi:
            prov["doi"] = src
        if pub.publication_year:
            prov["publication_year"] = src

        # Para campos que podrían haber venido de enriquecimiento posterior,
        # intentamos ser más precisos revisando el raw_data de cada fuente
        field_extractors = {
            "source_journal": lambda raw: (
                (raw.get("source", {}).get("display_name") if isinstance(raw.get("source"), dict) else None)
                or raw.get("prism:publicationName")
                or raw.get("source_journal")
                or raw.get("sourceTitle")
            ),
            "publication_type": lambda raw: (
                raw.get("publication_type")
                or raw.get("type")
                or raw.get("subtypeDescription")
            ),
            "is_open_access": lambda raw: (
                (raw.get("open_access", {}).get("is_oa") if isinstance(raw.get("open_access"), dict) else None)
                if raw.get("open_access") else raw.get("openaccessFlag")
            ),
            "issn": lambda raw: (
                (raw.get("source", {}).get("issn_l") if isinstance(raw.get("source"), dict) else None)
                or raw.get("prism:issn")
                or raw.get("issn")
            ),
            "citation_count": lambda raw: (
                raw.get("cited_by_count")
                or raw.get("citedby-count")
                or raw.get("citation_count")
            ),
            "publication_date": lambda raw: (
                raw.get("publication_date")
                or raw.get("prism:coverDate")
                or raw.get("publishDate")
            ),
        }

        for field_name, extractor in field_extractors.items():
            canon_value = getattr(pub, field_name, None)
            if canon_value is None:
                continue
            if field_name == "citation_count" and canon_value == 0:
                continue

            # Buscar cuál fuente aportó este campo (la primera que lo tiene)
            for er in ext_records:
                raw = er.raw_data or {}
                try:
                    val = extractor(raw)
                    if val is not None:
                        prov[field_name] = er.source_name
                        break
                except Exception:
                    continue
            else:
                # Si ningún raw_data lo tiene, asumir la primera fuente
                prov[field_name] = src

        if prov:
            pub.field_provenance = prov
            updated += 1

    session.commit()
    print(f"Backfill completado: {updated} publicaciones actualizadas con provenance")
