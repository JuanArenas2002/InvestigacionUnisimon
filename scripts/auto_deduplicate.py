"""
Script de Auto-Deduplicación de Publicaciones.

DDD: Application Layer
Automatiza la fusión de registros con fuzzy matching score >= umbral.

Cascada de decisión:
  1. Score >= fuzzy_auto_accept (0.95) → Fusionar automática
  2. Score >= fuzzy_manual_review (0.85) → Marcar para revisión
  3. Score < fuzzy_manual_review → NO hacer nada

Uso:
    python scripts/auto_deduplicate.py [--dry-run] [--threshold 0.95] [--limit 1000]

Salida:
    - Log de decisiones
    - Reporte CSV: reports/dedup_report_{timestamp}.csv
"""

import logging
import argparse
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import func, and_
from sqlalchemy.orm import Session
from config import reconciliation_config as rc_config, MatchType, RecordStatus
from db.session import get_session
from db.models import (
    CanonicalPublication,
    OpenalexRecord,
    ScopusRecord,
    WosRecord,
    CvlacRecord,
    DatosAbiertosRecord,
    ReconciliationLog,
    SOURCE_MODELS,
)
from reconciliation.fuzzy_matcher import compare_records, FuzzyMatchResult

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S"
))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class DeduplicationReport:
    """Resultado de deduplicación"""
    source_record_id: int
    source_name: str
    canonical_a_id: int
    canonical_b_id: int
    match_score: float
    decision: str  # merged, manual_review, skipped
    merged_canonical_id: Optional[int] = None
    reason: str = ""


class AutoDeduplicator:
    """Deduplicador automático (application layer)"""

    def __init__(
        self,
        session: Session,
        fuzzy_threshold: float = 0.95,
        manual_review_threshold: float = 0.85,
        dry_run: bool = False,
    ):
        self.session = session
        self.fuzzy_threshold = fuzzy_threshold
        self.manual_review_threshold = manual_review_threshold
        self.dry_run = dry_run
        self.reports: List[DeduplicationReport] = []
        self.stats = {
            "total_checked": 0,
            "merged_auto": 0,
            "marked_manual_review": 0,
            "skipped": 0,
        }

    def deduplicate_source(
        self,
        source_name: str,
        limit: Optional[int] = None,
    ):
        """Deduplicación para registros de una fuente"""
        model_cls = SOURCE_MODELS.get(source_name)
        if not model_cls:
            logger.error(f"Fuente desconocida: {source_name}")
            return

        logger.info(f"🔍 Iniciando deduplicación de {source_name}...")

        # Obtener registros ya reconciliados
        query = self.session.query(model_cls).filter(
            model_cls.canonical_publication_id.isnot(None)
        )

        if limit:
            query = query.limit(limit)

        records = query.all()
        logger.info(f"📥 Encontrados {len(records)} registros reconciliados")

        # Agrupar por canonical_publication_id
        canon_groups = {}
        for record in records:
            canon_id = record.canonical_publication_id
            if canon_id not in canon_groups:
                canon_groups[canon_id] = []
            canon_groups[canon_id].append(record)

        logger.info(f"📦 Agrupados en {len(canon_groups)} publicaciones canónicas")

        # Comparar publicaciones canónicas
        canon_ids = list(canon_groups.keys())
        for i, canon_id_a in enumerate(canon_ids):
            for canon_id_b in canon_ids[i + 1 :]:
                self._compare_and_merge(canon_id_a, canon_id_b, source_name)

        self._print_stats()

    def _compare_and_merge(self, canon_id_a: int, canon_id_b: int, source_name: str):
        """Compara dos publicaciones canónicas y decide si fusionar"""
        pub_a = self.session.query(CanonicalPublication).get(canon_id_a)
        pub_b = self.session.query(CanonicalPublication).get(canon_id_b)

        if not pub_a or not pub_b:
            return

        # ¿Ya están fusionadas?
        if pub_a.id == pub_b.id:
            return

        self.stats["total_checked"] += 1

        # Comparar con fuzzy matching
        result = self._fuzzy_compare(pub_a, pub_b)

        if result.combined_score < self.manual_review_threshold:
            self.stats["skipped"] += 1
            return

        # Decisión
        if result.combined_score >= self.fuzzy_threshold:
            decision = "merged_auto"
            reason = f"Score {result.combined_score:.2f} >= {self.fuzzy_threshold}"
            self.stats["merged_auto"] += 1

            if not self.dry_run:
                self._merge_canonicals(pub_a, pub_b)

        else:  # manual_review_threshold <= score < fuzzy_threshold
            decision = "manual_review"
            reason = (
                f"Score {result.combined_score:.2f} entre "
                f"{self.manual_review_threshold}-{self.fuzzy_threshold}"
            )
            self.stats["marked_manual_review"] += 1

            if not self.dry_run:
                self._mark_for_manual_review(pub_a, pub_b)

        report = DeduplicationReport(
            source_record_id=pub_b.id,
            source_name=source_name,
            canonical_a_id=pub_a.id,
            canonical_b_id=pub_b.id,
            match_score=result.combined_score,
            decision=decision,
            merged_canonical_id=pub_a.id if decision == "merged_auto" else None,
            reason=reason,
        )
        self.reports.append(report)

        logger.info(
            f"{'➕' if decision == 'merged_auto' else '👁️ '} "
            f"Canon #{pub_a.id} vs #{pub_b.id}: "
            f"{result.combined_score:.2f} → {decision}"
        )

    def _fuzzy_compare(self, pub_a: CanonicalPublication, pub_b: CanonicalPublication) -> FuzzyMatchResult:
        """Compara dos publicaciones usando fuzzy matching"""
        from reconciliation.fuzzy_matcher import (
            compare_titles,
            compare_years,
            compare_authors,
        )

        title_score = compare_titles(pub_a.title or "", pub_b.title or "")
        year_match, year_score = compare_years(pub_a.publication_year, pub_b.publication_year)

        # Ambos modelos tienen relación 'authors' → PublicationAuthor → Author
        authors_a = " ".join([pa.author.name for pa in pub_a.authors.all()] if pub_a.authors else [])
        authors_b = " ".join([pa.author.name for pa in pub_b.authors.all()] if pub_b.authors else [])
        author_score = compare_authors(authors_a, authors_b)

        # Score combinado (pesos desde config)
        combined_score = (
            title_score * rc_config.title_weight
            + year_score * rc_config.year_weight
            + author_score * rc_config.author_weight
        )

        result = FuzzyMatchResult(
            title_score=title_score,
            year_match=year_match,
            year_score=year_score,
            author_score=author_score,
            combined_score=combined_score,
        )

        return result

    def _merge_canonicals(self, keeper: CanonicalPublication, removable: CanonicalPublication):
        """Fusiona dos publicaciones canónicas"""
        # 0. ANTES de borrar 'removable', actualizar/eliminar sus PublicationAuthor
        # para evitar duplicados en la restricción única (publication_id, author_id)
        from db.models import PublicationAuthor
        authors_to_update = self.session.query(PublicationAuthor).filter(
            PublicationAuthor.publication_id == removable.id
        ).all()
        
        # Obtener todos los author_id del keeper para evitar duplicados
        keeper_authors = self.session.query(PublicationAuthor.author_id).filter(
            PublicationAuthor.publication_id == keeper.id
        ).all()
        keeper_author_ids = {row[0] for row in keeper_authors}
        
        for author_link in authors_to_update:
            if author_link.author_id in keeper_author_ids:
                # Ya existe este autor en keeper, simplemente eliminar del removable
                self.session.delete(author_link)
            else:
                # No existe en keeper, reasignar a keeper
                author_link.publication_id = keeper.id
                self.session.add(author_link)
        
        # 1. Actualizar todos los registros fuente de 'removable' → 'keeper'
        for source_name, model_cls in SOURCE_MODELS.items():
            records_to_update = self.session.query(model_cls).filter(
                model_cls.canonical_publication_id == removable.id
            ).all()

            for record in records_to_update:
                record.canonical_publication_id = keeper.id
                self.session.add(record)

        # 2. Actualizar field_provenance de keeper (incorporar campos de removable)
        if removable.field_provenance:
            if keeper.field_provenance is None:
                keeper.field_provenance = {}
            for field, source in removable.field_provenance.items():
                if field not in keeper.field_provenance:
                    keeper.field_provenance[field] = source

        # 3. Actualizar sources_count
        keeper.sources_count = (keeper.sources_count or 1) + (removable.sources_count or 1)

        # 4. Eliminar canonical_publications 'removable'
        self.session.delete(removable)

        # 5. Registrar en log
        log = ReconciliationLog(
            source_record_id=removable.id,
            source_name="auto_dedup",
            canonical_publication_id=keeper.id,
            match_type="fuzzy_combined",
            match_score=100.0,
            action="linked_existing",
            match_details={"reason": f"Auto-merged with #{keeper.id}"},
        )
        self.session.add(log)
        self.session.commit()

    def _mark_for_manual_review(
        self,
        pub_a: CanonicalPublication,
        pub_b: CanonicalPublication,
    ):
        """Marca dos publicaciones como similares para revisión"""
        # Crear registros de log como "manual_review pending"
        for pub in [pub_a, pub_b]:
            log = ReconciliationLog(
                source_record_id=pub.id,
                source_name="auto_dedup",
                canonical_publication_id=pub.id,
                match_type="manual_review",
                match_score=0.0,
                action="flagged_review",
                match_details={"reason": f"Potential duplicate with #{pub_b.id if pub == pub_a else pub_a.id}"},
            )
            self.session.add(log)

        self.session.commit()

    def _print_stats(self):
        """Imprime estadísticas"""
        logger.info("=" * 70)
        logger.info("📊 RESUMEN DE DEDUPLICACIÓN")
        logger.info("=" * 70)
        logger.info(f"Comparadas:        {self.stats['total_checked']}")
        logger.info(f"  ➕ Fusionadas:    {self.stats['merged_auto']}")
        logger.info(f"  👁️  Revisión:      {self.stats['marked_manual_review']}")
        logger.info(f"  ⏭️  Saltadas:      {self.stats['skipped']}")
        logger.info("=" * 70)

    def export_reports(self):
        """Exporta reporte de deduplicación"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reports_dir = Path(__file__).parent.parent / "reports"
        reports_dir.mkdir(exist_ok=True)

        csv_file = reports_dir / f"dedup_report_{timestamp}.csv"
        with open(csv_file, "w", encoding="utf-8") as f:
            f.write("canon_a,canon_b,match_score,decision,reason\n")
            for report in self.reports:
                f.write(
                    f'{report.canonical_a_id},'
                    f'{report.canonical_b_id},'
                    f'{report.match_score:.2f},'
                    f'{report.decision},'
                    f'"{report.reason}"\n'
                )

        logger.info(f"📄 Reporte: {csv_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Auto-deduplicación de publicaciones"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular sin cambios a BD"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.95,
        help="Umbral de fusión automática (default: 0.95)"
    )
    parser.add_argument(
        "--review-threshold",
        type=float,
        default=0.85,
        help="Umbral para marcar revisión (default: 0.85)"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["openalex", "scopus", "wos", "cvlac", "datos_abiertos", "all"],
        default="all",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
    )

    args = parser.parse_args()

    try:
        session = get_session()
        dedup = AutoDeduplicator(
            session,
            fuzzy_threshold=args.threshold,
            manual_review_threshold=args.review_threshold,
            dry_run=args.dry_run,
        )

        if args.dry_run:
            logger.info("🔍 Modo DRY RUN (sin cambios)")

        sources = (
            ["openalex", "scopus", "wos", "cvlac", "datos_abiertos"]
            if args.source == "all"
            else [args.source]
        )

        for source in sources:
            dedup.deduplicate_source(source, limit=args.limit)

        dedup.export_reports()
        logger.info("✅ Deduplicación completada!")
        return 0

    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
