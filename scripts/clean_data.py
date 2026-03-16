"""
Script de Limpieza y Validación de Datos.

DDD: Application Layer
Ejecuta validación de criterios (domain layer) sobre registros pendientes.
Genera reportes de limpieza y marca registros para revisión.

Uso:
    python scripts/clean_data.py [--dry-run] [--source openalex] [--limit 1000]

Salida:
    - Logs en consola
    - Reporte CSV: reports/clean_data_report_{timestamp}.csv
    - Reporte JSON: reports/clean_data_metrics_{timestamp}.json
"""

import logging
import argparse
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict
from enum import Enum

# Agregar raíz del proyecto al path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import func
from sqlalchemy.orm import Session
from config import criteria_config as c_config, MatchType, RecordStatus
from db.session import get_session
from db.models import (
    OpenalexRecord,
    ScopusRecord,
    WosRecord,
    CvlacRecord,
    DatosAbiertosRecord,
    SOURCE_MODELS,
    ReconciliationLog,
)
from extractors.base import normalize_text, normalize_doi
from config import reconciliation_config as rc_config

# Logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s │ %(name)-20s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S"
))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# =============================================================
# DOMAIN LAYER: Criterios de Validación
# =============================================================

class ValidationResult(Enum):
    """Resultado de validación"""
    ACCEPTED = "accepted"
    ACCEPTED_WITH_WARNINGS = "accepted_with_warnings"
    PENDING_REVIEW = "pending_review"
    REJECTED_INCOMPLETE = "rejected_incomplete"
    REJECTED_CORRUPTED = "rejected_corrupted"
    REJECTED_NON_SCIENTIFIC = "rejected_non_scientific"


@dataclass
class CleaningReport:
    """Reporte de limpieza para un registro"""
    source_record_id: int
    source_name: str
    doi: Optional[str]
    title: Optional[str]
    decision: ValidationResult
    reason: str
    completeness_pct: float
    issues: List[str]
    timestamp: str = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


# =============================================================
# DOMAIN LAYER: Validador de Registros
# =============================================================

class RecordValidator:
    """Validación de criterios según CRITERIA.md"""

    def __init__(self, config=None):
        self.criteria = config or c_config
        self.issues = []

    def validate(self, record: dict) -> CleaningReport:
        """
        Valida un registro contra criterios.
        
        Returns:
            CleaningReport con decisión
        """
        self.issues = []
        
        source_id = record.get("id")
        source_name = record.get("source_name", "unknown")
        title = record.get("title", "").strip()
        doi = record.get("doi", "").strip()
        year = record.get("publication_year")
        authors_count = len(record.get("authors_text", "").split(";")) if record.get("authors_text") else 0
        
        # 1️⃣ Validar campos obligatorios
        if not self._validate_required_fields(title, year, source_name):
            decision = ValidationResult.REJECTED_INCOMPLETE
            reason = "; ".join(self.issues)
            return self._build_report(source_id, source_name, doi, title, decision, reason)
        
        # 2️⃣ Validar datos corrompidos
        if not self._validate_data_integrity(title, doi, record):
            decision = ValidationResult.REJECTED_CORRUPTED
            reason = "; ".join(self.issues)
            return self._build_report(source_id, source_name, doi, title, decision, reason)
        
        # 3️⃣ Validar contenido científico
        if not self._validate_scientific_content(title):
            decision = ValidationResult.REJECTED_NON_SCIENTIFIC
            reason = "; ".join(self.issues)
            return self._build_report(source_id, source_name, doi, title, decision, reason)
        
        # 4️⃣ Calcular completitud de metadatos
        completeness = self._calculate_completeness(record)
        
        # 5️⃣ Tomar decisión final
        if completeness >= self.criteria.min_completeness_accepted:
            decision = ValidationResult.ACCEPTED
            reason = f"Registro completo ({completeness:.1%})"
            if self.issues:
                decision = ValidationResult.ACCEPTED_WITH_WARNINGS
                reason += f". Advertencias: {'; '.join(self.issues[:2])}"
        elif completeness >= self.criteria.min_completeness_review:
            decision = ValidationResult.PENDING_REVIEW
            reason = f"Completitud marginal ({completeness:.1%}), requiere revisión"
        else:
            decision = ValidationResult.REJECTED_INCOMPLETE
            reason = f"Completitud insuficiente ({completeness:.1%} < {self.criteria.min_completeness_review:.1%})"
        
        return self._build_report(source_id, source_name, doi, title, decision, reason, completeness)

    def _validate_required_fields(self, title: str, year: Optional[int], source: str) -> bool:
        """Valida campos obligatorios"""
        if not title or len(title) < self.criteria.min_title_length:
            self.issues.append(f"Título inválido o muy corto (< {self.criteria.min_title_length} caracteres)")
            return False
        
        if len(title) > self.criteria.max_title_length:
            self.issues.append(f"Título muy largo (> {self.criteria.max_title_length} caracteres)")
            return False
        
        if year is None or year < self.criteria.min_year or year > self.criteria.max_year:
            self.issues.append(f"Año fuera de rango ({self.criteria.min_year}-{self.criteria.max_year})")
            return False
        
        if source not in self.criteria.valid_sources:
            self.issues.append(f"Fuente desconocida: {source}")
            return False
        
        return True

    def _validate_data_integrity(self, title: str, doi: str, record: dict) -> bool:
        """Valida posibles datos corrompidos"""
        # Títulos que parecen XML corrompido
        if "<" in title or ">" in title or "{" in title:
            self.issues.append("Título contiene caracteres XML/JSON (posible corrupción)")
            return False
        
        # Títulos repetidos
        if title == title.split()[0] * len(title.split()):
            self.issues.append("Título con palabras repetidas (corrupto)")
            return False
        
        # DOI inválido
        if doi:
            norm_doi = normalize_doi(doi)
            if not norm_doi or not norm_doi.startswith("10."):
                self.issues.append(f"DOI inválido: {doi}")
                return False
        
        # ISSN muy corto (sin normalizar)
        issn = record.get("issn", "").strip()
        if issn and len(issn) < 8:
            self.issues.append(f"ISSN parece incompleto: {issn}")
            # No rechazamos por esto, solo advertencia
        
        return True

    def _validate_scientific_content(self, title: str) -> bool:
        """Detecta contenido no científico"""
        title_lower = title.lower()
        
        for bad_keyword in self.criteria.blacklist_keywords:
            if bad_keyword.lower() in title_lower:
                self.issues.append(f"Título contiene palabra prohibida: '{bad_keyword}'")
                return False
        
        # Detectar URLs en título
        if "http://" in title or "https://" in title:
            self.issues.append("Título parece ser una URL")
            return False
        
        # Detectar emails
        if "@" in title:
            self.issues.append("Título contiene email")
            return False
        
        return True

    def _calculate_completeness(self, record: dict) -> float:
        """Calcula porcentaje de campos completados"""
        fields_to_check = [
            "title",
            "publication_year",
            "doi",
            "authors_text",
            "source_journal",
            "issn",
            "publication_type",
            "language",
            "is_open_access",
        ]
        
        completed = sum(1 for f in fields_to_check if record.get(f))
        return completed / len(fields_to_check)

    def _build_report(
        self,
        source_id: int,
        source_name: str,
        doi: str,
        title: str,
        decision: ValidationResult,
        reason: str,
        completeness: float = 0.0,
    ) -> CleaningReport:
        return CleaningReport(
            source_record_id=source_id,
            source_name=source_name,
            doi=doi,
            title=title[:50] if title else None,
            decision=decision,
            reason=reason,
            completeness_pct=completeness,
            issues=self.issues.copy(),
        )


# =============================================================
# APPLICATION LAYER: Limpiador
# =============================================================

class DataCleaner:
    """Ejecuta limpieza de datos (application layer)"""

    def __init__(self, session: Session, dry_run: bool = False):
        self.session = session
        self.dry_run = dry_run
        self.validator = RecordValidator()
        self.reports: List[CleaningReport] = []
        self.stats = {
            "total_processed": 0,
            "accepted": 0,
            "accepted_with_warnings": 0,
            "pending_review": 0,
            "rejected_incomplete": 0,
            "rejected_corrupted": 0,
            "rejected_non_scientific": 0,
        }

    def clean_source(self, source_name: str, limit: Optional[int] = None):
        """Limpia una fuente específica"""
        model_cls = SOURCE_MODELS.get(source_name)
        if not model_cls:
            logger.error(f"Fuente desconocida: {source_name}")
            return
        
        logger.info(f"🧹 Iniciando limpieza de {source_name}...")
        
        # Obtener registros pendientes
        query = self.session.query(model_cls).filter(
            model_cls.status == "pending"
        )
        
        if limit:
            query = query.limit(limit)
        
        records = query.all()
        logger.info(f"📥 Encontrados {len(records)} registros pendientes en {source_name}")
        
        for record in records:
            self._process_record(record, source_name)
        
        logger.info(f"✅ Limpieza de {source_name} completada")
        self._print_stats_summary()

    def _process_record(self, record: object, source_name: str):
        """Procesa un registro individual"""
        # Convertir SQLAlchemy model a dict
        record_dict = {
            "id": record.id,
            "source_name": source_name,
            "doi": record.doi,
            "title": record.title,
            "publication_year": record.publication_year,
            "authors_text": record.authors_text,
            "source_journal": record.source_journal,
            "issn": record.issn,
            "publication_type": record.publication_type,
            "language": getattr(record, "language", None),
            "is_open_access": record.is_open_access,
        }
        
        # Validar
        report = self.validator.validate(record_dict)
        self.reports.append(report)
        self.stats["total_processed"] += 1
        self.stats[report.decision.value] += 1
        
        # Actualizar registro
        if not self.dry_run:
            if report.decision == ValidationResult.ACCEPTED:
                record.status = 'accepted'
                logger.debug(f"✅ {record.id} aceptado")
            elif report.decision == ValidationResult.ACCEPTED_WITH_WARNINGS:
                record.status = 'accepted'
                logger.debug(f"⚠️  {record.id} aceptado con advertencias")
            elif report.decision == ValidationResult.PENDING_REVIEW:
                record.status = 'manual_review'
                logger.debug(f"👁️  {record.id} marcado para revisión")
            else:
                record.status = 'rejected'
                logger.debug(f"❌ {record.id} rechazado ({report.decision.value})")
        
        self.session.commit()

    def _print_stats_summary(self):
        """Imprime resumen de estadísticas"""
        logger.info("=" * 70)
        logger.info("📊 RESUMEN DE LIMPIEZA")
        logger.info("=" * 70)
        logger.info(f"Total procesados:           {self.stats['total_processed']}")
        logger.info(f"  ✅ Aceptados:             {self.stats['accepted']}")
        logger.info(f"  ⚠️  Aceptados (adv.):     {self.stats['accepted_with_warnings']}")
        logger.info(f"  👁️  Revisión manual:      {self.stats['pending_review']}")
        logger.info(f"  ❌ Rechazados (incomp.):  {self.stats['rejected_incomplete']}")
        logger.info(f"  ❌ Rechazados (corrupt):  {self.stats['rejected_corrupted']}")
        logger.info(f"  ❌ Rechazados (no-sci):   {self.stats['rejected_non_scientific']}")
        logger.info("=" * 70)

    def export_reports(self) -> str:
        """Exporta reportes a archivos CSV y JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reports_dir = Path(__file__).parent.parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        # CSV
        csv_file = reports_dir / f"clean_data_report_{timestamp}.csv"
        with open(csv_file, "w", encoding="utf-8") as f:
            f.write("id,source,doi,title,decision,reason,completeness,issues\n")
            for report in self.reports:
                issues_str = "; ".join(report.issues) if report.issues else ""
                f.write(
                    f'{report.source_record_id},'
                    f'{report.source_name},'
                    f'"{report.doi or ""}",'
                    f'"{report.title or ""}",'
                    f'{report.decision.value},'
                    f'"{report.reason}",'
                    f'{report.completeness_pct:.2%},'
                    f'"{issues_str}"\n'
                )
        logger.info(f"📄 Reporte CSV: {csv_file}")
        
        # JSON metrics
        json_file = reports_dir / f"clean_data_metrics_{timestamp}.json"
        import json
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(self.stats, f, indent=2)
        logger.info(f"📊 Métricas JSON: {json_file}")
        
        return str(reports_dir)


# =============================================================
# MAIN
# =============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Script de limpieza y validación de datos bibliográficos"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular limpieza sin actualizar BD"
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["openalex", "scopus", "wos", "cvlac", "datos_abiertos", "all"],
        default="all",
        help="Fuente a limpiar (default: todas)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limitar registros a procesar (default: todos)"
    )
    
    args = parser.parse_args()
    
    try:
        session = get_session()
        cleaner = DataCleaner(session, dry_run=args.dry_run)
        
        if args.dry_run:
            logger.info("🔍 Modo DRY RUN (sin cambios a BD)")
        
        sources = (
            ["openalex", "scopus", "wos", "cvlac", "datos_abiertos"]
            if args.source == "all"
            else [args.source]
        )
        
        for source in sources:
            cleaner.clean_source(source, limit=args.limit)
        
        cleaner.export_reports()
        
        logger.info("✅ Limpieza completada!")
        return 0
    
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
