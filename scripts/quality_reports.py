"""
Script de Reportes de Calidad de Datos.

DDD: Application Layer + Reporting
Genera métricas de cobertura, completitud y calidad de la BD reconciliada.

Métricas:
  - Cobertura de identificadores (DOI, PMID, ISSN, etc.)
  - Distribución temporal y por tipo de publicación
  - Tasas de éxito de reconciliación por fuente
  - Outliers y anomalías

Uso:
    python scripts/quality_reports.py [--output html|csv|json]

Salida:
    - reports/quality_metrics_{timestamp}.csv
    - reports/quality_report_{timestamp}.html (con gráficos)
"""

import logging
import argparse
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import func, and_
from sqlalchemy.orm import Session
from config import MatchType
from db.session import get_session
from db.models import (
    CanonicalPublication,
    Author,
    PublicationAuthor,
    OpenalexRecord,
    ScopusRecord,
    WosRecord,
    CvlacRecord,
    DatosAbiertosRecord,
    ReconciliationLog,
    SOURCE_MODELS,
)

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s │ %(levelname)-7s │ %(message)s"
))
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclass
class QualityMetrics:
    """Métricas de calidad consolidadas"""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    # Conteos
    total_canonical: int = 0
    total_authors: int = 0
    total_records_by_source: Dict[str, int] = field(default_factory=dict)
    
    # Cobertura de identificadores
    pct_with_doi: float = 0.0
    pct_with_pmid: float = 0.0
    pct_with_issn: float = 0.0
    pct_with_journal: float = 0.0
    pct_with_year: float = 0.0
    pct_with_type: float = 0.0
    pct_with_language: float = 0.0
    pct_open_access_known: float = 0.0  # % that have is_open_access != NULL
    
    # Reconciliación
    pct_doi_exact: float = 0.0
    pct_fuzzy: float = 0.0
    pct_manual_review: float = 0.0
    pct_new_canonical: float = 0.0
    
    # Autores
    total_author_affiliations: int = 0
    pct_authors_with_orcid: float = 0.0
    pct_authors_with_scopus_id: float = 0.0
    
    # Años
    min_year: Optional[int] = None
    max_year: Optional[int] = None
    avg_year: float = 0.0
    
    # Publicación por tipo
    types_distribution: Dict[str, int] = field(default_factory=dict)
    
    # Calidad general (0-100)
    quality_score: float = 0.0  # Composición ponderada de métricas
    
    # Alertas
    alerts: List[str] = field(default_factory=list)


class QualityReporter:
    """Generador de reportes de calidad"""

    def __init__(self, session: Session):
        self.session = session
        self.metrics = QualityMetrics()

    def generate_report(self) -> QualityMetrics:
        """Genera todas las métricas"""
        logger.info("📊 Generando métricas de calidad...")

        self._count_records()
        self._calculate_identifier_coverage()
        self._calculate_reconciliation_stats()
        self._calculate_author_stats()
        self._analyze_temporal_distribution()
        self._analyze_publication_types()
        self._detect_alerts()
        self._calculate_quality_score()

        return self.metrics

    def _count_records(self):
        """Conteo básico de registros"""
        self.metrics.total_canonical = self.session.query(
            CanonicalPublication
        ).count()
        
        self.metrics.total_authors = self.session.query(Author).count()
        
        for source_name, model_cls in SOURCE_MODELS.items():
            count = self.session.query(model_cls).count()
            self.metrics.total_records_by_source[source_name] = count
        
        logger.info(f"  📦 Total canónicas: {self.metrics.total_canonical}")
        logger.info(f"  👥 Total autores: {self.metrics.total_authors}")

    def _calculate_identifier_coverage(self):
        """Calcula cobertura de identificadores"""
        total = max(self.metrics.total_canonical, 1)
        
        with_doi = self.session.query(CanonicalPublication).filter(
            CanonicalPublication.doi.isnot(None)
        ).count()
        
        with_pmid = self.session.query(CanonicalPublication).filter(
            CanonicalPublication.pmid.isnot(None)
        ).count()
        
        with_issn = self.session.query(CanonicalPublication).filter(
            CanonicalPublication.issn.isnot(None)
        ).count()
        
        with_journal = self.session.query(CanonicalPublication).filter(
            CanonicalPublication.journal_id.isnot(None)
        ).count()
        
        with_year = self.session.query(CanonicalPublication).filter(
            CanonicalPublication.publication_year.isnot(None)
        ).count()
        
        with_type = self.session.query(CanonicalPublication).filter(
            CanonicalPublication.publication_type.isnot(None)
        ).count()
        
        with_language = self.session.query(CanonicalPublication).filter(
            CanonicalPublication.language.isnot(None)
        ).count()
        
        with_oa = self.session.query(CanonicalPublication).filter(
            CanonicalPublication.is_open_access.isnot(None)
        ).count()
        
        self.metrics.pct_with_doi = with_doi / total
        self.metrics.pct_with_pmid = with_pmid / total
        self.metrics.pct_with_issn = with_issn / total
        self.metrics.pct_with_journal = with_journal / total
        self.metrics.pct_with_year = with_year / total
        self.metrics.pct_with_type = with_type / total
        self.metrics.pct_with_language = with_language / total
        self.metrics.pct_open_access_known = with_oa / total
        
        logger.info(f"  📋 Cobertura:")
        logger.info(f"    DOI: {self.metrics.pct_with_doi:.1%}")
        logger.info(f"    PMID: {self.metrics.pct_with_pmid:.1%}")
        logger.info(f"    ISSN: {self.metrics.pct_with_issn:.1%}")

    def _calculate_reconciliation_stats(self):
        """Estadísticas de tipos de match en reconciliación"""
        total_logs = self.session.query(ReconciliationLog).count()
        
        if total_logs == 0:
            return
        
        doi_exact = self.session.query(ReconciliationLog).filter(
            ReconciliationLog.match_type == "doi_exact"
        ).count()
        
        fuzzy = self.session.query(ReconciliationLog).filter(
            ReconciliationLog.match_type == "fuzzy_combined"
        ).count()
        
        manual_review = self.session.query(ReconciliationLog).filter(
            ReconciliationLog.match_type == "manual_review"
        ).count()
        
        new = self.session.query(ReconciliationLog).filter(
            ReconciliationLog.match_type == "no_match"
        ).count()
        
        self.metrics.pct_doi_exact = doi_exact / total_logs if total_logs > 0 else 0
        self.metrics.pct_fuzzy = fuzzy / total_logs if total_logs > 0 else 0
        self.metrics.pct_manual_review = manual_review / total_logs if total_logs > 0 else 0
        self.metrics.pct_new_canonical = new / total_logs if total_logs > 0 else 0
        
        logger.info(f"  🔗 Reconciliación:")
        logger.info(f"    DOI exacto: {self.metrics.pct_doi_exact:.1%}")
        logger.info(f"    Fuzzy: {self.metrics.pct_fuzzy:.1%}")

    def _calculate_author_stats(self):
        """Estadísticas de autores"""
        total_authors = max(self.metrics.total_authors, 1)
        
        with_orcid = self.session.query(Author).filter(
            Author.orcid.isnot(None)
        ).count()
        
        with_scopus = self.session.query(Author).filter(
            Author.external_ids.has_key("scopus")
        ).count()
        
        self.metrics.pct_authors_with_orcid = with_orcid / total_authors
        self.metrics.pct_authors_with_scopus_id = with_scopus / total_authors
        
        # Total de afiliaciones (authors que tienen al menos una AuthorInstitution)
        from db.models import AuthorInstitution
        authors_with_affiliations = self.session.query(Author).join(
            AuthorInstitution, Author.id == AuthorInstitution.author_id,
            isouter=True
        ).filter(AuthorInstitution.id.isnot(None)).distinct().count()
        self.metrics.total_author_affiliations = authors_with_affiliations
        
        logger.info(f"  👥 Autores:")
        logger.info(f"    Con ORCID: {self.metrics.pct_authors_with_orcid:.1%}")

    def _analyze_temporal_distribution(self):
        """Análisis temporal de publicaciones"""
        result = self.session.query(
            func.min(CanonicalPublication.publication_year),
            func.max(CanonicalPublication.publication_year),
            func.avg(CanonicalPublication.publication_year),
        ).filter(
            CanonicalPublication.publication_year.isnot(None)
        ).first()
        
        if result[0]:
            self.metrics.min_year = int(result[0])
            self.metrics.max_year = int(result[1])
            self.metrics.avg_year = float(result[2]) if result[2] else 0.0
        
        logger.info(f"  📆 Temporal:")
        logger.info(f"    Rango: {self.metrics.min_year}-{self.metrics.max_year}")

    def _analyze_publication_types(self):
        """Distribución de tipos de publicación"""
        types = self.session.query(
            CanonicalPublication.publication_type,
            func.count(CanonicalPublication.id),
        ).filter(
            CanonicalPublication.publication_type.isnot(None)
        ).group_by(
            CanonicalPublication.publication_type
        ).all()
        
        for pub_type, count in types:
            self.metrics.types_distribution[pub_type] = count

    def _detect_alerts(self):
        """Detecta anomalías y genera alertas"""
        # Alerta: Muy pocas con DOI
        if self.metrics.pct_with_doi < 0.5:
            self.metrics.alerts.append(
                f"⚠️  Baja cobertura DOI ({self.metrics.pct_with_doi:.1%})"
            )
        
        # Alerta: Muchas con año NULL
        if self.metrics.pct_with_year < 0.9:
            self.metrics.alerts.append(
                f"⚠️  {(1-self.metrics.pct_with_year):.1%} publicaciones sin año"
            )
        
        # Alerta: Autores sin identificadores
        if self.metrics.pct_authors_with_orcid < 0.2:
            self.metrics.alerts.append(
                f"⚠️  Baja cobertura ORCID en autores ({self.metrics.pct_authors_with_orcid:.1%})"
            )
        
        # Alerta: Muchas reconciliaciones manually review
        if self.metrics.pct_manual_review > 0.3:
            self.metrics.alerts.append(
                f"⚠️  Alto porcentaje de revisiones manuales ({self.metrics.pct_manual_review:.1%})"
            )

    def _calculate_quality_score(self):
        """Calcula puntuación de calidad general (0-100)"""
        # Pesos de cada meta
        weights = {
            "doi": 0.20,
            "year": 0.15,
            "issn": 0.15,
            "journal": 0.15,
            "type": 0.10,
            "orcid": 0.10,
            "oa_known": 0.10,
            "reconcilied": 0.05,
        }
        
        score = (
            self.metrics.pct_with_doi * weights["doi"] * 100 +
            self.metrics.pct_with_year * weights["year"] * 100 +
            self.metrics.pct_with_issn * weights["issn"] * 100 +
            self.metrics.pct_with_journal * weights["journal"] * 100 +
            self.metrics.pct_with_type * weights["type"] * 100 +
            self.metrics.pct_authors_with_orcid * weights["orcid"] * 100 +
            self.metrics.pct_open_access_known * weights["oa_known"] * 100 +
            (1 - self.metrics.pct_manual_review) * weights["reconcilied"] * 100
        )
        
        self.metrics.quality_score = min(score, 100.0)
        logger.info(f"  🎯 Calidad general: {self.metrics.quality_score:.1f}/100")

    def export_csv(self) -> str:
        """Exporta métricas a CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reports_dir = Path(__file__).parent.parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        csv_file = reports_dir / f"quality_metrics_{timestamp}.csv"
        
        with open(csv_file, "w", encoding="utf-8") as f:
            f.write("Métrica,Valor\n")
            f.write(f"Timestamp,{self.metrics.timestamp}\n")
            f.write(f"Total publicaciones canónicas,{self.metrics.total_canonical}\n")
            f.write(f"Total autores,{self.metrics.total_authors}\n")
            f.write(f"% con DOI,{self.metrics.pct_with_doi:.2%}\n")
            f.write(f"% con ISSN,{self.metrics.pct_with_issn:.2%}\n")
            f.write(f"% con Open Access info,{self.metrics.pct_open_access_known:.2%}\n")
            f.write(f"% DOI exact match,{self.metrics.pct_doi_exact:.2%}\n")
            f.write(f"% Fuzzy match,{self.metrics.pct_fuzzy:.2%}\n")
            f.write(f"Calidad general,{self.metrics.quality_score:.1f}/100\n")
        
        logger.info(f"📄 CSV: {csv_file}")
        return str(csv_file)

    def export_html(self) -> str:
        """Exporta reporte HTML con visualizaciones"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        reports_dir = Path(__file__).parent.parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        
        html_file = reports_dir / f"quality_report_{timestamp}.html"
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Reporte de Calidad — Reconciliación Bibliográfica</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; }}
        h1 {{ color: #333; }}
        .metric {{ display: inline-block; margin: 10px; padding: 15px; background: #f0f0f0; border-radius: 5px; min-width: 200px; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #0066cc; }}
        .metric-label {{ font-size: 12px; color: #666; }}
        .alert {{ background: #fff3cd; padding: 10px; margin: 5px 0; border-radius: 3px; border-left: 4px solid #ffc107; }}
        .alert span {{ color: #d39e00; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        table th, table td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
        table th {{ background: #f5f5f5; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Reporte de Calidad de Datos</h1>
        <p>Generado: {self.metrics.timestamp}</p>
        
        <h2>Indicadores Principales</h2>
        <div>
            <div class="metric">
                <div class="metric-value">{self.metrics.total_canonical}</div>
                <div class="metric-label">Publicaciones Canónicas</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.metrics.total_authors}</div>
                <div class="metric-label">Autores Únicos</div>
            </div>
            <div class="metric">
                <div class="metric-value">{self.metrics.quality_score:.1f}</div>
                <div class="metric-label">Calidad General (0-100)</div>
            </div>
        </div>
        
        <h2>Cobertura de Identificadores</h2>
        <table>
            <tr><th>Identificador</th><th>Cobertura</th></tr>
            <tr><td>DOI</td><td>{self.metrics.pct_with_doi:.1%}</td></tr>
            <tr><td>PMID</td><td>{self.metrics.pct_with_pmid:.1%}</td></tr>
            <tr><td>ISSN</td><td>{self.metrics.pct_with_issn:.1%}</td></tr>
            <tr><td>Año</td><td>{self.metrics.pct_with_year:.1%}</td></tr>
            <tr><td>Tipo</td><td>{self.metrics.pct_with_type:.1%}</td></tr>
            <tr><td>Open Access Info</td><td>{self.metrics.pct_open_access_known:.1%}</td></tr>
        </table>
        
        <h2>Distribución de Tipos de Publicación</h2>
        <table>
            <tr><th>Tipo</th><th>Cantidad</th></tr>
"""
        
        for pub_type, count in sorted(
            self.metrics.types_distribution.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]:
            html_content += f"            <tr><td>{pub_type}</td><td>{count}</td></tr>\n"
        
        html_content += """
        </table>
        
        <h2>Alertas</h2>
"""
        
        if self.metrics.alerts:
            for alert in self.metrics.alerts:
                html_content += f'        <div class="alert"><span>{alert}</span></div>\n'
        else:
            html_content += '        <div style="color: green;">✅ No hay alertas críticas</div>\n'
        
        html_content += """
    </div>
</body>
</html>
"""
        
        with open(html_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        logger.info(f"📊 HTML: {html_file}")
        return str(html_file)


def main():
    parser = argparse.ArgumentParser(
        description="Generar reportes de calidad de datos"
    )
    parser.add_argument(
        "--output",
        choices=["csv", "html", "both"],
        default="both",
        help="Formato de salida"
    )
    
    args = parser.parse_args()
    
    try:
        session = get_session()
        reporter = QualityReporter(session)
        
        metrics = reporter.generate_report()
        
        if args.output in ["csv", "both"]:
            reporter.export_csv()
        
        if args.output in ["html", "both"]:
            reporter.export_html()
        
        logger.info("✅ Reportes generados!")
        return 0
    
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
