"""
Servicio de métricas generales del autor desde el inventario local.

Calcula todas las estadísticas bibliométricas sin conectarse a APIs externas.
"""

import logging
import time
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session

from db.models import Author, CanonicalPublication, PublicationAuthor, Journal
from api.schemas.author_metrics import (
    AuthorGeneralMetricsResponse,
    GeneralMetrics,
    PlatformMetrics,
    YearMetrics,
    PublicationTypeDistribution,
    JournalMetrics,
    OpenAccessMetrics,
)

logger = logging.getLogger(__name__)

# ── Cache en memoria para métricas de autores ─────────────────────────────────
# Estructura: {author_id: (resultado, timestamp_expiracion)}
# TTL por defecto: 10 minutos. Se invalida automáticamente al vencer.
_METRICS_CACHE: Dict[int, Tuple[AuthorGeneralMetricsResponse, float]] = {}
_CACHE_TTL_SECONDS = 600  # 10 minutos


def _cache_get(author_id: int) -> Optional[AuthorGeneralMetricsResponse]:
    """Retorna el resultado cacheado si aún es válido, None si expiró o no existe."""
    entry = _METRICS_CACHE.get(author_id)
    if entry is None:
        return None
    result, expires_at = entry
    if time.monotonic() > expires_at:
        del _METRICS_CACHE[author_id]
        return None
    return result


def _cache_set(author_id: int, result: AuthorGeneralMetricsResponse) -> None:
    _METRICS_CACHE[author_id] = (result, time.monotonic() + _CACHE_TTL_SECONDS)


def invalidate_author_metrics_cache(author_id: int) -> None:
    """Invalida el cache para un autor específico (llamar tras merge o enrich)."""
    _METRICS_CACHE.pop(author_id, None)


def clear_metrics_cache() -> int:
    """Limpia todo el cache. Retorna el número de entradas eliminadas."""
    count = len(_METRICS_CACHE)
    _METRICS_CACHE.clear()
    return count


class AuthorMetricsService:
    """Servicio para calcular métricas generales de autores"""

    @staticmethod
    def get_author_metrics(author_id: int, db: Session) -> AuthorGeneralMetricsResponse:
        """
        Obtiene todas las métricas de un autor desde el inventario local.
        
        Args:
            author_id: ID del autor en la BD
            db: Sesión de SQLAlchemy
            
        Returns:
            AuthorGeneralMetricsResponse con todas las estadísticas
            
        Raises:
            ValueError: Si el autor no existe o no tiene publicaciones
        """
        # ── Cache hit ─────────────────────────────────────────────────────────
        cached = _cache_get(author_id)
        if cached is not None:
            logger.debug("Cache hit para métricas del autor %s", author_id)
            return cached

        # 1. Buscar autor
        author = db.query(Author).filter(Author.id == author_id).first()
        if not author:
            raise ValueError(f"Autor con ID {author_id} no existe")

        logger.info(f"Analizando métricas del autor: {author.name}")
        
        # 2. Obtener publicaciones del autor
        publications = db.query(CanonicalPublication).join(
            PublicationAuthor
        ).filter(
            PublicationAuthor.author_id == author_id
        ).all()
        
        if not publications:
            raise ValueError(f"El autor '{author.name}' no tiene publicaciones en el inventario")
        
        total_pubs = len(publications)
        logger.info(f"Autor {author.name} tiene {total_pubs} publicaciones")
        
        # 3. Calcular métricas
        platform_metrics = AuthorMetricsService._calculate_platform_metrics(publications)
        general_metrics = AuthorMetricsService._calculate_general_metrics(publications)
        publications_by_year = AuthorMetricsService._calculate_publications_by_year(publications)
        publication_types = AuthorMetricsService._calculate_publication_types(publications)
        top_journals = AuthorMetricsService._calculate_top_journals(publications, db)
        open_access = AuthorMetricsService._calculate_open_access(publications)
        languages = AuthorMetricsService._calculate_languages(publications)

        institutional_pubs = sum(1 for p in publications if p.institutional_authors_count > 0)

        # 4. Construir respuesta
        response = AuthorGeneralMetricsResponse(
            author_id=author_id,
            author_name=author.name,
            orcid=author.orcid,
            platforms=platform_metrics,
            general_metrics=general_metrics,
            publications_by_year=publications_by_year,
            publication_types=publication_types,
            top_journals=top_journals,
            open_access=open_access,
            institutional_publications=institutional_pubs,
            languages=languages
        )
        
        logger.info(f"Métricas de {author.name} calculadas exitosamente")

        # ── Guardar en cache ──────────────────────────────────────────────────
        _cache_set(author_id, response)
        return response
    
    @staticmethod
    def _calculate_general_metrics(publications: List) -> GeneralMetrics:
        """Calcula métricas generales de productividad e impacto"""
        
        total_pubs = len(publications)
        total_citations = sum(p.citation_count or 0 for p in publications)
        
        # Años activos
        years = sorted(set(p.publication_year for p in publications if p.publication_year and p.publication_year > 0))
        years_active_from = min(years) if years else None
        years_active_to = max(years) if years else None
        years_active_count = len(years) if years else 0
        
        # H-index
        citations_sorted = sorted([p.citation_count or 0 for p in publications], reverse=True)
        h_index = sum(1 for i, c in enumerate(citations_sorted, 1) if c >= i)
        
        # Promedios
        cpp = round(total_citations / total_pubs, 2) if total_pubs > 0 else 0
        avg_pubs_per_year = round(total_pubs / years_active_count, 2) if years_active_count > 0 else 0
        max_citations = max([p.citation_count or 0 for p in publications])
        
        return GeneralMetrics(
            total_publications=total_pubs,
            total_citations=total_citations,
            years_active_from=years_active_from,
            years_active_to=years_active_to,
            years_active_count=years_active_count,
            avg_publications_per_year=avg_pubs_per_year,
            h_index=h_index,
            cpp=cpp,
            most_cited_pub_count=max_citations
        )
    
    @staticmethod
    def _calculate_publications_by_year(publications: List) -> List[YearMetrics]:
        """Calcula publicaciones y citas por año"""
        
        by_year = {}
        for pub in publications:
            year = pub.publication_year or 0
            if year > 0:
                if year not in by_year:
                    by_year[year] = {'count': 0, 'citations': 0}
                by_year[year]['count'] += 1
                by_year[year]['citations'] += pub.citation_count or 0
        
        return [
            YearMetrics(
                year=year,
                publications_count=by_year[year]['count'],
                citations_count=by_year[year]['citations'],
                avg_citations_per_pub=round(
                    by_year[year]['citations'] / by_year[year]['count'], 2
                )
            )
            for year in sorted(by_year.keys(), reverse=True)
        ]
    
    @staticmethod
    def _calculate_publication_types(publications: List) -> List[PublicationTypeDistribution]:
        """Distribuye publicaciones por tipo"""
        
        types_dict = {}
        for pub in publications:
            ptype = pub.publication_type or "Unknown"
            types_dict[ptype] = types_dict.get(ptype, 0) + 1
        
        total = len(publications)
        return [
            PublicationTypeDistribution(
                type_name=ptype,
                count=count,
                percentage=round((count / total) * 100, 2)
            )
            for ptype, count in sorted(types_dict.items(), key=lambda x: x[1], reverse=True)
        ]
    
    @staticmethod
    def _calculate_top_journals(publications: List, db: Session) -> List[JournalMetrics]:
        """Calcula top 10 revistas por número de publicaciones"""
        
        journals_dict = {}
        journals_citations = {}
        journals_issn = {}
        
        for pub in publications:
            journal_name = pub.source_journal or "Unknown"
            journals_dict[journal_name] = journals_dict.get(journal_name, 0) + 1
            journals_citations[journal_name] = journals_citations.get(journal_name, 0) + (pub.citation_count or 0)
            
            # Obtener ISSN si existe
            if pub.journal_id and journal_name not in journals_issn:
                j = db.query(Journal).filter(Journal.id == pub.journal_id).first()
                if j:
                    journals_issn[journal_name] = j.issn
        
        return [
            JournalMetrics(
                journal_name=journal,
                issn=journals_issn.get(journal),
                publications_count=journals_dict[journal],
                total_citations=journals_citations[journal],
                avg_citations=round(journals_citations[journal] / journals_dict[journal], 2)
            )
            for journal in sorted(journals_dict.keys(), key=lambda j: journals_dict[j], reverse=True)[:10]
        ]
    
    @staticmethod
    def _calculate_open_access(publications: List) -> OpenAccessMetrics:
        """Calcula estadísticas de acceso abierto"""
        
        pubs_with_oa = sum(1 for p in publications if p.is_open_access is not None)
        open_access_count = sum(1 for p in publications if p.is_open_access)
        
        oa_percentage = round((open_access_count / pubs_with_oa * 100), 2) if pubs_with_oa > 0 else 0
        
        # Distribución de estados OA
        oa_status_dict = {}
        for pub in publications:
            if pub.oa_status:
                oa_status_dict[pub.oa_status] = oa_status_dict.get(pub.oa_status, 0) + 1
        
        return OpenAccessMetrics(
            total_with_oa_status=pubs_with_oa,
            open_access_count=open_access_count,
            open_access_percentage=oa_percentage,
            oa_status_distribution=oa_status_dict
        )
    
    @staticmethod
    def _calculate_languages(publications: List) -> Dict[str, int]:
        """Calcula distribución de idiomas"""

        languages = {}
        for pub in publications:
            lang = pub.language or "unknown"
            languages[lang] = languages.get(lang, 0) + 1

        return languages

    @staticmethod
    def _calculate_platform_metrics(publications: List) -> List[PlatformMetrics]:
        """Calcula métricas por plataforma usando citations_by_source."""

        # Recolectar plataformas presentes en las publicaciones del autor
        all_platforms: set = set()
        for pub in publications:
            if pub.citations_by_source:
                all_platforms.update(pub.citations_by_source.keys())

        result = []
        for platform in sorted(all_platforms):
            platform_pubs = [
                p for p in publications
                if p.citations_by_source and platform in p.citations_by_source
            ]
            if not platform_pubs:
                continue

            total_pubs = len(platform_pubs)
            cit_list = [platform_pubs[i].citations_by_source[platform] or 0 for i in range(total_pubs)]
            total_citations = sum(cit_list)
            sorted_cits = sorted(cit_list, reverse=True)
            h_index = sum(1 for i, c in enumerate(sorted_cits, 1) if c >= i)
            cpp = round(total_citations / total_pubs, 2) if total_pubs > 0 else 0

            result.append(PlatformMetrics(
                platform=platform,
                total_publications=total_pubs,
                total_citations=total_citations,
                h_index=h_index,
                cpp=cpp,
            ))

        return result
