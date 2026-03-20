"""
Servicio unificado que orquesta la extracción de múltiples plataformas.

Responsabilidad:
  - Tomar un autor por ID o ORCID
  - Si es ORCID: detectar si es institucional, crear autor si aplica
  - Extraer sus identificadores (Scopus, WOS, OpenAlex, CVLac, ORCID)
  - Ejecutar todos los extractores en paralelo
  - Consolidar la información en un perfil unificado
  - Manejar errores sin romper el flujo completo
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy.orm import Session
import pyalex
from pyalex import Works

from db.models import Author, CanonicalPublication, PublicationAuthor
from extractors.base import StandardRecord
from extractors.scopus import ScopusExtractor, ScopusAPIError
from extractors.wos import WosExtractor, WosAPIError
from extractors.cvlac import CvlacExtractor, CvlacScrapingError
from extractors.datos_abiertos import DatosAbiertosExtractor, DatosAbiertosError
from extractors.openalex.extractor import OpenAlexExtractor
from config import institution

logger = logging.getLogger(__name__)


# =============================================================================
# SCHEMAS DE RESPUESTA
# =============================================================================

@dataclass
class PlatformExtractionResult:
    """Resultado de extracción de una plataforma individual"""
    platform: str  # scopus, wos, openalex, cvlac, datos_abiertos
    success: bool
    records_count: int = 0
    records: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None
    extracted_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class UnifiedAuthorProfile:
    """Perfil consolidado del autor con información de todas las fuentes"""
    author_id: int
    author_name: str
    orcid: Optional[str] = None
    is_institutional: bool = False
    
    # Datos del autor consolidados
    author_data: Dict[str, Any] = field(default_factory=dict)  # h_index, citations, etc.
    
    # Identificadores por plataforma
    identifiers: Dict[str, Optional[str]] = field(default_factory=dict)
    
    # Resultados por plataforma
    platform_results: Dict[str, PlatformExtractionResult] = field(default_factory=dict)
    
    # Consolidado
    total_publications: int = 0
    total_citations: int = 0
    platforms_with_data: List[str] = field(default_factory=list)
    extraction_summary: str = ""
    
    # Timestamps
    extracted_at: str = field(default_factory=lambda: datetime.now().isoformat())


# =============================================================================
# SERVICIO UNIFICADO
# =============================================================================

class UnifiedExtractorService:
    """
    Orquesta la extracción de múltiples plataformas para un autor.
    
    Soporta dos modos:
    1. Por ID de autor (BD)
    2. Por ORCID (detecta si es institucional, crea autor si aplica)
    
    Ejemplo:
        service = UnifiedExtractorService(db)
        # Modo 1: Por ID
        profile = service.extract_author_profile(author_id=123)
        
        # Modo 2: Por ORCID
        profile = service.extract_author_profile(orcid="0000-0001-8757-3778")
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.affiliations = None  # Para Scopus, puede venir de config
        
    def extract_author_profile(
        self,
        author_id: Optional[int] = None,
        orcid: Optional[str] = None,
        include_platforms: Optional[List[str]] = None,
    ) -> UnifiedAuthorProfile:
        """
        Extrae información del autor de todas las plataformas disponibles.
        
        Args:
            author_id: ID del autor en la BD (mutuamente exclusivo con orcid)
            orcid: ORCID del autor (si no existe, detecta si es institucional y lo crea)
            include_platforms: Lista de plataformas a incluir
                              
        Returns:
            UnifiedAuthorProfile con resultados de cada plataforma
        """
        
        # 1. Resolver autor (por ID o ORCID)
        author = None
        
        if author_id:
            author = self.db.query(Author).filter(Author.id == author_id).first()
            if not author:
                raise ValueError(f"Autor con ID {author_id} no encontrado")
        
        elif orcid:
            # Buscar autor por ORCID
            author = self.db.query(Author).filter(Author.orcid == orcid).first()
            
            # Si no existe, detectar si es institucional y crear
            if not author:
                logger.info(f"Autor con ORCID {orcid} no existe. Detectando...")
                
                is_institutional, identifiers = self._detect_institutional_author(orcid)
                
                if not is_institutional:
                    raise ValueError(
                        f"ORCID {orcid} no tiene afiliaciones institucionales detectadas. "
                        "Solo se pueden crear autores institucionales."
                    )
                
                logger.info(f"✓ Autor {orcid} detectado como institucional. Creando...")
                author = self._create_author_from_orcid(orcid, identifiers)
                
        else:
            raise ValueError("Debes proporcionar 'author_id' o 'orcid'")
        
        if not author:
            raise ValueError("No se pudo resolver el autor")
        
        logger.info(f"Extrayendo perfil unificado para: {author.name}")
        
        # 2. Crear perfil base
        profile = UnifiedAuthorProfile(
            author_id=author.id,
            author_name=author.name,
            orcid=author.orcid,
            is_institutional=author.is_institutional,
            identifiers={
                'scopus_id': author.scopus_id,
                'wos_id': author.wos_id,
                'openalex_id': author.openalex_id,
                'cvlac_id': author.cvlac_id,
                'orcid': author.orcid,
            }
        )
        
        # 3. Determinar qué plataformas incluir
        if include_platforms is None:
            include_platforms = self._determine_available_platforms(author)
        
        logger.info(f"Plataformas a consultar: {include_platforms}")
        
        # 4. Ejecutar extracciones
        for platform in include_platforms:
            try:
                result = self._extract_from_platform(author, platform)
                profile.platform_results[platform] = result
                
                if result.success:
                    profile.platforms_with_data.append(platform)
                    profile.total_publications += result.records_count
                    
                    # Contar citas
                    for record in result.records:
                        if isinstance(record, dict):
                            profile.total_citations += int(record.get('citation_count', 0) or 0)
                        elif isinstance(record, StandardRecord):
                            profile.total_citations += record.citation_count
                            
                    logger.info(
                        f"✓ {platform}: {result.records_count} publicaciones extraídas"
                    )
                else:
                    logger.warning(f"✗ {platform}: {result.error}")
                    
            except Exception as e:
                logger.error(f"Error inesperado en {platform}: {e}", exc_info=True)
                profile.platform_results[platform] = PlatformExtractionResult(
                    platform=platform,
                    success=False,
                    error=str(e),
                )
        
        # 5. Extraer datos del autor y guardar en BD
        logger.info("Extrayendo datos del autor desde plataformas...")
        author_data = self._extract_author_data(author)
        profile.author_data = author_data
        
        # Guardar datos del autor en BD
        if author_data:
            self._save_author_data(author, author_data)
        
        # 6. Generar resumen
        profile.extraction_summary = self._generate_summary(profile)
        
        logger.info(f"Extracción completada: {profile.extraction_summary}")
        
        return profile
    
    # ─────────────────────────────────────────────────────────────────────────
    # MÉTODOS PRIVADOS
    # ─────────────────────────────────────────────────────────────────────────
    
    def _detect_institutional_author(self, orcid: str) -> Tuple[bool, Dict[str, Optional[str]]]:
        """
        Detecta si un autor con ORCID tiene afiliaciones institucionales.
        
        Busca en Scopus y OpenAlex por ORCID.
        Si tiene afiliaciones en AMBAS plataformas → es institucional
        
        Returns:
            (is_institutional: bool, identifiers: dict con scopus_id, openalex_id, name)
        """
        identifiers = {
            'orcid': orcid,
            'scopus_id': None,
            'openalex_id': None,
            'name': None,
        }
        
        has_scopus_affiliation = False
        has_openalex_affiliation = False
        
        # ─ Buscar en Scopus por ORCID
        try:
            logger.info(f"Buscando en Scopus por ORCID: {orcid}")
            scopus_extractor = ScopusExtractor()
            query = f"ORCID({orcid})"
            scopus_records = scopus_extractor.extract(query=query)
            
            if scopus_records:
                # Obtener ID de Scopus del primer registro
                for record in scopus_records:
                    if hasattr(record, 'source_id'):
                        identifiers['scopus_id'] = record.source_id
                        break
                
                # Verificar si tiene afiliaciones
                for record in scopus_records:
                    if isinstance(record, StandardRecord):
                        if record.authors:
                            for author_data in record.authors:
                                if author_data.get('is_institutional'):
                                    has_scopus_affiliation = True
                                    if not identifiers['name'] and author_data.get('name'):
                                        identifiers['name'] = author_data['name']
                                    break
                    if has_scopus_affiliation:
                        break
                
                logger.info(f"Scopus: {len(scopus_records)} registros | Afiliación: {has_scopus_affiliation}")
        except Exception as e:
            logger.warning(f"Error detectando en Scopus: {e}")
        
        # ─ Buscar en OpenAlex por ORCID usando PyAlex directamente
        try:
            logger.info(f"Buscando en OpenAlex por ORCID: {orcid}")
            
            # Usar PyAlex directamente para buscar por ORCID
            # Configurar polite pool
            pyalex.config.email = institution.contact_email
            
            # Filtrar works por ORCID del autor
            query = Works().filter(
                authorships={"author": {"orcid": f"https://orcid.org/{orcid}"}}
            )
            
            openalex_records_raw = []
            for page in query.paginate(per_page=100, n_max=100):
                works_list = page.results if hasattr(page, 'results') else (
                    list(page) if hasattr(page, '__iter__') else [page]
                )
                openalex_records_raw.extend(works_list)
            
            if openalex_records_raw:
                logger.info(f"OpenAlex: {len(openalex_records_raw)} registros encontrados")
                
                # Parsear registros usando OpenAlexExtractor
                extractor = OpenAlexExtractor()
                openalex_records = [
                    extractor._parse_record(work) 
                    for work in openalex_records_raw
                ]
                
                # Obtener ID de OpenAlex del primer registro
                for record in openalex_records:
                    if hasattr(record, 'source_id'):
                        identifiers['openalex_id'] = record.source_id
                        break
                
                # Verificar si tiene afiliaciones institucionales
                for record in openalex_records:
                    if isinstance(record, StandardRecord):
                        if record.institutional_authors:
                            has_openalex_affiliation = True
                            if not identifiers['name'] and record.institutional_authors:
                                auth = record.institutional_authors[0]
                                if isinstance(auth, dict) and auth.get('name'):
                                    identifiers['name'] = auth['name']
                            break
                
                logger.info(f"OpenAlex: Afiliación: {has_openalex_affiliation}")
        except Exception as e:
            logger.warning(f"Error detectando en OpenAlex: {e}")
        
        # ─ Determinar si es institucional (ambas plataformas)
        is_institutional = has_scopus_affiliation and has_openalex_affiliation
        
        logger.info(
            f"Detección completada: "
            f"Scopus={has_scopus_affiliation}, OpenAlex={has_openalex_affiliation} → "
            f"Institucional={is_institutional}"
        )
        
        return is_institutional, identifiers
    
    def _create_author_from_orcid(self, orcid: str, identifiers: Dict[str, Optional[str]]) -> Author:
        """
        Crea un nuevo registro de autor en la BD basado en ORCID e identificadores.
        
        Args:
            orcid: ORCID del autor
            identifiers: Dict con scopus_id, openalex_id, name, etc.
            
        Returns:
            Nuevo objeto Author creado y guardado
        """
        author = Author(
            name=identifiers.get('name') or f"Author {orcid}",
            orcid=orcid,
            scopus_id=identifiers.get('scopus_id'),
            openalex_id=identifiers.get('openalex_id'),
            is_institutional=True,
            field_provenance={
                'orcid': 'user',
                'scopus_id': 'scopus' if identifiers.get('scopus_id') else None,
                'openalex_id': 'openalex' if identifiers.get('openalex_id') else None,
            },
        )
        
        self.db.add(author)
        self.db.commit()
        self.db.refresh(author)
        
        logger.info(f"✓ Autor creado: {author.name} (ID: {author.id}, ORCID: {orcid})")
        
        return author
    
    def _determine_available_platforms(self, author: Author) -> List[str]:
        """Determina qué plataformas se pueden consultar según IDs disponibles"""
        available = []
        
        if author.scopus_id:
            available.append('scopus')
        if author.wos_id:
            available.append('wos')
        if author.openalex_id:
            available.append('openalex')
        if author.cvlac_id:
            available.append('cvlac')
        if author.orcid:
            available.append('openalex')  # OpenAlex también busca por ORCID
            
        # OpenAlex casi siempre está disponible (por ROR de institución)
        if 'openalex' not in available:
            available.append('openalex')
        
        # Datos Abiertos Colombia (siempre disponible para búsqueda)
        available.append('datos_abiertos')
        
        return available
    
    # ─────────────────────────────────────────────────────────────────────────
    # EXTRACCIÓN DE DATOS DEL AUTOR
    # ─────────────────────────────────────────────────────────────────────────
    
    def _extract_author_data(self, author: Author) -> Dict[str, Any]:
        """
        Extrae datos del perfil del autor desde Scopus y OpenAlex.
        
        Returns:
            Dict con: h_index, total_citations, years_active, name, etc.
        """
        author_data = {
            'scopus_profile': None,
            'openalex_profile': None,
            'consolidated': {},
        }
        
        # ─ Obtener datos de Scopus
        if author.scopus_id:
            try:
                logger.info(f"Obteniendo datos del autor desde Scopus...")
                scopus_data = self._get_scopus_author_profile(author.scopus_id)
                if scopus_data:
                    author_data['scopus_profile'] = scopus_data
                    logger.info(f"✓ Datos Scopus obtenidos")
            except Exception as e:
                logger.warning(f"Error obteniendo datos Scopus: {e}")
        
        # ─ Obtener datos de OpenAlex
        if author.openalex_id or author.orcid:
            try:
                logger.info(f"Obteniendo datos del autor desde OpenAlex...")
                openalex_data = self._get_openalex_author_profile(
                    author.openalex_id, 
                    author.orcid
                )
                if openalex_data:
                    author_data['openalex_profile'] = openalex_data
                    logger.info(f"✓ Datos OpenAlex obtenidos")
            except Exception as e:
                logger.warning(f"Error obteniendo datos OpenAlex: {e}")
        
        # ─ Consolidar datos
        if author_data['scopus_profile'] or author_data['openalex_profile']:
            author_data['consolidated'] = self._consolidate_author_data(author_data)
        
        return author_data
    
    def _get_scopus_author_profile(self, scopus_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene datos del perfil del autor desde Scopus.
        
        Returns:
            Dict con h_index, total_citations, years_active, etc.
        """
        try:
            extractor = ScopusExtractor()
            
            # Buscar publicaciones del autor en Scopus
            query = f"AU-ID({scopus_id})"
            records = extractor.extract(query=query)
            
            if not records:
                return None
            
            # Calcular estadísticas
            h_index = 0
            total_citations = 0
            years = set()
            author_name = None
            
            citations_list = []
            for record in records:
                if isinstance(record, StandardRecord):
                    citations = record.citation_count or 0
                    citations_list.append(citations)
                    total_citations += citations
                    
                    if record.publication_year:
                        years.add(record.publication_year)
                    
                    if not author_name and record.authors:
                        for auth in record.authors:
                            if isinstance(auth, dict) and auth.get('name'):
                                author_name = auth['name']
                                break
            
            # Calcular H-index
            if citations_list:
                citations_list.sort(reverse=True)
                for i, c in enumerate(citations_list, 1):
                    if c >= i:
                        h_index = i
            
            year_range = None
            if years:
                year_list = sorted(years)
                year_range = f"{year_list[0]}-{year_list[-1]}"
            
            return {
                'h_index': h_index,
                'total_citations': total_citations,
                'total_publications': len(records),
                'years_active': year_range,
                'author_name': author_name,
                'source': 'scopus',
            }
            
        except Exception as e:
            logger.error(f"Error en _get_scopus_author_profile: {e}")
            return None
    
    def _get_openalex_author_profile(
        self, 
        openalex_id: Optional[str],
        orcid: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """
        Obtiene datos del perfil del autor desde OpenAlex.
        
        Returns:
            Dict con h_index, total_citations, years_active, etc.
        """
        try:
            extractor = OpenAlexExtractor()
            
            records = []
            author_name = None
            
            # Intentar por OpenAlex ID
            if openalex_id:
                try:
                    pyalex.config.email = institution.contact_email
                    query = Works().filter(
                        authorships={"author": {"id": openalex_id}}
                    )
                    
                    for page in query.paginate(per_page=100, n_max=500):
                        works_list = page.results if hasattr(page, 'results') else (
                            list(page) if hasattr(page, '__iter__') else [page]
                        )
                        for work in works_list:
                            record = extractor._parse_record(work)
                            if record:
                                records.append(record)
                except Exception as e:
                    logger.warning(f"OpenAlex by ID failed: {e}")
            
            # Si no hay registros, intentar por ORCID
            if not records and orcid:
                try:
                    pyalex.config.email = institution.contact_email
                    query = Works().filter(
                        authorships={"author": {"orcid": f"https://orcid.org/{orcid}"}}
                    )
                    
                    for page in query.paginate(per_page=100, n_max=500):
                        works_list = page.results if hasattr(page, 'results') else (
                            list(page) if hasattr(page, '__iter__') else [page]
                        )
                        for work in works_list:
                            record = extractor._parse_record(work)
                            if record:
                                records.append(record)
                except Exception as e:
                    logger.warning(f"OpenAlex by ORCID failed: {e}")
            
            if not records:
                return None
            
            # Calcular estadísticas
            h_index = 0
            total_citations = 0
            years = set()
            author_name = None
            
            citations_list = []
            for record in records:
                if isinstance(record, StandardRecord):
                    citations = record.citation_count or 0
                    citations_list.append(citations)
                    total_citations += citations
                    
                    if record.publication_year:
                        years.add(record.publication_year)
                    
                    if not author_name and record.authors:
                        for auth in record.authors:
                            if isinstance(auth, dict) and auth.get('name'):
                                author_name = auth['name']
                                break
            
            # Calcular H-index
            if citations_list:
                citations_list.sort(reverse=True)
                for i, c in enumerate(citations_list, 1):
                    if c >= i:
                        h_index = i
            
            year_range = None
            if years:
                year_list = sorted(years)
                year_range = f"{year_list[0]}-{year_list[-1]}"
            
            return {
                'h_index': h_index,
                'total_citations': total_citations,
                'total_publications': len(records),
                'years_active': year_range,
                'author_name': author_name,
                'source': 'openalex',
            }
            
        except Exception as e:
            logger.error(f"Error en _get_openalex_author_profile: {e}")
            return None
    
    def _consolidate_author_data(self, author_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Consolida datos del autor de múltiples fuentes.
        
        Prioridad: Scopus > OpenAlex
        """
        consolidated = {}
        
        scopus = author_data.get('scopus_profile')
        openalex = author_data.get('openalex_profile')
        
        # ─ H-index (usar Scopus si disponible)
        if scopus and scopus.get('h_index'):
            consolidated['h_index'] = scopus['h_index']
            consolidated['h_index_source'] = 'scopus'
        elif openalex and openalex.get('h_index'):
            consolidated['h_index'] = openalex['h_index']
            consolidated['h_index_source'] = 'openalex'
        
        # ─ Total de citas
        citations = []
        if scopus:
            citations.append(scopus.get('total_citations', 0))
        if openalex:
            citations.append(openalex.get('total_citations', 0))
        
        if citations:
            # Usar el máximo (mejor mérica)
            consolidated['total_citations'] = max(citations)
            consolidated['citations_sources'] = [
                'scopus' if scopus and scopus.get('total_citations') == consolidated['total_citations'] else None,
                'openalex' if openalex and openalex.get('total_citations') == consolidated['total_citations'] else None,
            ]
        
        # ─ Nombre del autor
        if scopus and scopus.get('author_name'):
            consolidated['author_name'] = scopus['author_name']
            consolidated['name_source'] = 'scopus'
        elif openalex and openalex.get('author_name'):
            consolidated['author_name'] = openalex['author_name']
            consolidated['name_source'] = 'openalex'
        
        # ─ Años activo
        if scopus and scopus.get('years_active'):
            consolidated['years_active'] = scopus['years_active']
            consolidated['years_source'] = 'scopus'
        elif openalex and openalex.get('years_active'):
            consolidated['years_active'] = openalex['years_active']
            consolidated['years_source'] = 'openalex'
        
        # ─ Total de publicaciones
        pubs = []
        if scopus:
            pubs.append(('scopus', scopus.get('total_publications', 0)))
        if openalex:
            pubs.append(('openalex', openalex.get('total_publications', 0)))
        
        if pubs:
            # Usar el máximo
            source, count = max(pubs, key=lambda x: x[1])
            consolidated['total_publications'] = count
            consolidated['publications_source'] = source
        
        consolidated['extracted_at'] = datetime.now().isoformat()
        
        return consolidated
    
    def _save_author_data(self, author: Author, author_data: Dict[str, Any]) -> None:
        """
        Guarda los datos consolidados del autor en la BD.
        """
        try:
            consolidated = author_data.get('consolidated', {})
            
            if not consolidated:
                logger.info("No hay datos consolidados para guardar")
                return
            
            # Actualizar el autor con datos consolidados
            if consolidated.get('author_name'):
                author.name = consolidated['author_name']
            
            # Guardar en raw_data o crear columnas adicionales según la estructura
            if not author.field_provenance:
                author.field_provenance = {}
            
            author.field_provenance['author_data'] = {
                'h_index': consolidated.get('h_index'),
                'h_index_source': consolidated.get('h_index_source'),
                'total_citations': consolidated.get('total_citations'),
                'total_publications': consolidated.get('total_publications'),
                'years_active': consolidated.get('years_active'),
                'last_updated': datetime.now().isoformat(),
            }
            
            # Guardar en BD
            self.db.add(author)
            self.db.commit()
            self.db.refresh(author)
            
            logger.info(
                f"✓ Datos del autor guardados: "
                f"h-index={consolidated.get('h_index')}, "
                f"citas={consolidated.get('total_citations')}"
            )
            
        except Exception as e:
            logger.error(f"Error guardando datos del autor: {e}")
            self.db.rollback()
    
    def _determine_available_platforms(self, author: Author) -> List[str]:
        """Determina qué plataformas se pueden consultar según IDs disponibles"""
        available = []
        
        if author.scopus_id:
            available.append('scopus')
        if author.wos_id:
            available.append('wos')
        if author.openalex_id:
            available.append('openalex')
        if author.cvlac_id:
            available.append('cvlac')
        if author.orcid:
            if 'openalex' not in available:
                available.append('openalex')  # OpenAlex también busca por ORCID
            
        # OpenAlex casi siempre está disponible (por ROR de institución)
        if 'openalex' not in available:
            available.append('openalex')
        
        # Datos Abiertos Colombia (siempre disponible para búsqueda)
        available.append('datos_abiertos')
        
        return available
    
    def _extract_from_platform(
        self,
        author: Author,
        platform: str,
    ) -> PlatformExtractionResult:
        """Ejecuta el extractor specific de una plataforma"""
        
        if platform == 'scopus':
            return self._extract_scopus(author)
        elif platform == 'wos':
            return self._extract_wos(author)
        elif platform == 'openalex':
            return self._extract_openalex(author)
        elif platform == 'cvlac':
            return self._extract_cvlac(author)
        elif platform == 'datos_abiertos':
            return self._extract_datos_abiertos(author)
        else:
            return PlatformExtractionResult(
                platform=platform,
                success=False,
                error=f"Plataforma desconocida: {platform}",
            )
    
    def _extract_scopus(self, author: Author) -> PlatformExtractionResult:
        """Extrae de Scopus usando scopus_id"""
        try:
            if not author.scopus_id:
                return PlatformExtractionResult(
                    platform='scopus',
                    success=False,
                    error="scopus_id no disponible",
                )
            
            extractor = ScopusExtractor()
            
            # Búsqueda por author ID
            query = f"AU-ID({author.scopus_id})"
            records = extractor.extract(query=query)
            
            return PlatformExtractionResult(
                platform='scopus',
                success=True,
                records_count=len(records),
                records=[r.to_dict() if hasattr(r, 'to_dict') else r for r in records],
            )
            
        except ScopusAPIError as e:
            logger.error(f"Scopus API error: {e}")
            return PlatformExtractionResult(
                platform='scopus',
                success=False,
                error=f"API Error: {str(e)[:100]}",
            )
        except Exception as e:
            logger.error(f"Scopus extraction error: {e}", exc_info=True)
            return PlatformExtractionResult(
                platform='scopus',
                success=False,
                error=str(e)[:100],
            )
    
    def _extract_wos(self, author: Author) -> PlatformExtractionResult:
        """Extrae de Web of Science usando wos_id"""
        try:
            if not author.wos_id:
                return PlatformExtractionResult(
                    platform='wos',
                    success=False,
                    error="wos_id no disponible",
                )
            
            extractor = WosExtractor()
            records = extractor.extract(researcher_id=author.wos_id)
            
            return PlatformExtractionResult(
                platform='wos',
                success=True,
                records_count=len(records),
                records=[r.to_dict() if hasattr(r, 'to_dict') else r for r in records],
            )
            
        except WosAPIError as e:
            logger.error(f"WOS API error: {e}")
            return PlatformExtractionResult(
                platform='wos',
                success=False,
                error=f"API Error: {str(e)[:100]}",
            )
        except Exception as e:
            logger.error(f"WOS extraction error: {e}", exc_info=True)
            return PlatformExtractionResult(
                platform='wos',
                success=False,
                error=str(e)[:100],
            )
    
    def _extract_openalex(self, author: Author) -> PlatformExtractionResult:
        """Extrae de OpenAlex usando openalex_id u ORCID"""
        try:
            extractor = OpenAlexExtractor()
            records = []
            
            # Intentar por openalex_id
            if author.openalex_id:
                try:
                    # Usar PyAlex para buscar por author ID
                    pyalex.config.email = institution.contact_email
                    query = Works().filter(
                        authorships={"author": {"id": author.openalex_id}}
                    )
                    
                    openalex_records_raw = []
                    for page in query.paginate(per_page=100, n_max=200):
                        works_list = page.results if hasattr(page, 'results') else (
                            list(page) if hasattr(page, '__iter__') else [page]
                        )
                        openalex_records_raw.extend(works_list)
                    
                    records = [
                        extractor._parse_record(work)
                        for work in openalex_records_raw
                    ]
                except Exception as e:
                    logger.warning(f"OpenAlex by ID failed: {e}")
            
            # Si no hay registros, intentar por ORCID
            if not records and author.orcid:
                try:
                    pyalex.config.email = institution.contact_email
                    query = Works().filter(
                        authorships={"author": {"orcid": f"https://orcid.org/{author.orcid}"}}
                    )
                    
                    openalex_records_raw = []
                    for page in query.paginate(per_page=100, n_max=200):
                        works_list = page.results if hasattr(page, 'results') else (
                            list(page) if hasattr(page, '__iter__') else [page]
                        )
                        openalex_records_raw.extend(works_list)
                    
                    records = [
                        extractor._parse_record(work)
                        for work in openalex_records_raw
                    ]
                except Exception as e:
                    logger.warning(f"OpenAlex by ORCID failed: {e}")
            
            if not records:
                return PlatformExtractionResult(
                    platform='openalex',
                    success=False,
                    error="No se encontraron publicaciones",
                )
            
            return PlatformExtractionResult(
                platform='openalex',
                success=True,
                records_count=len(records),
                records=[r.to_dict() if hasattr(r, 'to_dict') else r for r in records],
            )
            
        except Exception as e:
            logger.error(f"OpenAlex extraction error: {e}", exc_info=True)
            return PlatformExtractionResult(
                platform='openalex',
                success=False,
                error=str(e)[:100],
            )
    
    def _extract_cvlac(self, author: Author) -> PlatformExtractionResult:
        """Extrae de CVLac usando cvlac_id"""
        try:
            if not author.cvlac_id:
                return PlatformExtractionResult(
                    platform='cvlac',
                    success=False,
                    error="cvlac_id no disponible",
                )
            
            extractor = CvlacExtractor()
            records = extractor.extract(cvlac_codes=[author.cvlac_id])
            
            return PlatformExtractionResult(
                platform='cvlac',
                success=True,
                records_count=len(records),
                records=[r.to_dict() if hasattr(r, 'to_dict') else r for r in records],
            )
            
        except CvlacScrapingError as e:
            logger.error(f"CVLac scraping error: {e}")
            return PlatformExtractionResult(
                platform='cvlac',
                success=False,
                error=f"Scraping Error: {str(e)[:100]}",
            )
        except Exception as e:
            logger.error(f"CVLac extraction error: {e}", exc_info=True)
            return PlatformExtractionResult(
                platform='cvlac',
                success=False,
                error=str(e)[:100],
            )
    
    def _extract_datos_abiertos(self, author: Author) -> PlatformExtractionResult:
        """Extrae de Datos Abiertos Colombia"""
        try:
            extractor = DatosAbiertosExtractor()
            
            # Buscar por nombre del autor
            records = extractor.extract(researcher_name=author.name)
            
            if not records:
                return PlatformExtractionResult(
                    platform='datos_abiertos',
                    success=False,
                    error="No se encontraron publicaciones",
                )
            
            return PlatformExtractionResult(
                platform='datos_abiertos',
                success=True,
                records_count=len(records),
                records=[r.to_dict() if hasattr(r, 'to_dict') else r for r in records],
            )
            
        except DatosAbiertosError as e:
            logger.error(f"Datos Abiertos error: {e}")
            return PlatformExtractionResult(
                platform='datos_abiertos',
                success=False,
                error=f"API Error: {str(e)[:100]}",
            )
        except Exception as e:
            logger.error(f"Datos Abiertos extraction error: {e}", exc_info=True)
            return PlatformExtractionResult(
                platform='datos_abiertos',
                success=False,
                error=str(e)[:100],
            )
    
    def _generate_summary(self, profile: UnifiedAuthorProfile) -> str:
        """Genera un resumen legible de la extracción"""
        lines = [
            f"Autor: {profile.author_name}",
            f"Total publicaciones: {profile.total_publications}",
            f"Total citas: {profile.total_citations}",
            f"Plataformas con éxito: {', '.join(profile.platforms_with_data) or 'ninguna'}",
        ]
        
        failed = [
            p for p, r in profile.platform_results.items()
            if not r.success
        ]
        if failed:
            lines.append(f"Plataformas sin datos: {', '.join(failed)}")
        
        return " | ".join(lines)
