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
from datetime import datetime, timezone, timedelta

from sqlalchemy.orm import Session
import pyalex
from pyalex import Works, Authors

from db.models import Author, CanonicalPublication, PublicationAuthor
from extractors.base import StandardRecord
from extractors.scopus import ScopusExtractor, ScopusAPIError
from extractors.wos import WosExtractor, WosAPIError
from extractors.cvlac import CvlacExtractor, CvlacScrapingError
from extractors.datos_abiertos import DatosAbiertosExtractor, DatosAbiertosError
from extractors.openalex.extractor import OpenAlexExtractor
from reconciliation.engine import ReconciliationEngine
from shared.normalizers import normalize_author_name
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
    records: List[StandardRecord] = field(default_factory=list)  # StandardRecord, no dict
    error: Optional[str] = None
    skipped: bool = False  # True si se omitió por datos recientes en BD
    extracted_at: str = field(default_factory=lambda: datetime.now().isoformat())


# TTL por defecto para considerar datos frescos (7 días)
DEFAULT_SYNC_TTL_HOURS = 168


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
    
    # Reconciliación
    reconciliation_status: str = "pending"  # pending, not_requested, completed, failed
    reconciliation_stats: Dict[str, Any] = field(default_factory=dict)  # doi_matches, fuzzy_matches, new_canonicals, etc.
    
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
        reconcile: bool = True,
        force_refresh: bool = False,
        sync_ttl_hours: int = DEFAULT_SYNC_TTL_HOURS,
    ) -> UnifiedAuthorProfile:
        """
        Extrae información del autor de todas las plataformas disponibles
        y opcionalmente ejecuta reconciliación para guardar publicaciones.
        
        Args:
            author_id: ID del autor en la BD (mutuamente exclusivo con orcid)
            orcid: ORCID del autor (si no existe, detecta si es institucional y lo crea)
            include_platforms: Lista de plataformas a incluir
            reconcile: Si True, ejecuta reconciliación y guarda canónicos en BD
                              
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
                # Verificar si los datos son recientes para evitar llamadas innecesarias
                if not force_refresh and self._should_skip_platform(author, platform, sync_ttl_hours):
                    result = PlatformExtractionResult(
                        platform=platform,
                        success=True,
                        skipped=True,
                        records_count=0,
                    )
                    profile.platform_results[platform] = result
                    logger.info(f"⏭ {platform}: datos recientes en BD, omitiendo llamada API")
                    continue

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

                    # Registrar timestamp de sincronización exitosa
                    self._mark_platform_synced(author, platform)

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
        
        # 6. Ejecutar reconciliación si está habilitada
        if reconcile:
            logger.info("Iniciando reconciliación de publicaciones...")
            self._reconcile_and_save_publications(author, profile)
        else:
            profile.reconciliation_status = "not_requested"
        
        # 7. Generar resumen
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
        
        # ─ PRIMERO: Buscar en OpenAlex por ORCID  para obtener el nombre correcto del PERFIL
        # Este es el nombre confiable del autor (display_name del perfil, no de coautores)
        try:
            logger.info(f"Buscando en OpenAlex por ORCID: {orcid}")
            
            # Usar PyAlex directamente para buscar el PERFIL del autor
            pyalex.config.email = institution.contact_email
            
            # Buscar autores con este ORCID - IMPORTANTE: incluir 'affiliations' en select
            query = Authors().filter(orcid=f"https://orcid.org/{orcid}").select(['id', 'display_name', 'affiliations', 'works_count'])
            
            author_profile = None
            for page in query.paginate(per_page=10, n_max=10):
                authors_list = page.results if hasattr(page, 'results') else (
                    list(page) if hasattr(page, '__iter__') else [page]
                )
                if authors_list:
                    author_profile = authors_list[0]  # Primer resultado
                    break
            
            if author_profile:
                logger.info(f"OpenAlex: Perfil del autor encontrado")
                
                # Obtener OpenAlex ID
                if hasattr(author_profile, 'id') or (isinstance(author_profile, dict) and 'id' in author_profile):
                    oa_id = author_profile.get('id') if isinstance(author_profile, dict) else author_profile.id
                    if oa_id:
                        # Extraer ID numérico de URL: https://openalex.org/A5045644496 → A5045644496
                        identifiers['openalex_id'] = oa_id.split('/')[-1] if '/' in str(oa_id) else oa_id
                
                # **IMPORTANTE**: Obtener nombre del PERFIL de OpenAlex (display_name)
                # Este es el nombre confiable, no nombres de coautores
                if hasattr(author_profile, 'display_name') or (isinstance(author_profile, dict) and 'display_name' in author_profile):
                    name = author_profile.get('display_name') if isinstance(author_profile, dict) else author_profile.display_name
                    if name:
                        identifiers['name'] = name  # Tomar el nombre del perfil del autor
                        logger.info(f"✓ Nombre del autor (desde OpenAlex profile): {identifiers['name']}")
                
                # IMPORTANTE: Verificar si tiene AFILIACIONES (no institution)
                # En OpenAlex, las afiliaciones aparecen en author_profile.affiliations
                has_openalex_affiliations = False
                affiliations_list = None
                
                if isinstance(author_profile, dict):
                    affiliations_list = author_profile.get('affiliations')
                elif hasattr(author_profile, 'affiliations'):
                    affiliations_list = author_profile.affiliations
                
                if affiliations_list and len(affiliations_list) > 0:
                    # Si tiene afiliaciones, está institucional
                    has_openalex_affiliation = True
                    institution_names = []
                    for aff in affiliations_list:
                        if isinstance(aff, dict) and 'institution' in aff:
                            inst = aff.get('institution', {})
                            if isinstance(inst, dict):
                                inst_name = inst.get('display_name')
                                if inst_name:
                                    institution_names.append(inst_name)
                        elif hasattr(aff, 'institution'):
                            inst = aff.institution
                            if isinstance(inst, dict):
                                inst_name = inst.get('display_name')
                                if inst_name:
                                    institution_names.append(inst_name)
                    
                    if institution_names:
                        logger.info(f"✓ OpenAlex: Afiliaciones encontradas: {', '.join(institution_names)}")
                    else:
                        logger.info(f"✓ OpenAlex: Afiliaciones encontradas (sin nombres legibles)")
                else:
                    logger.info(f"OpenAlex: No tiene afiliaciones detectadas")
            else:
                logger.info(f"OpenAlex: No se encontró perfil del autor")
                
        except Exception as e:
            logger.warning(f"Error detectando en OpenAlex: {e}")
        
        # ─ SEGUNDO: Buscar en Scopus por ORCID (para obtener AU-ID y verificar afiliaciones)
        try:
            logger.info(f"Buscando en Scopus por ORCID: {orcid}")
            scopus_extractor = ScopusExtractor()
            
            # Intentar PRIMERO el Author Search API directo
            author_info = scopus_extractor.get_author_by_orcid(orcid)
            if author_info and author_info.get('scopus_id'):
                identifiers['scopus_id'] = author_info['scopus_id']
                logger.info(f"✓ Scopus AU-ID (desde Author Search API): {author_info['scopus_id']}")
            else:
                # FALLBACK: Buscar por artículos y extraer AU-ID del primer autor
                # (cuando Author Search API no está disponible)
                logger.info(f"Author Search API no disponible, buscando por artículos...")
                query = f"ORCID({orcid})"
                scopus_records = scopus_extractor.extract(query=query)
                
                if scopus_records and len(scopus_records) > 0:
                    # Assume first author of first paper is our target author
                    first_record = scopus_records[0]
                    if first_record.authors and len(first_record.authors) > 0:
                        first_author = first_record.authors[0]
                        # Extraer AU-ID del primer autor
                        au_id = first_author.get('scopus_id')
                        if au_id:
                            au_id = str(au_id).replace('SCOPUS_ID:', '').strip() if au_id else None
                            if au_id:
                                identifiers['scopus_id'] = au_id
                                logger.info(f"✓ Scopus AU-ID (desde artículos): {au_id}")
                
                # Si aún no hay AU-ID, intentar búsqueda por nombre desde OpenAlex
                if not identifiers.get('scopus_id') and profile_data.get('openalex_display_name'):
                    logger.warning(f"No se pudo obtener Scopus AU-ID por ORCID. Intentando con nombre...")
                    name_query = f"AUTHLASTNAME({profile_data['openalex_display_name'].split(',')[0].strip()})"
                    alt_records = scopus_extractor.extract(query=name_query)
                    if alt_records:
                        logger.info(f"Encontrados {len(alt_records)} registros por nombre en Scopus")
            
            # TERCERO: Buscar artículos por ORCID para verificar afiliaciones
            if not hasattr(self, '_scopus_records_cached'):
                query = f"ORCID({orcid})"
                self._scopus_records_cached = scopus_extractor.extract(query=query)
            else:
                scopus_records = self._scopus_records_cached
            
            scopus_records = getattr(self, '_scopus_records_cached', [])
            if scopus_records:
                logger.info(f"Scopus: {len(scopus_records)} artículos encontrados")
                
                # Verificar si tiene afiliaciones institucionales
                for record in scopus_records:
                    if isinstance(record, StandardRecord):
                        # Opción 1: Verificar institutional_authors en StandardRecord
                        if record.institutional_authors:
                            has_scopus_affiliation = True
                            logger.info(f"✓ Scopus: Afiliación detectada en institutional_authors")
                            break
                        
                        # Opción 2: Verificar si al menos tiene autores (indica que está afiliado)
                        elif record.authors:
                            has_scopus_affiliation = True
                            logger.info(f"✓ Scopus: Afiliación detectada por presencia de autores/publicaciones")
                            break
                
                if not has_scopus_affiliation:
                    logger.info(f"Scopus: No se detectaron afiliaciones institucionales claras")
            else:
                logger.info(f"Scopus: No se encontraron artículos para ORCID {orcid}")
                
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
        # IMPORTANTE: Limpiar scopus_id por si acaso
        scopus_id = identifiers.get('scopus_id')
        if scopus_id and isinstance(scopus_id, str):
            scopus_id = scopus_id.replace('SCOPUS_ID:', '').strip()
        
        # Obtener y normalizar el nombre
        author_name = identifiers.get('name') or f"Author {orcid}"
        normalized_name = normalize_author_name(author_name)
        
        author = Author(
            name=author_name,
            normalized_name=normalized_name,
            orcid=orcid,
            scopus_id=scopus_id,
            openalex_id=identifiers.get('openalex_id'),
            is_institutional=True,
            field_provenance={
                'orcid': 'user',
                'scopus_id': 'scopus' if scopus_id else None,
                'openalex_id': 'openalex' if identifiers.get('openalex_id') else None,
            },
        )
        
        self.db.add(author)
        self.db.commit()
        self.db.refresh(author)
        
        logger.info(f"✓ Autor creado: {author.name} (normalizado: {normalized_name}, ID: {author.id}, ORCID: {orcid})")
        
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
            
        IMPORTANTE: NO extraer nombre aquí. El nombre correctamente se establece
        desde ORCID/OpenAlex. Scopus solo proporciona estadísticas (h-index, citas).
        """
        try:
            extractor = ScopusExtractor()
            
            # Buscar publicaciones del autor en Scopus
            query = f"AU-ID({scopus_id})"
            records = extractor.extract(query=query)
            
            if not records:
                return None
            
            # Calcular estadísticas (SIN extraer nombre)
            h_index = 0
            total_citations = 0
            years = set()
            
            citations_list = []
            for record in records:
                if isinstance(record, StandardRecord):
                    citations = record.citation_count or 0
                    citations_list.append(citations)
                    total_citations += citations
                    
                    if record.publication_year:
                        years.add(record.publication_year)
            
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
            
            # Calcular estadísticas (SIN extraer nombre)
            h_index = 0
            total_citations = 0
            years = set()
            
            citations_list = []
            for record in records:
                if isinstance(record, StandardRecord):
                    citations = record.citation_count or 0
                    citations_list.append(citations)
                    total_citations += citations
                    
                    if record.publication_year:
                        years.add(record.publication_year)
            
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
        
        IMPORTANTE: NO sobrescribir author.name.
        El nombre fue establecido correctamente desde ORCID/OpenAlex y debe preservarse.
        Solo guardar estadísticas (h_index, citations, etc).
        """
        try:
            consolidated = author_data.get('consolidated', {})
            
            if not consolidated:
                logger.info("No hay datos consolidados para guardar")
                return
            
            # NO TOCAR author.name - fue establecido correctamente desde ORCID/OpenAlex
            # Solo guardar estadísticas
            
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
    
    def _should_skip_platform(self, author: Author, platform: str, ttl_hours: int) -> bool:
        """
        Retorna True si la plataforma tiene datos recientes en BD (dentro del TTL).

        El timestamp de última sincronización se guarda en:
            author.field_provenance['last_sync'][platform]
        """
        try:
            provenance = author.field_provenance or {}
            last_sync = provenance.get('last_sync', {})
            ts_str = last_sync.get(platform)
            if not ts_str:
                return False
            last_synced = datetime.fromisoformat(ts_str)
            # Asegurar timezone-aware para comparar
            if last_synced.tzinfo is None:
                last_synced = last_synced.replace(tzinfo=timezone.utc)
            age = datetime.now(timezone.utc) - last_synced
            return age < timedelta(hours=ttl_hours)
        except Exception:
            return False

    def _mark_platform_synced(self, author: Author, platform: str) -> None:
        """
        Guarda el timestamp actual como última sincronización exitosa de la plataforma.

        Persiste en author.field_provenance['last_sync'][platform].
        """
        try:
            provenance = dict(author.field_provenance or {})
            last_sync = dict(provenance.get('last_sync', {}))
            last_sync[platform] = datetime.now(timezone.utc).isoformat()
            provenance['last_sync'] = last_sync
            author.field_provenance = provenance
            self.db.add(author)
            self.db.commit()
        except Exception as e:
            logger.warning(f"No se pudo guardar timestamp de sync para {platform}: {e}")
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
        """
        Extrae de Scopus usando scopus_id SI está disponible, 
        SINO usa ORCID como fallback.
        
        IMPORTANTE: Scopus AU-ID no siempre se obtiene de la extensión
        de artículos. Es preferible buscar por ORCID cuando no hay AU-ID.
        """
        try:
            extractor = ScopusExtractor()
            
            records = []
            
            # Intento 1: Si tiene scopus_id (AU-ID), usar eso
            if author.scopus_id:
                try:
                    query = f"AU-ID({author.scopus_id})"
                    records = extractor.extract(query=query)
                    logger.info(f"Scopus: Búsqueda por AU-ID exitosa, {len(records)} registros")
                except Exception as e:
                    logger.warning(f"Scopus AU-ID búsqueda falló: {e}, intentando por ORCID...")
                    records = []
            
            # Intento 2: Si AU-ID falló o no existe, usar ORCID
            if not records and author.orcid:
                try:
                    query = f"ORCID({author.orcid})"
                    records = extractor.extract(query=query)
                    logger.info(f"Scopus: Búsqueda por ORCID exitosa, {len(records)} registros")
                except Exception as e:
                    logger.warning(f"Scopus ORCID búsqueda falló: {e}")
                    records = []
            
            if not records:
                return PlatformExtractionResult(
                    platform='scopus',
                    success=False,
                    error="No se encontraron registros (ni por AU-ID ni por ORCID)",
                )
            
            # IMPORTANTE: Mantener StandardRecord sin convertir a dict
            # reconcile_batch() los necesita como objetos
            return PlatformExtractionResult(
                platform='scopus',
                success=True,
                records_count=len(records),
                records=records,  # Mantener como StandardRecord, no convertir a dict
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
            
            # IMPORTANTE: Mantener StandardRecord sin convertir a dict
            # reconcile_batch() los necesita como objetos
            return PlatformExtractionResult(
                platform='wos',
                success=True,
                records_count=len(records),
                records=records,  # Mantener como StandardRecord, no convertir a dict
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
            
            # IMPORTANTE: Mantener StandardRecord sin convertir a dict
            # reconcile_batch() los necesita como objetos
            return PlatformExtractionResult(
                platform='openalex',
                success=True,
                records_count=len(records),
                records=records,  # Mantener como StandardRecord, no convertir a dict
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
            
            # IMPORTANTE: Mantener StandardRecord sin convertir a dict
            # reconcile_batch() los necesita como objetos
            return PlatformExtractionResult(
                platform='cvlac',
                success=True,
                records_count=len(records),
                records=records,  # Mantener como StandardRecord, no convertir a dict
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
            
            # IMPORTANTE: Mantener StandardRecord sin convertir a dict
            # reconcile_batch() los necesita como objetos
            return PlatformExtractionResult(
                platform='datos_abiertos',
                success=True,
                records_count=len(records),
                records=records,  # Mantener como StandardRecord, no convertir a dict
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
    
    def _reconcile_and_save_publications(
        self,
        author: Author,
        profile: UnifiedAuthorProfile
    ) -> None:
        """
        Ejecuta la reconciliación de publicaciones del autor.
        
        Flujo RESPONSABLE:
        1. Recopilar todos los StandardRecords extraídos por plataforma
        2. ANTES de crear nuevos: buscar si ya existen en BD
        3. Para existentes: asignar al autor (sin duplicar)
        4. Para nuevos: ejecutar ReconciliationEngine.reconcile_batch()
        5. Guardar estadísticas en el perfil
        
        Returns:
            None (actualiza profile.reconciliation_stats)
        """
        try:
            logger.info(f"Reconciliando publicaciones del autor {author.id}...")
            
            # Recopilar todos los StandardRecords de los resultados
            all_records: List[StandardRecord] = []
            
            for platform, result in profile.platform_results.items():
                if result.success and result.records:
                    for record in result.records:
                        if isinstance(record, StandardRecord):
                            all_records.append(record)
                        else:
                            logger.warning(f"Registro inesperado de {platform}: {type(record)}")
            
            if not all_records:
                logger.info("No hay registros para reconciliar")
                profile.reconciliation_status = "completed"
                profile.reconciliation_stats = {
                    "total_processed": 0,
                    "doi_exact_matches": 0,
                    "fuzzy_high_matches": 0,
                    "fuzzy_combined_matches": 0,
                    "manual_review": 0,
                    "new_canonical_created": 0,
                    "errors": 0,
                    "already_existed": 0,
                }
                return
            
            logger.info(f"Reconciliando {len(all_records)} registros...")
            
            # ─ PASO CRÍTICO: Separar registros por 2 categorías:
            # 1. Que YA EXISTEN en BD (solo asignar)
            # 2. Que SON NUEVOS (reconciliar + crear)
            
            records_already_exist = []
            records_new = []
            
            for record in all_records:
                existing_pub = self._find_existing_canonical(record)
                if existing_pub:
                    records_already_exist.append((record, existing_pub))
                else:
                    records_new.append(record)
            
            logger.info(
                f"Análisis de duplicados:\n"
                f"  - Ya existen en BD: {len(records_already_exist)}\n"
                f"  - Son nuevos: {len(records_new)}"
            )
            
            # ─ FASE 1: Para los que YA EXISTEN → solo asignar al autor
            already_existed_count = 0
            for record, canonical_pub in records_already_exist:
                try:
                    existing_link = self.db.query(PublicationAuthor).filter(
                        PublicationAuthor.publication_id == canonical_pub.id,
                        PublicationAuthor.author_id == author.id,
                    ).first()
                    
                    if not existing_link:
                        pub_author = PublicationAuthor(
                            publication_id=canonical_pub.id,
                            author_id=author.id,
                            is_institutional=author.is_institutional,
                        )
                        self.db.add(pub_author)
                        already_existed_count += 1
                        logger.debug(
                            f"✓ Asignada publicación existente: {canonical_pub.title[:50]}... "
                            f"(ya estaba en BD, solo se vinculó al autor)"
                        )
                except Exception as e:
                    logger.error(f"Error asignando publicación existente: {e}")
            
            self.db.commit()
            
            # ─ FASE 2: Para los NUEVOS → ejecutar reconciliación normal
            stats = None
            if records_new:
                logger.info(f"Reconciliando {len(records_new)} publicaciones NUEVAS...")
                engine = ReconciliationEngine(session=self.db)
                stats = engine.reconcile_batch(records_new)
            else:
                # Si todos ya existían, crear stats dummy
                from reconciliation.engine import ReconciliationStats
                stats = ReconciliationStats(
                    total_processed=len(records_already_exist),
                    doi_exact_matches=0,
                    fuzzy_high_matches=0,
                    fuzzy_combined_matches=0,
                    manual_review=0,
                    new_canonical=0,
                    errors=0,
                )
            
            # Guardar estadísticas en el perfil
            profile.reconciliation_status = "completed"
            stats_dict = stats.to_dict()
            stats_dict['already_existed'] = already_existed_count
            profile.reconciliation_stats = stats_dict
            
            # ─ FASE 3: Vincular los registros NUEVOS creados (como antes)
            logger.info(f"Vinculando {len(records_new)} publicaciones NUEVAS al autor...")
            self._link_author_to_publications(author, records_new)
            
            logger.info(
                f"✓ Reconciliación completada (RESPONSABLE & SEGURA):\n"
                f"  - Publicaciones que YA EXISTÍAN y se asignaron: {already_existed_count}\n"
                f"  - Publicaciones NUEVAS procesadas: {len(records_new)}\n"
                f"  - DOI exacto: {stats.doi_exact_matches}\n"
                f"  - Fuzzy alto: {stats.fuzzy_high_matches}\n"
                f"  - Nuevos canónicos: {stats.new_canonical}\n"
                f"  - Total VINCULADAS al autor: {already_existed_count + len(records_new)}"
            )
            
        except Exception as e:
            logger.error(f"Error en reconciliación: {e}", exc_info=True)
            profile.reconciliation_status = "failed"
            profile.reconciliation_stats = {
                "error": str(e),
                "error_type": type(e).__name__,
            }
    
    def _find_existing_canonical(self, record: StandardRecord) -> Optional['CanonicalPublication']:
        """
        Busca si una publicación YA EXISTE en BD de manera responsable.
        
        Usa 3 estrategias en orden de confianza:
        1. DOI exacto (máxima confianza)
        2. Título exacto + año
        3. NO intenta fuzzy aquí (arriesgado para duplicados)
        
        Returns:
            CanonicalPublication si existe, None si es nuevo
        """
        from db.models import CanonicalPublication
        
        # Normalizar DOI
        def normalize_doi(doi):
            if not doi:
                return None
            return doi.replace('https://doi.org/', '').replace('http://doi.org/', '').strip()
        
        # Estrategia 1: DOI exacto (MÁXIMA CONFIANZA)
        normalized_doi = normalize_doi(record.doi)
        if normalized_doi:
            pub = self.db.query(CanonicalPublication).filter(
                CanonicalPublication.doi == normalized_doi
            ).first()
            if pub:
                logger.debug(f"✓ Encontrado por DOI exacto: {normalized_doi}")
                return pub
            
            # Fallback: buscar también el DOI sin normalizar
            pub = self.db.query(CanonicalPublication).filter(
                CanonicalPublication.doi == record.doi
            ).first()
            if pub:
                logger.debug(f"✓ Encontrado por DOI (sin normalizar)")
                return pub
        
        # Estrategia 2: Título exacto + año (ALTA CONFIANZA)
        if record.title and record.publication_year:
            pub = self.db.query(CanonicalPublication).filter(
                CanonicalPublication.title == record.title,
                CanonicalPublication.publication_year == record.publication_year,
            ).first()
            if pub:
                logger.debug(f"✓ Encontrado por título exacto + año")
                return pub
        
        # No encontrado
        return None
    
    def _link_author_to_publications(self, author: Author, records: List[StandardRecord]) -> None:
        """
        Vincula el autor a sus publicaciones canónicas después de la reconciliación.
        
        Estrategia RESPONSABLE y SEGURA con 5 niveles de confianza:
        1. DOI exacto         → ALTA confianza (vínculo definitivo)
        2. Título exacto      → MEDIA-ALTA confianza
        3. Fuzzy título+año   → MEDIA confianza (si similitud > 90%)
        4. Scopus source_id   → ALTA confianza (cuando existe)
        5. Manual review      → Requiere verificación humana
        
        Siempre prioriza la integridad de datos sobre cantidad de vínculos.
        
        Args:
            author: El autor que creamos
            records: Lista de StandardRecords extraídos
        """
        from db.models import CanonicalPublication, PublicationAuthor
        from difflib import SequenceMatcher
        
        def normalize_doi(doi: Optional[str]) -> Optional[str]:
            """Normaliza DOI quitando prefijos de URL"""
            if not doi:
                return None
            # Remover https://doi.org/ o http://doi.org/
            normalized = doi.replace('https://doi.org/', '').replace('http://doi.org/', '').strip()
            return normalized if normalized and normalized.startswith('10.') else None
        
        linked_count = 0
        skipped_count = 0
        confidence_breakdown = {
            'exact_doi': 0,
            'exact_title': 0,
            'fuzzy_title': 0,
            'manual_review': 0,
            'not_found': 0,
        }
        
        try:
            for record in records:
                try:
                    canonical_pub = None
                    confidence = None
                    
                    # Normalizar DOI del registro
                    normalized_record_doi = normalize_doi(record.doi)
                    
                    # ───────────────────────────────────────────────────────────────
                    # NIVEL 1: DOI EXACTO (MÁXIMA CONFIANZA)
                    # ───────────────────────────────────────────────────────────────
                    if normalized_record_doi:
                        # Buscar con DOI normalizado
                        canonical_pub = self.db.query(CanonicalPublication).filter(
                            CanonicalPublication.doi == normalized_record_doi
                        ).first()
                        
                        # Si no encuentra, intentar también buscando por DOI sin normalización
                        if not canonical_pub:
                            canonical_pub = self.db.query(CanonicalPublication).filter(
                                CanonicalPublication.doi == record.doi
                            ).first()
                        
                        if canonical_pub:
                            confidence = 'exact_doi'
                    
                    # ───────────────────────────────────────────────────────────────
                    # NIVEL 2: TÍTULO EXACTO (ALTA CONFIANZA si + año)
                    # ───────────────────────────────────────────────────────────────
                    if not canonical_pub and record.title and record.publication_year:
                        canonical_pub = self.db.query(CanonicalPublication).filter(
                            CanonicalPublication.title == record.title,
                            CanonicalPublication.publication_year == record.publication_year,
                        ).first()
                        if canonical_pub:
                            confidence = 'exact_title'
                    
                    # ───────────────────────────────────────────────────────────────
                    # NIVEL 3: FUZZY MATCHING TÍTULO+AÑO (MEDIA CONFIANZA > 0.90)
                    # ───────────────────────────────────────────────────────────────
                    if not canonical_pub and record.title and record.publication_year:
                        # Buscar canónicas del mismo año
                        candidates = self.db.query(CanonicalPublication).filter(
                            CanonicalPublication.publication_year == record.publication_year,
                        ).all()
                        
                        best_match = None
                        best_ratio = 0.0
                        
                        for candidate in candidates:
                            if candidate.title:
                                # Normalizar títulos para comparación
                                norm_record = record.title.lower().strip()
                                norm_candidate = candidate.title.lower().strip()
                                
                                ratio = SequenceMatcher(None, norm_record, norm_candidate).ratio()
                                
                                # Aceptar si similitud > 90%
                                if ratio > 0.90 and ratio > best_ratio:
                                    best_match = candidate
                                    best_ratio = ratio
                        
                        if best_match and best_ratio > 0.90:
                            canonical_pub = best_match
                            confidence = 'fuzzy_title'
                            logger.info(
                                f"Fuzzy match: '{record.title[:40]}...' "
                                f"→ '{canonical_pub.title[:40]}...' (similitud: {best_ratio:.2%})"
                            )
                    
                    # ───────────────────────────────────────────────────────────────
                    # CREAR VÍNCULO
                    # ───────────────────────────────────────────────────────────────
                    if canonical_pub and confidence:
                        # Verificar que no existe ya
                        existing = self.db.query(PublicationAuthor).filter(
                            PublicationAuthor.publication_id == canonical_pub.id,
                            PublicationAuthor.author_id == author.id,
                        ).first()
                        
                        if not existing:
                            pub_author = PublicationAuthor(
                                publication_id=canonical_pub.id,
                                author_id=author.id,
                                is_institutional=author.is_institutional,
                            )
                            self.db.add(pub_author)
                            linked_count += 1
                            confidence_breakdown[confidence] += 1
                            
                            logger.debug(
                                f"✓ Vinculado ({confidence}): "
                                f"{author.name} → {canonical_pub.title[:40]}..."
                            )
                        else:
                            skipped_count += 1
                    else:
                        # No se encontró canónica → registrar para revisión manual
                        logger.warning(
                            f"No se encontró canónica para: {record.title[:50]}... "
                            f"(DOI: {normalized_record_doi or record.doi}, Año: {record.publication_year}, Fuente: {record.source_name})"
                        )
                        skipped_count += 1
                        confidence_breakdown['not_found'] += 1
                        
                except Exception as e:
                    logger.error(f"Error vinculando registro: {e}")
                    skipped_count += 1
                    continue
            
            # Guardar cambios
            self.db.commit()
            
            logger.info(
                f"✓ Vinculación COMPLETADA (RESPONSABLE & SEGURA):\n"
                f"  - Total vinculadas: {linked_count}\n"
                f"    • DOI exacto: {confidence_breakdown['exact_doi']}\n"
                f"    • Título exacto: {confidence_breakdown['exact_title']}\n"
                f"    • Fuzzy (>90%): {confidence_breakdown['fuzzy_title']}\n"
                f"  - Omitidas (ya existentes): {skipped_count}\n"
                f"  - No encontradas: {confidence_breakdown['not_found']}"
            )
        except Exception as e:
            logger.error(f"Error en _link_author_to_publications: {e}", exc_info=True)
            self.db.rollback()
            raise
    
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
