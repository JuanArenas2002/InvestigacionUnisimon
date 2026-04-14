"""
Extractor de Scopus API (Elsevier).

Requiere API Key de Elsevier Developer Portal:
  https://dev.elsevier.com/

Endpoints principales:
  - Scopus Search API: búsqueda de documentos
  - Scopus Abstract Retrieval: detalle de un documento
"""

import json
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import scopus_config, institution, SourceName
from extractors.base import BaseExtractor, StandardRecord

logger = logging.getLogger(__name__)


class ScopusAPIError(Exception):
    """Excepción para errores de la API de Scopus"""
    pass


class ScopusExtractor(BaseExtractor):
    """
    Extractor de publicaciones desde Scopus Search API.

    Documentación: https://dev.elsevier.com/documentation/ScopusSearchAPI.wadl

    Requiere configurar en variables de entorno:
      - SCOPUS_API_KEY: API key de Elsevier
      - SCOPUS_INST_TOKEN: Token institucional (opcional, para mayor cuota)
    """

    source_name = SourceName.SCOPUS

    SEARCH_URL = f"{scopus_config.base_url}/search/scopus"
    ABSTRACT_URL = f"{scopus_config.base_url}/abstract/scopus_id"

    def __init__(self, api_key: str = None, inst_token: str = None):
        self.api_key = api_key or scopus_config.api_key
        self.inst_token = inst_token or scopus_config.inst_token
        self.config = scopus_config

        if not self.api_key:
            logger.warning(
                "SCOPUS_API_KEY no configurada. "
                "Obten una en https://dev.elsevier.com/"
            )

        self.session = self._create_session()
        logger.info("ScopusExtractor inicializado.")

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        # Headers comunes para Scopus
        session.headers.update({
            "X-ELS-APIKey": self.api_key,
            "Accept": "application/xml",
        })
        if self.inst_token:
            session.headers["X-ELS-Insttoken"] = self.inst_token

        return session



    # ---------------------------------------------------------
    # BÚSQUEDA DE AUTOR POR ORCID
    # ---------------------------------------------------------
    
    def get_author_by_orcid(self, orcid: str) -> Optional[Dict[str, str]]:
        """
        Busca un autor en Scopus por su ORCID y retorna su AU-ID (scopus_id).
        
        Nota: Si el Author Search API no funciona, no devuelve error fatal,
        solo retorna None para poder continuar con otras formas de detección.
        
        Args:
            orcid: ORCID del autor (ej: "0000-0002-2096-7900")
            
        Returns:
            Dict con {"scopus_id": "...", "name": "..."} o None si no se encuentra/falla el API
        """
        try:
            url = "https://api.elsevier.com/content/search/author"
            
            # Nota: El Author Search API tiene restricciones y puede no estar disponible
            # para todas las claves API. Intentamos sin parámetros extra primero.
            params = {
                "query": f"ORCID({orcid})"
            }
            
            logger.info(f"Scopus Author Search: Buscando autor por ORCID {orcid}")
            response = self.session.get(url, params=params, timeout=10)
            
            # Si falla con 400, es un error de sintaxis o permisos - registra pero no falla
            if response.status_code == 400:
                logger.warning(f"Scopus Author Search API retornó 400. Probablemente API Key no tiene acceso a Author Search.")
                return None
            
            response.raise_for_status()
            
            # Parsear respuesta XML
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            
            # Namespaces
            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'opensearch': 'http://a9.com/-/spec/opensearch/1.1/',
            }
            
            # Buscar primer entry (autor)
            entries = root.findall('atom:entry', ns)
            
            if not entries:
                logger.warning(f"No se encontró autor con ORCID {orcid} en Scopus Author Search")
                return None
            
            entry = entries[0]
            
            # Extraer AU-ID
            au_id = entry.findtext('dc:identifier', default=None, namespaces=ns)
            if au_id and au_id.startswith('AUTHOR_ID:'):
                au_id = au_id.replace('AUTHOR_ID:', '').strip()
            
            # Extraer nombre
            name = entry.findtext('dc:title', default=None, namespaces=ns)
            
            if au_id:
                result = {
                    'scopus_id': au_id,
                    'name': name,
                }
                logger.info(f"✓ Autor encontrado en Scopus Author Search: {name} (AU-ID: {au_id})")
                return result
            else:
                logger.warning(f"No se extrajo AU-ID para ORCID {orcid}")
                return None
            
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Error HTTP en Scopus Author Search: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error inesperado en búsqueda Scopus Author Search: {e}")
            return None

    # ---------------------------------------------------------
    # INTERFAZ BaseExtractor
    # ---------------------------------------------------------

    def extract(
        self,
        query: Optional[str] = None,
        start: int = 0,
        max_results: Optional[int] = None,
        affiliation_id: Optional[str] = None,
        orcid: Optional[str] = None,
    ) -> List[StandardRecord]:
        """
        Extrae registros de Scopus Search API y normaliza campos.
        Puede buscar por query, affiliation_id o orcid.
        """
        # Construir query si no se pasa explícitamente
        if query is None:
            if affiliation_id:
                query = f"AF-ID({affiliation_id})"
            elif orcid:
                query = f"ORCID({orcid})"
            else:
                raise ValueError("Debes proporcionar 'query', 'affiliation_id' o 'orcid' para la extracción de Scopus.")

        records = []
        total_fetched = 0
        import xml.etree.ElementTree as ET
        while True:
            params = {
                "query": query,
                "start": start,
                "count": self.config.max_per_page,
                "sort": "pubyear",
                "field": (
                    "dc:identifier,doi,dc:title,prism:publicationName,"\
                    "prism:coverDate,subtypeDescription,citedby-count,"\
                    "author,prism:issn,openaccess,openaccessFlag,"\
                    "dc:description,authkeywords,prism:volume,prism:issueIdentifier,"\
                    "prism:pageRange,afid,affiliation"
                ),
            }


            try:
                resp = self.session.get(
                    self.SEARCH_URL,
                    params=params,
                    timeout=self.config.timeout,
                )
                print(f"ScopusExtractor: URL={resp.url}")
                resp.raise_for_status()
                xml_content = resp.text
                print("Respuesta cruda Scopus (XML):", xml_content[:2000])
            except requests.exceptions.RequestException as e:
                print(f"Error en Scopus API: {e}")
                raise ScopusAPIError(f"Error en Scopus API: {e}")

            # Manejo robusto de errores de parseo XML
            try:
                root = ET.fromstring(xml_content)
            except ET.ParseError as e:
                logger.warning(f"Respuesta de Scopus no es XML válido: {e}. Respuesta: {xml_content[:200]}")
                break

            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'opensearch': 'http://a9.com/-/spec/opensearch/1.1/',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'prism': 'http://prismstandard.org/namespaces/basic/2.0/',
                'scopus': 'http://www.elsevier.com/xml/svapi/abstract/dtd',
            }
            entries = root.findall('atom:entry', ns)

            if not entries:
                print("ScopusExtractor: Sin resultados o error en entries.")
                break

            for entry in entries:
                # Saltar entradas de error (e.g. <entry><error>Result set was empty</error></entry>)
                if entry.find('error') is not None or entry.findtext('error') is not None:
                    break

                try:
                    # Extraer campos principales del XML
                    doi = entry.findtext('prism:doi', default=None, namespaces=ns)
                    title = entry.findtext('dc:title', default=None, namespaces=ns)
                    scopus_id = entry.findtext('dc:identifier', default=None, namespaces=ns)
                    
                    # IMPORTANTE: Limpiar prefijo "SCOPUS_ID:" del ID
                    if scopus_id and isinstance(scopus_id, str):
                        scopus_id = scopus_id.replace('SCOPUS_ID:', '').strip()
                    
                    cover_date = entry.findtext('prism:coverDate', default=None, namespaces=ns)
                    pub_year = int(cover_date[:4]) if cover_date and len(cover_date) >= 4 else None
                    source_journal = entry.findtext('prism:publicationName', default=None, namespaces=ns)
                    issn = entry.findtext('prism:issn', default=None, namespaces=ns)
                    eissn = entry.findtext('prism:eIssn', default=None, namespaces=ns)  # E-ISSN
                    
                    # TIPO DE PUBLICACIÓN — intentar múltiples campos
                    subtype = None
                    # Opción 1: subtypeDescription (en namespace atom)
                    subtype = entry.findtext('atom:subtypeDescription', default=None, namespaces=ns)
                    # Opción 2: subtype (a secas, en namespace atom)
                    if not subtype:
                        subtype = entry.findtext('atom:subtype', default=None, namespaces=ns)
                    # Opción 3: aggregationType (en namespace prism)
                    if not subtype:
                        subtype = entry.findtext('prism:aggregationType', default=None, namespaces=ns)
                    
                    # citedby-count está en el namespace atom
                    citedby_count = int(entry.findtext('atom:citedby-count', default='0', namespaces=ns))
                    
                    # ACCESO ABIERTO — intentar TODOS los campos posibles
                    # El campo viene como string: "All Open Access; Bronze Open Access; Green Open Access"
                    oa_status = None
                    is_oa = None
                    
                    # Intentar openAccessStatus primero (con namespace prism)
                    oa_status = entry.findtext('prism:openAccessStatus', default=None, namespaces=ns)
                    
                    # Intentar openaccessFlag (en namespace atom)
                    if not oa_status:
                        oa_status = entry.findtext('atom:openaccessFlag', default=None, namespaces=ns)
                    
                    # Intentar openaccess (en namespace atom) como número (1=sí, 0=no)
                    if not oa_status:
                        oa_text = entry.findtext('atom:openaccess', default=None, namespaces=ns)
                        if oa_text:
                            oa_status = oa_text
                    
                    # Intentar freetoreadLabel para extraer info descriptiva de OA
                    if not oa_status:
                        freetoread_label = entry.findtext('atom:freetoreadLabel', default=None, namespaces=ns)
                        if freetoread_label:
                            oa_status = freetoread_label
                    
                    # Normalizar: Si contiene "All Open Access" o similares, es OA
                    if oa_status:
                        oa_status_lower = str(oa_status).lower().strip()
                        is_oa = any(x in oa_status_lower for x in ['all open access', 'gold', 'bronze', 'green', 'hybrid', 'open access', 'true', '1', 'yes'])
                    
                    # Obtener total de citaciones desde Scopus
                    # (No disponible años precisos de citación desde Scopus API)

                    # Autores
                    authors = []
                    # Buscar con ambos formatos de namespace por si acaso
                    author_elements = entry.findall('atom:author', ns) or entry.findall('author', ns)
                    for author in author_elements:
                        name = author.findtext('atom:authname', default=None, namespaces=ns) or author.findtext('authname', default=None)
                        authid = author.findtext('atom:authid', default=None, namespaces=ns) or author.findtext('authid', default=None)
                        if name:  # Solo agregar si tiene nombre
                            authors.append({
                                "name": name,
                                "orcid": None,
                                "scopus_id": authid,
                                "is_institutional": False,
                            })

                    record = StandardRecord(
                        source_name=self.source_name,
                        source_id=scopus_id,
                        doi=doi,
                        title=title,
                        publication_year=pub_year,
                        publication_date=cover_date,
                        publication_type=subtype,
                        source_journal=source_journal,
                        issn=issn,
                        is_open_access=is_oa,
                        oa_status=oa_status,  # Guardar el string original también
                        authors=authors,
                        citation_count=citedby_count,
                        citations_by_year={},  # No disponible desde Scopus API
                        url=None,
                        raw_data={"eissn": eissn} if eissn else None,  # Guardar E-ISSN en raw_data
                    )
                    record.compute_normalized_fields()
                    records.append(record)
                    total_fetched += 1
                    if max_results and total_fetched >= max_results:
                        break
                except Exception as e:
                    print(f"Error parseando entrada Scopus XML: {e}")
                    continue

            print(f"  Extraídos de Scopus: {total_fetched}")

            # Paginación
            total_results_el = root.find('opensearch:totalResults', ns)
            total_results = int(total_results_el.text) if total_results_el is not None else 0
            start += self.config.max_per_page
            if start >= total_results:
                break

            time.sleep(0.2)  # Rate limit

        return self._post_process(records)

    def get_author_profile(self, author_id: str) -> Optional[Dict[str, str]]:
        """
        Obtiene el perfil completo de un autor desde Scopus Author Retrieval API.

        Returns:
            Dict con: name, orcid, subject_areas, institution_current, etc.
            None si falla
        """
        AUTHOR_URL = f"{scopus_config.base_url}/author/author_id/{author_id}"

        headers = {
            "User-Agent": "ScopusExtractor/1.0",
            "Accept": "application/json",
        }

        if self.api_key:
            headers["X-ELS-APIKey"] = self.api_key
        if self.inst_token:
            headers["X-ELS-Insttoken"] = self.inst_token

        try:
            resp = self.session.get(AUTHOR_URL, headers=headers, timeout=self.config.timeout)
            resp.raise_for_status()
            data = resp.json()

            # Navegar por la estructura JSON de respuesta
            author_entry = data.get("author-retrieval-response", {})
            if isinstance(author_entry, list):
                author_entry = author_entry[0] if author_entry else {}

            author_data = author_entry.get("author-profile", {})
            personal_data = author_data.get("personal-data", {})

            # Extraer fields
            given_name = personal_data.get("given-name", "")
            surname = personal_data.get("surname", "")
            name = f"{given_name} {surname}".strip() or personal_data.get("name", "")

            # ORCID
            orcid = personal_data.get("orcid", "")

            # Subject areas
            subject_areas = []
            subject_elements = author_data.get("subject-areas", {}).get("subject-area", [])
            if not isinstance(subject_elements, list):
                subject_elements = [subject_elements] if subject_elements else []
            for subj in subject_elements:
                if isinstance(subj, dict):
                    subject_areas.append(subj.get("$", ""))
                else:
                    subject_areas.append(str(subj))

            # Institución actual (de affiliation-current)
            institution = ""
            aff_current = author_data.get("affiliation-current", {})
            if isinstance(aff_current, dict):
                aff_data = aff_current.get("affiliation-data", {})
                if isinstance(aff_data, dict):
                    institution = aff_data.get("institution-display-name", "")

            # Citaciones
            citation_count = author_entry.get("coredata", {}).get("citation-count", 0)

            logger.info(
                f"Perfil cargado: {name} (ID={author_id}), "
                f"ORCID={orcid}, Institution={institution}, "
                f"Citations={citation_count}"
            )

            return {
                "name": name,
                "given_name": given_name,
                "surname": surname,
                "orcid": orcid,
                "subject_areas": "; ".join(subject_areas) if subject_areas else "",
                "institution_current": institution,
                "citation_count": int(citation_count),
                "author_id": author_id,
            }

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Autor {author_id} no encontrado en Scopus")
            else:
                logger.error(f"Error HTTP obteniendo perfil {author_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error inesperado obteniendo perfil {author_id}: {e}")
            return None

    def _parse_record(self, entry: dict) -> StandardRecord:
        """Convierte una entrada de Scopus Search a StandardRecord"""

        # Autores
        authors = []
        for auth in entry.get("author", []) or []:
            authors.append({
                "name": auth.get("authname"),
                "orcid": None,  # Scopus Search no incluye ORCID
                "scopus_id": auth.get("authid"),
                "is_institutional": False,  # Se determina después
            })

        # DOI
        doi = entry.get("prism:doi") or entry.get("doi")

        # Scopus ID
        scopus_id = entry.get("dc:identifier", "")  # Formato: SCOPUS_ID:xxxxx
        if scopus_id.startswith("SCOPUS_ID:"):
            scopus_id = scopus_id.replace("SCOPUS_ID:", "")

        # Año de publicación
        cover_date = entry.get("prism:coverDate", "")
        pub_year = int(cover_date[:4]) if cover_date and len(cover_date) >= 4 else None

        # Tipo de publicación (intentar múltiples campos)
        subtype = entry.get("subtypeDescription") or entry.get("aggregationType") or entry.get("subtype")

        # Acceso abierto — puede venir como string descriptivo
        oa_status = entry.get("openaccessFlag") or entry.get("openaccess") or entry.get("openAccessStatus")
        is_oa = None
        if oa_status:
            oa_status_lower = str(oa_status).lower().strip()
            is_oa = any(x in oa_status_lower for x in ['all open access', 'gold', 'bronze', 'green', 'hybrid', 'open access', 'true', '1', 'yes'])

        # E-ISSN (para fallback si ISSN no disponible)
        eissn = entry.get("prism:eIssn") or entry.get("prism:eissn")

        return StandardRecord(
            source_name=self.source_name,
            source_id=scopus_id,
            doi=doi,
            title=entry.get("dc:title"),
            publication_year=pub_year,
            publication_date=cover_date,
            publication_type=subtype,
            source_journal=entry.get("prism:publicationName"),
            issn=entry.get("prism:issn"),
            is_open_access=is_oa,
            oa_status=oa_status,
            authors=authors,
            citation_count=int(entry.get("citedby-count", 0)),
            url=None,  # Se puede obtener del link
            raw_data={**entry, "eissn": eissn} if eissn else entry,  # Guardar E-ISSN en raw_data
        )

    # ---------------------------------------------------------
    # BÚSQUEDA POR DOI (para cruce con inventario)
    # ---------------------------------------------------------

    def search_by_doi(self, doi: str) -> Optional[StandardRecord]:
        """
        Busca un solo documento en Scopus por su DOI.

        Args:
            doi: DOI del documento (ej: 10.1016/j.jhydrol.2020.125741)

        Returns:
            StandardRecord si se encuentra, None si no.
        """
        if not self.api_key:
            raise ScopusAPIError("API key de Scopus no configurada.")

        # Limpiar DOI
        clean_doi = doi.strip()
        if clean_doi.startswith("https://doi.org/"):
            clean_doi = clean_doi.replace("https://doi.org/", "")
        elif clean_doi.startswith("http://doi.org/"):
            clean_doi = clean_doi.replace("http://doi.org/", "")

        query = f"DOI({clean_doi})"
        params = {
            "query": query,
            "count": 1,
            "field": (
                "dc:identifier,doi,dc:title,prism:publicationName,"
                "prism:coverDate,subtypeDescription,citedby-count,"
                "author,prism:issn,openaccess,openaccessFlag,"
                "dc:description,authkeywords,prism:volume,prism:issueIdentifier,"
                "prism:pageRange,afid,affiliation"
            ),
        }

        try:
            resp = self.session.get(
                self.SEARCH_URL,
                params=params,
                timeout=self.config.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Error buscando DOI {clean_doi} en Scopus: {e}")
            return None

        entries = data.get("search-results", {}).get("entry", [])
        if not entries or (len(entries) == 1 and "error" in entries[0]):
            return None

        try:
            record = self._parse_record(entries[0])
            record.compute_normalized_fields()
            return record
        except Exception as e:
            logger.warning(f"Error parseando resultado Scopus para DOI {clean_doi}: {e}")
            return None

    def search_by_dois(
        self, dois: List[str], delay: float = 0.25
    ) -> List[StandardRecord]:
        """
        Busca múltiples documentos en Scopus por DOI.

        Args:
            dois: Lista de DOIs a buscar.
            delay: Pausa entre peticiones (seg) para respetar rate-limit.

        Returns:
            Lista de StandardRecords encontrados.
        """
        records: List[StandardRecord] = []
        total = len(dois)

        for i, doi in enumerate(dois, 1):
            record = self.search_by_doi(doi)
            if record:
                records.append(record)
            if i % 50 == 0:
                logger.info(f"  Progreso Scopus DOI: {i}/{total} — encontrados: {len(records)}")
            if delay and i < total:
                time.sleep(delay)

        logger.info(
            f"Búsqueda Scopus por DOI completada: {len(records)} encontrados de {total} consultados."
        )
        return records

    # ---------------------------------------------------------
    # CONSULTA AVANZADA — operadores de campo de Scopus
    # ---------------------------------------------------------

    # Códigos de tipo de documento que acepta DOCTYPE(...)
    DOCTYPE_CODES: Dict[str, str] = {
        "article":           "ar",
        "review":            "re",
        "conference paper":  "cp",
        "book":              "bk",
        "book chapter":      "ch",
        "editorial":         "ed",
        "letter":            "le",
        "note":              "no",
        "short survey":      "sh",
        "erratum":           "er",
        "report":            "rp",
        "abstract report":   "ab",
    }

    @staticmethod
    def build_advanced_query(
        *,
        # ── Contenido ──────────────────────────────────────────
        title: Optional[str] = None,
        abstract: Optional[str] = None,
        keywords: Optional[str] = None,
        title_abs_key: Optional[str] = None,
        # ── Autoría ────────────────────────────────────────────
        author: Optional[str] = None,
        first_author: Optional[str] = None,
        author_id: Optional[str] = None,
        orcid: Optional[str] = None,
        # ── Afiliación ─────────────────────────────────────────
        affiliation_id: Optional[str] = None,
        affiliation_name: Optional[str] = None,
        # ── Fuente ─────────────────────────────────────────────
        source_title: Optional[str] = None,
        issn: Optional[str] = None,
        doi: Optional[str] = None,
        publisher: Optional[str] = None,
        # ── Rango de años ──────────────────────────────────────
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        year_exact: Optional[int] = None,
        # ── Clasificación ──────────────────────────────────────
        document_type: Optional[str] = None,
        subject_area: Optional[str] = None,
        language: Optional[str] = None,
        open_access: Optional[bool] = None,
        # ── Financiación ───────────────────────────────────────
        funder: Optional[str] = None,
        grant_number: Optional[str] = None,
        # ── Cláusula libre adicional ───────────────────────────
        extra: Optional[str] = None,
        operator: str = "AND",
    ) -> str:
        """
        Construye una query para Scopus Search API usando los mismos
        operadores de campo que la búsqueda avanzada de la web de Scopus.

        Referencia de operadores:
          https://dev.elsevier.com/sc_search_tips.html

        Ejemplos equivalentes a la interfaz web:
          TITLE-ABS-KEY(machine learning) AND AF-ID(60106970) AND PUBYEAR > 2018
          AUTH(García) AND AFFIL(Universidad de Antioquia) AND DOCTYPE(ar)
          SRCTITLE(Sustainability) AND OPENACCESS(1) AND PUBYEAR = 2023
          FUND-SPONSOR(Minciencias) AND SUBJAREA(MEDI)

        Tipos de documento (document_type):
          'article', 'review', 'conference paper', 'book', 'book chapter',
          'editorial', 'letter', 'note', 'short survey', 'erratum', 'report'

        Áreas temáticas (subject_area):
          AGRI, ARTS, BIOC, BUSI, CENG, CHEM, COMP, DECI, DENT, EART,
          ECON, ENER, ENGI, ENVI, IMMU, MATE, MATH, MEDI, MULT, NEUR,
          NURS, PHAR, PHYS, PSYC, SOCI, VETE

        Args:
            title:          Buscar en título únicamente   → TITLE(...)
            abstract:       Buscar en resumen             → ABS(...)
            keywords:       Buscar en palabras clave      → KEY(...)
            title_abs_key:  Buscar en título+resumen+kw   → TITLE-ABS-KEY(...)
            author:         Apellido o "Apellido, N."     → AUTH(...)
            first_author:   Solo el primer autor          → AUTHFIRST(...)
            author_id:      Scopus Author ID numérico     → AU-ID(...)
            orcid:          ORCID del autor               → ORCID(...)
            affiliation_id: AF-ID de institución          → AF-ID(...)
            affiliation_name: Nombre de institución       → AFFIL(...)
            source_title:   Nombre de la revista          → SRCTITLE(...)
            issn:           ISSN de la revista            → ISSN(...)
            doi:            DOI exacto                    → DOI(...)
            publisher:      Editorial                     → PUBLISHER(...)
            year_from:      Año mínimo (inclusive)        → PUBYEAR > year-1
            year_to:        Año máximo (inclusive)        → PUBYEAR < year+1
            year_exact:     Año exacto                    → PUBYEAR = year
            document_type:  Tipo de documento             → DOCTYPE(código)
            subject_area:   Código de área temática       → SUBJAREA(...)
            language:       Idioma (English, Spanish...)  → LANGUAGE(...)
            open_access:    True = solo OA               → OPENACCESS(1)
            funder:         Organismo financiador         → FUND-SPONSOR(...)
            grant_number:   Número de grant              → FUND-NO(...)
            extra:          Cláusula libre adicional
            operator:       Operador entre cláusulas ('AND' | 'OR')

        Returns:
            String de query lista para pasar al parámetro ?query=...
        """
        parts: List[str] = []

        # ── Contenido ──────────────────────────────────────────
        if title_abs_key:
            parts.append(f'TITLE-ABS-KEY("{title_abs_key}")')
        if title:
            parts.append(f'TITLE("{title}")')
        if abstract:
            parts.append(f'ABS("{abstract}")')
        if keywords:
            parts.append(f'KEY("{keywords}")')

        # ── Autoría ────────────────────────────────────────────
        if author:
            parts.append(f'AUTH("{author}")')
        if first_author:
            parts.append(f'AUTHFIRST("{first_author}")')
        if author_id:
            parts.append(f"AU-ID({author_id})")
        if orcid:
            cleaned = orcid.replace("https://orcid.org/", "").strip()
            parts.append(f"ORCID({cleaned})")

        # ── Afiliación ─────────────────────────────────────────
        if affiliation_id:
            # Soporta múltiples AF-IDs separados por coma: "60106970,60112687"
            ids = [i.strip() for i in str(affiliation_id).split(",") if i.strip()]
            if len(ids) == 1:
                parts.append(f"AF-ID({ids[0]})")
            else:
                af_parts = " OR ".join(f"AF-ID({i})" for i in ids)
                parts.append(f"({af_parts})")
        if affiliation_name:
            parts.append(f'AFFIL("{affiliation_name}")')

        # ── Fuente ─────────────────────────────────────────────
        if source_title:
            parts.append(f'SRCTITLE("{source_title}")')
        if issn:
            clean_issn = issn.replace("-", "")
            parts.append(f"ISSN({clean_issn})")
        if doi:
            clean_doi = doi.replace("https://doi.org/", "").replace("http://doi.org/", "").strip()
            parts.append(f"DOI({clean_doi})")
        if publisher:
            parts.append(f'PUBLISHER("{publisher}")')

        # ── Rango de años ──────────────────────────────────────
        if year_exact is not None:
            parts.append(f"PUBYEAR = {year_exact}")
        else:
            if year_from is not None:
                parts.append(f"PUBYEAR > {year_from - 1}")
            if year_to is not None:
                parts.append(f"PUBYEAR < {year_to + 1}")

        # ── Clasificación ──────────────────────────────────────
        if document_type:
            # Acepta tanto el nombre legible como el código corto
            dt_lower = document_type.lower().strip()
            code = ScopusExtractor.DOCTYPE_CODES.get(dt_lower, dt_lower)
            parts.append(f"DOCTYPE({code})")
        if subject_area:
            parts.append(f"SUBJAREA({subject_area.upper()})")
        if language:
            parts.append(f"LANGUAGE({language})")
        if open_access is True:
            parts.append("OPENACCESS(1)")

        # ── Financiación ───────────────────────────────────────
        if funder:
            parts.append(f'FUND-SPONSOR("{funder}")')
        if grant_number:
            parts.append(f'FUND-NO("{grant_number}")')

        # ── Cláusula libre ─────────────────────────────────────
        if extra:
            parts.append(extra.strip())

        if not parts:
            raise ValueError(
                "build_advanced_query: debes especificar al menos un criterio de búsqueda."
            )

        sep = f" {operator.upper()} "
        return sep.join(parts)

    def extract_advanced(
        self,
        *,
        title: Optional[str] = None,
        abstract: Optional[str] = None,
        keywords: Optional[str] = None,
        title_abs_key: Optional[str] = None,
        author: Optional[str] = None,
        first_author: Optional[str] = None,
        author_id: Optional[str] = None,
        orcid: Optional[str] = None,
        affiliation_id: Optional[str] = None,
        affiliation_name: Optional[str] = None,
        source_title: Optional[str] = None,
        issn: Optional[str] = None,
        doi_filter: Optional[str] = None,
        publisher: Optional[str] = None,
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        year_exact: Optional[int] = None,
        document_type: Optional[str] = None,
        subject_area: Optional[str] = None,
        language: Optional[str] = None,
        open_access: Optional[bool] = None,
        funder: Optional[str] = None,
        grant_number: Optional[str] = None,
        extra: Optional[str] = None,
        operator: str = "AND",
        max_results: Optional[int] = None,
    ) -> List[StandardRecord]:
        """
        Extrae registros usando la búsqueda avanzada de Scopus.

        Equivale a la pestaña 'Advanced search' de la web de Scopus,
        donde puedes combinar operadores de campo con AND/OR/AND NOT.

        Ejemplos de uso::

            # Artículos de dos instituciones colombianas entre 2020-2024
            extractor.extract_advanced(
                affiliation_id="60106970,60112687",
                year_from=2020, year_to=2024,
                document_type="article",
            )

            # Publicaciones OA de un autor por ORCID sobre machine learning
            extractor.extract_advanced(
                orcid="0000-0002-2096-7900",
                title_abs_key="machine learning",
                open_access=True,
            )

            # Publicaciones en una revista específica financiadas por Minciencias
            extractor.extract_advanced(
                source_title="Biomédica",
                funder="Minciencias",
                year_from=2018,
            )

        Returns:
            Lista de StandardRecord normalizados.
        """
        query = self.build_advanced_query(
            title=title,
            abstract=abstract,
            keywords=keywords,
            title_abs_key=title_abs_key,
            author=author,
            first_author=first_author,
            author_id=author_id,
            orcid=orcid,
            affiliation_id=affiliation_id,
            affiliation_name=affiliation_name,
            source_title=source_title,
            issn=issn,
            doi=doi_filter,
            publisher=publisher,
            year_from=year_from,
            year_to=year_to,
            year_exact=year_exact,
            document_type=document_type,
            subject_area=subject_area,
            language=language,
            open_access=open_access,
            funder=funder,
            grant_number=grant_number,
            extra=extra,
            operator=operator,
        )
        logger.info(f"Scopus advanced query: {query}")
        return self.extract(query=query, max_results=max_results)

    # ---------------------------------------------------------
    # LÓGICA INTERNA
    # ---------------------------------------------------------

    def _build_query(
        self,
        year_from: Optional[int],
        year_to: Optional[int],
        affiliation_id: Optional[str],
    ) -> str:
        """
        Construye query Scopus estándar para la extracción institucional.
        Ejemplo: AF-ID(60000000) AND PUBYEAR > 2019 AND PUBYEAR < 2026
        """
        return self.build_advanced_query(
            affiliation_id=affiliation_id,
            affiliation_name=None if affiliation_id else institution.name,
            year_from=year_from,
            year_to=year_to,
        )
