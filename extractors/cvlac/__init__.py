"""
Paquete extractor de CvLAC — Currículum Vitae de Latinoamérica y el Caribe.

Fuente primaria: API JSON de Metrik Unisimon
    GET https://metrik.unisimon.edu.co/scienti/cvlac/{cc_investigador}

Entrada: cédula de ciudadanía del investigador (cc_investigadores=[...]).

Estructura interna (arquitectura DDD):
  domain/
    record_parser.py  → mapea item normalizado de Metrik a StandardRecord
    html_parser.py    → (legacy) parser HTML del portal Minciencias
  application/
    metrik_service.py → cliente JSON Metrik: fetch, validar, normalizar (PRINCIPAL)
    profile_service.py → (legacy) scraper HTML de Minciencias
  infrastructure/
    http_client.py    → sesión HTTP con User-Agent (usada por el scraper legacy)
  extractor.py        → CvlacExtractor: implementa BaseExtractor con Metrik como fuente

Punto de entrada público:

    from extractors.cvlac import CvlacExtractor
    from extractors.cvlac import CvlacExtractor, CvlacScrapingError
"""

from extractors.cvlac.extractor import CvlacExtractor
from extractors.cvlac._exceptions import CvlacScrapingError

__all__ = ["CvlacExtractor", "CvlacScrapingError"]
