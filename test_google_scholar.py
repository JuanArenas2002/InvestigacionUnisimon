#!/usr/bin/env python
"""
Script de prueba rápida para Google Scholar Extractor

Uso:
    python test_google_scholar.py

Nota:
    - Reemplaza los Scholar IDs con los tuyos
    - Necesita: pip install scholarly
"""

import logging
import sys
from typing import List

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_basic_import():
    """Verificar que scholarly está instalado"""
    print("\n" + "="*70)
    print("PRUEBA 1: Verificar dependencias")
    print("="*70)
    try:
        import scholarly
        print("✅ scholarly instalado correctamente")
        return True
    except ImportError:
        print("❌ scholarly NO está instalado")
        print("   Instala con: pip install scholarly")
        return False

def test_extractor_import():
    """Verificar que el extractor puede importarse"""
    print("\n" + "="*70)
    print("PRUEBA 2: Verificar extractor de Google Scholar")
    print("="*70)
    try:
        from extractors.google_scholar.extractor import GoogleScholarExtractor
        print("✅ Extractor importado correctamente")
        return True
    except ImportError as e:
        print(f"❌ Error al importar extractor: {e}")
        return False

def test_single_profile():
    """Prueba extracción de un perfil"""
    print("\n" + "="*70)
    print("PRUEBA 3: Extracción de perfil único")
    print("="*70)
    
    # Scholar IDs de prueba
    test_scholar_ids = [
        "V94aovUAAAAJ",  # Juan Arenas
    ]
    
    try:
        from extractors.google_scholar.extractor import GoogleScholarExtractor
        
        extractor = GoogleScholarExtractor()
        
        print(f"\n🔍 Extrayendo del perfil: {test_scholar_ids[0]}")
        print(f"   (Si no es válido, cámbialo en el script)\n")
        
        records = extractor.extract(
            scholar_ids=test_scholar_ids,
            year_from=2020,
            max_results=5
        )
        
        print(f"✅ Se obtuvieron {len(records)} registros\n")
        
        if records:
            print("Primeros registros:")
            print("-" * 70)
            for i, record in enumerate(records[:3], 1):
                print(f"\n[{i}] {record.title}")
                print(f"    Año: {record.publication_year}")
                # Autores vienen como dicts: [{"name": "...", "orcid": None, ...}, ...]
                if record.authors:
                    author_names = [a.get("name", "Anónimo") if isinstance(a, dict) else str(a) for a in record.authors[:2]]
                    print(f"    Autores: {', '.join(author_names)}")
                else:
                    print(f"    Autores: N/A")
                print(f"    Citas: {record.citation_count}")
                print(f"    URL: {record.url[:60]}..." if record.url else "    URL: N/A")
        
        return True
        
    except ValueError as e:
        print(f"❌ Error de validación: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_multiple_profiles():
    """Prueba extracción de múltiples perfiles"""
    print("\n" + "="*70)
    print("PRUEBA 4: Extracción de múltiples perfiles")
    print("="*70)
    
    # REEMPLAZA ESTOS IDS
    test_scholar_ids = [
        "V94aovUAAAAJ"     # Juan Arenas
    ]
    
    try:
        from extractors.google_scholar.extractor import GoogleScholarExtractor
        
        extractor = GoogleScholarExtractor()
        
        print(f"\n🔍 Extrayendo de {len(test_scholar_ids)} perfil(es)")
        print(f"   IDs: {', '.join(test_scholar_ids)}\n")
        
        records = extractor.extract(
            scholar_ids=test_scholar_ids,
            year_from=2022,
            max_results=10
        )
        
        print(f"✅ Total de {len(records)} registros obtenidos\n")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_record_structure():
    """Validar que los registros tienen la estructura correcta"""
    print("\n" + "="*70)
    print("PRUEBA 5: Validación de estructura de registros")
    print("="*70)
    
    test_scholar_ids = ["V94aovUAAAAJ"]
    
    try:
        from extractors.google_scholar.extractor import GoogleScholarExtractor
        
        extractor = GoogleScholarExtractor()
        
        records = extractor.extract(
            scholar_ids=test_scholar_ids,
            max_results=3
        )
        
        if not records:
            print("⚠️  No hay registros para validar")
            return False
        
        # Campos obligatorios
        required_fields = [
            'source_name', 'title', 'authors', 
            'publication_year', 'citation_count'
        ]
        
        print(f"\nValidando {len(records)} registro(s)...")
        
        all_valid = True
        for i, record in enumerate(records, 1):
            missing = []
            for field in required_fields:
                value = getattr(record, field, None)
                if not value:
                    missing.append(field)
            
            if missing:
                print(f"⚠️  Registro {i}: Campos vacíos: {missing}")
                all_valid = False
            else:
                print(f"✅ Registro {i}: Válido")
        
        return all_valid
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Ejecutar todas las pruebas"""
    print("\n" + "#"*70)
    print("# PRUEBA COMPLETA: GOOGLE SCHOLAR EXTRACTOR")
    print("#"*70)
    
    results = {
        "Dependencias": test_basic_import(),
        "Extractor": test_extractor_import(),
    }
    
    # Continuar solo si pasaron pruebas básicas
    if results["Dependencias"] and results["Extractor"]:
        results["Perfil único"] = test_single_profile()
        results["Múltiples perfiles"] = test_multiple_profiles()
        results["Estructura"] = test_record_structure()
    
    # Resumen
    print("\n" + "="*70)
    print("RESUMEN DE PRUEBAS")
    print("="*70)
    
    for test_name, result in results.items():
        status = "✅ PASÓ" if result else "❌ FALLÓ"
        print(f"{test_name:.<40} {status}")
    
    passed = sum(1 for r in results.values() if r)
    total = len(results)
    
    print(f"\nTotal: {passed}/{total} pruebas pasadas")
    
    if passed == total:
        print("\n🎉 ¡Todas las pruebas pasaron!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} prueba(s) fallaron")
        print("\nSoluciones:")
        print("1. Instala scholarly: pip install scholarly")
        print("2. Verifica Scholar IDs en: https://scholar.google.com/")
        print("3. Lee la guía completa: docs/GOOGLE_SCHOLAR_TESTING.md")
        return 1

if __name__ == "__main__":
    sys.exit(main())
