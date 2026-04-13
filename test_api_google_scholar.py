#!/usr/bin/env python
"""
Test de endpoints Google Scholar via API

Uso:
    python test_api_google_scholar.py

Requiere:
    • API corriendo: uvicorn api.main:app --reload --port 8000
"""

import requests
import json
import time
from typing import Dict, Optional, List

API_BASE_URL = "http://localhost:8000"
INGEST_ENDPOINT = f"{API_BASE_URL}/ingest"

class GoogleScholarAPITester:
    """Test de endpoints Google Scholar"""
    
    def __init__(self, base_url: str = API_BASE_URL):
        self.base_url = base_url
        self.ingest_url = f"{base_url}/ingest"
    
    def check_api_running(self) -> bool:
        """Verifica que la API está corriendo"""
        try:
            response = requests.get(f"{self.base_url}/docs", timeout=5)
            return response.status_code == 200
        except requests.exceptions.ConnectionError:
            return False
    
    def extract(
        self,
        scholar_ids: List[str],
        year_from: Optional[int] = None,
        year_to: Optional[int] = None,
        max_results: int = 50,
        dry_run: bool = False,
    ) -> Optional[Dict]:
        """
        Extrae publicaciones de Google Scholar via API.
        
        Args:
            scholar_ids: Lista de Scholar IDs
            year_from: Año inicial (optional)
            year_to: Año final (optional)
            max_results: Máximo de registros
            dry_run: Si True, no guarda en BD
        
        Returns:
            Response JSON o None si error
        """
        
        payload = {
            "sources": ["google_scholar"],
            "scholar_ids": scholar_ids,
            "max_results": max_results,
            "dry_run": dry_run,
        }
        
        if year_from:
            payload["year_from"] = year_from
        if year_to:
            payload["year_to"] = year_to
        
        print(f"\n📤 Enviando request:")
        print(f"   URL: POST {self.ingest_url}")
        print(f"   Payload: {json.dumps(payload, indent=2)}")
        
        try:
            start = time.time()
            response = requests.post(
                self.ingest_url,
                json=payload,
                timeout=300,
                headers={"Content-Type": "application/json"}
            )
            elapsed = time.time() - start
            
            print(f"\n📬 Response: HTTP {response.status_code} ({elapsed:.2f}s)")
            
            if response.status_code != 200:
                print(f"❌ Error:")
                print(response.text)
                return None
            
            result = response.json()
            return result
            
        except requests.exceptions.ConnectionError:
            print("❌ No se puede conectar a la API")
            print(f"   ¿Está corriendo? uvicorn api.main:app --reload")
            return None
        except requests.exceptions.Timeout:
            print("❌ Timeout (la extracción tomó más de 5 minutos)")
            return None
        except Exception as e:
            print(f"❌ Error: {e}")
            return None
    
    def print_result(self, result: Dict) -> None:
        """Imprime resultado de forma legible"""
        
        print("\n" + "="*70)
        print("RESULTADO DE EXTRACCIÓN")
        print("="*70)
        
        # Status
        print(f"\n✅ Status: {result['status']}")
        
        # Fuentes
        print(f"\n📊 Fuentes procesadas: {result['selected_sources']}")
        
        # Pipeline stages
        print(f"\n🔄 Etapas del Pipeline:")
        stages = result['stages']
        print(f"   Recolectados:  {stages['collect']:>4}")
        print(f"   Deduplicados:  {stages['deduplicate']:>4}")
        print(f"   Normalizados:  {stages['normalize']:>4}")
        print(f"   Coincidencias: {stages['match']:>4}")
        print(f"   Enriquecidos:  {stages['enrich']:>4}")
        
        # Persistence
        print(f"\n💾 Persistencia:")
        pers = result['persistence']
        print(f"   Autores guardados:    {pers['authors_saved']:>4}")
        print(f"   Registros fuente:     {pers['source_saved']:>4}")
        print(f"   Publicaciones canónicas: {pers['canonical_upserted']:>4}")
        print(f"   Dry run: {pers['dry_run']}")
        
        # Por fuente
        print(f"\n📈 Por fuente:")
        for source, count in result['by_source'].items():
            print(f"   {source}: {count}")
        
        # Errores
        if result['errors']:
            print(f"\n⚠️  Errores:")
            for source, error in result['errors'].items():
                print(f"   {source}: {error}")
        else:
            print(f"\n✅ Sin errores")

def main():
    """Ejecutar pruebas"""
    
    print("\n" + "#"*70)
    print("# TEST: Google Scholar API Endpoints")
    print("#"*70)
    
    tester = GoogleScholarAPITester()
    
    # Verificar API
    print("\n🔍 Verificando que API está corriendo...")
    if not tester.check_api_running():
        print("❌ API no responde en http://localhost:8000")
        print("   Inicia con: uvicorn api.main:app --reload --port 8000")
        return 1
    
    print("✅ API disponible")
    
    # Prueba 1: Dry run (sin guardar)
    print("\n" + "="*70)
    print("PRUEBA 1: Extracción simulada (dry_run=true)")
    print("="*70)
    
    result1 = tester.extract(
        scholar_ids=["V94aovUAAAAJ"],
        year_from=2020,
        max_results=5,
        dry_run=True
    )
    
    if result1:
        tester.print_result(result1)
    else:
        print("❌ Prueba 1 falló")
        return 1
    
    # Prueba 2: Con filtro de años
    print("\n" + "="*70)
    print("PRUEBA 2: Con filtro de años (2022-2024)")
    print("="*70)
    
    result2 = tester.extract(
        scholar_ids=["V94aovUAAAAJ"],
        year_from=2022,
        year_to=2024,
        max_results=10,
        dry_run=True
    )
    
    if result2:
        tester.print_result(result2)
    else:
        print("❌ Prueba 2 falló")
        return 1
    
    # Prueba 3: Extracción real (guardar en BD)
    print("\n" + "="*70)
    print("PRUEBA 3: Extracción real (guardando en BD)")
    print("="*70)
    print("\n⚠️  Esto guardará datos en la base de datos")
    
    result3 = tester.extract(
        scholar_ids=["V94aovUAAAAJ"],
        max_results=10,
        dry_run=False  # ← Guardará en BD
    )
    
    if result3:
        tester.print_result(result3)
    else:
        print("❌ Prueba 3 falló")
        return 1
    
    # Resumen
    print("\n" + "="*70)
    print("✅ TODAS LAS PRUEBAS COMPLETADAS")
    print("="*70)
    
    print("\n📚 Documentación:")
    print("   • docs/GOOGLE_SCHOLAR_ENDPOINTS.md")
    print("   • docs/GOOGLE_SCHOLAR_TESTING.md")
    
    print("\n🔗 URLs útiles:")
    print(f"   • API Docs: http://localhost:8000/docs")
    print(f"   • Swagger: http://localhost:8000/swagger")
    print(f"   • ReDoc: http://localhost:8000/redoc")
    
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
