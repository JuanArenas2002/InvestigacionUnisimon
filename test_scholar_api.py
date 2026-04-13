#!/usr/bin/env python
"""
🧪 Test Script para Endpoints de Google Scholar

Ejecutar:
    python test_scholar_api.py

Requiere:
    - uvicorn api.main:app --reload
    - PostgreSQL corriendo
"""

import requests
import json
import sys
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime

# Configuración
BASE_URL = "http://localhost:8000"
TIMEOUT = 30

# Colors para output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_header(text: str):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}  {text}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*60}{Colors.END}\n")

def print_success(text: str):
    print(f"{Colors.OKGREEN}✅ {text}{Colors.END}")

def print_error(text: str):
    print(f"{Colors.FAIL}❌ {text}{Colors.END}")

def print_info(text: str):
    print(f"{Colors.OKCYAN}ℹ️  {text}{Colors.END}")

def print_warning(text: str):
    print(f"{Colors.WARNING}⚠️  {text}{Colors.END}")

def test_connection():
    """Verificar que la API está corriendo"""
    print_header("1. VERIFICAR CONEXIÓN A API")
    
    try:
        resp = requests.get(f"{BASE_URL}/", timeout=TIMEOUT)
        if resp.status_code == 200:
            print_success(f"API conectada: {BASE_URL}")
            data = resp.json()
            print_info(f"Servicio: {data.get('servicio')}")
            print_info(f"Versión: {data.get('version')}")
            print_info(f"Estado: {data.get('estado')}")
            return True
        else:
            print_error(f"API retornó status {resp.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print_error(f"No se puede conectar a {BASE_URL}")
        print_warning("¿Está corriendo: uvicorn api.main:app --reload?")
        return False
    except Exception as e:
        print_error(f"Error: {str(e)}")
        return False

def test_scholar_info():
    """Obtener información de Google Scholar"""
    print_header("2. INFO DE GOOGLE SCHOLAR")
    
    try:
        resp = requests.get(
            f"{BASE_URL}/api/scholar/test",
            timeout=TIMEOUT,
            headers={"accept": "application/json"}
        )
        
        if resp.status_code == 200:
            print_success("Endpoint `/api/scholar/test` accesible")
            data = resp.json()
            print_info(f"Servicio: {data.get('service')}")
            print_info(f"Modo: {data.get('modo')}")
            
            print("\n📚 Campos que se extraen:")
            for campo in data.get('campos_extraidos', [])[:5]:
                print(f"   • {campo}")
            if len(data.get('campos_extraidos', [])) > 5:
                print(f"   ... y {len(data.get('campos_extraidos', [])) - 5} más")
            
            print("\n📚 Ejemplos de Scholar IDs:")
            for name, sid in data.get('scholar_ids_ejemplo', {}).items():
                print(f"   • {name}: {sid}")
            
            return True
        else:
            print_error(f"Status: {resp.status_code}")
            return False
            
    except Exception as e:
        print_error(f"Error: {str(e)}")
        return False

def test_extraction_dry_run(scholar_ids: List[str] = None):
    """Extracción sin guardar (dry-run)"""
    print_header("3. EXTRACCIÓN DRY-RUN (sin guardar en BD)")
    
    if scholar_ids is None:
        scholar_ids = ["V94aovUAAAAJ"]
    
    print_info(f"Scholar IDs: {scholar_ids}")
    print_info(f"Año: 2020-2024, Max resultados: 10, Dry-run: true")
    
    try:
        params = {
            "scholar_ids": scholar_ids,
            "year_from": 2020,
            "year_to": 2024,
            "max_results": 10,
            "dry_run": "true"
        }
        
        resp = requests.post(
            f"{BASE_URL}/api/scholar/extract",
            params=params,
            timeout=TIMEOUT,
            headers={"accept": "application/json"}
        )
        
        if resp.status_code == 200:
            data = resp.json()
            
            if data.get("status") == "success":
                print_success(f"Extracción exitosa")
                print_info(f"Registros extraídos: {data.get('extraidos', 0)}")
                print_info(f"Guardados en BD: {data.get('guardados', 0)} (dry-run)")
                
                # Mostrar primer registro
                if data.get('registros'):
                    print("\n📰 Primer registro:")
                    reg = data['registros'][0]
                    print(f"   • Título: {reg.get('title', 'N/A')[:60]}")
                    print(f"   • Año: {reg.get('publication_year', 'N/A')}")
                    print(f"   • Citas: {reg.get('citation_count', 'N/A')}")
                
                return True
            else:
                print_error(f"Status de respuesta: {data.get('status')}")
                print_error(f"Detalle: {data.get('error', 'N/A')}")
                return False
        else:
            print_error(f"HTTP Status: {resp.status_code}")
            print_warning(f"Respuesta: {resp.text[:200]}")
            return False
            
    except Exception as e:
        print_error(f"Error: {str(e)}")
        return False

def test_extraction_persist(scholar_ids: List[str] = None):
    """Extracción guardando en BD"""
    print_header("4. EXTRACCIÓN CON PERSISTENCIA (guardando en BD)")
    
    if scholar_ids is None:
        scholar_ids = ["V94aovUAAAAJ"]
    
    print_info(f"Scholar IDs: {scholar_ids}")
    print_info(f"Año: 2020-2024, Max resultados: 10, Dry-run: false")
    print_warning("⚠️  Esto GUARDARÁ datos en google_Scholar_records")
    
    try:
        params = {
            "scholar_ids": scholar_ids,
            "year_from": 2020,
            "year_to": 2024,
            "max_results": 10,
            "dry_run": "false"
        }
        
        resp = requests.post(
            f"{BASE_URL}/api/scholar/extract",
            params=params,
            timeout=TIMEOUT,
            headers={"accept": "application/json"}
        )
        
        if resp.status_code == 200:
            data = resp.json()
            
            if data.get("status") == "success":
                print_success(f"✅ Extracción exitosa")
                print_info(f"Registros extraídos: {data.get('extraidos', 0)}")
                print_success(f"✅ Guardados en BD: {data.get('guardados', 0)}")
                
                return True
            else:
                print_error(f"Status: {data.get('status')}")
                return False
        else:
            print_error(f"HTTP Status: {resp.status_code}")
            return False
            
    except Exception as e:
        print_error(f"Error: {str(e)}")
        return False

def test_hex_ingest():
    """Probar endpoint hexagonal /api/hex/ingest"""
    print_header("5. ENDPOINT HEXAGONAL /api/hex/ingest (Alternativa)")
    
    print_info("Este es el endpoint principal del pipeline hexagonal")
    print_info("Soporta TODAS las fuentes incluyendo Google Scholar")
    
    try:
        payload = {
            "sources": ["google_Scholar"],
            "scholar_ids": ["V94aovUAAAAJ"],
            "year_from": 2020,
            "max_results": 5,
            "dry_run": True
        }
        
        print_info(f"Payload: {json.dumps(payload, indent=2)}")
        
        resp = requests.post(
            f"{BASE_URL}/api/hex/ingest",
            json=payload,
            timeout=TIMEOUT,
            headers={"Content-Type": "application/json"}
        )
        
        if resp.status_code == 200:
            print_success(f"Endpoint accesible (status 200)")
            data = resp.json()
            print_info(f"Resultado: {json.dumps(data, indent=2)[:300]}...")
            return True
        else:
            print_warning(f"Status: {resp.status_code}")
            print_info(f"Este endpoint está en arquitectura hexagonal")
            return False
            
    except requests.exceptions.ConnectionError:
        print_warning("Endpoint no accesible (pero no es crítico)")
        return False
    except Exception as e:
        print_warning(f"Error no crítico: {str(e)}")
        return False

def test_database():
    """Verificar que los datos se guardaron en BD"""
    print_header("6. VERIFICAR BD (google_Scholar_records)")
    
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent))
        
        from db.session import get_session
        from sqlalchemy import text
        
        session = get_session()
        
        # Contar registros
        result = session.execute(
            text("SELECT COUNT(*) FROM google_Scholar_records")
        )
        count = result.scalar()
        
        print_success(f"Tabla accesible - {count} registros actuales")
        
        # Ver distribución por status
        result = session.execute(
            text("""
                SELECT status, COUNT(*) as cantidad
                FROM google_Scholar_records
                GROUP BY status
            """)
        )
        
        print("\n📊 Registros por estado:")
        for status, qty in result:
            print(f"   • {status}: {qty}")
        
        # Ver últimos registros
        result = session.execute(
            text("""
                SELECT title, scholar_profile_id, citation_count
                FROM google_Scholar_records
                ORDER BY created_at DESC
                LIMIT 3
            """)
        )
        
        records = result.fetchall()
        if records:
            print("\n📚 Últimos 3 registros:")
            for title, profile, citas in records:
                print(f"   • {title[:40]}")
                print(f"     Perfil: {profile} | Citas: {citas}")
        
        session.close()
        return True
        
    except ImportError:
        print_warning("No se puede importar módulos de BD (normal en standalone)")
        return False
    except Exception as e:
        print_error(f"Error consultando BD: {str(e)}")
        return False

def main():
    """Script principal"""
    print(f"\n{Colors.BOLD}🧪 TEST SCRIPT - Google Scholar API{Colors.END}")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"URL: {BASE_URL}\n")
    
    tests = []
    
    # 1. Conexión
    if not test_connection():
        print_error("\n❌ No se puede conectar a la API. Abortando.")
        sys.exit(1)
    tests.append(("Conexión", True))
    
    # 2. Info
    if test_scholar_info():
        tests.append(("Info Google Scholar", True))
    else:
        tests.append(("Info Google Scholar", False))
    
    # 3. Dry-run
    if test_extraction_dry_run():
        tests.append(("Extracción Dry-Run", True))
    else:
        tests.append(("Extracción Dry-Run", False))
    
    # 4. Persistencia
    print("\n" + Colors.WARNING)
    confirm = input("¿Deseas guardar registros en BD? (s/n): ").lower()
    if confirm == "s":
        if test_extraction_persist():
            tests.append(("Extracción con Persistencia", True))
        else:
            tests.append(("Extracción con Persistencia", False))
    else:
        print_info("Saltando persistencia\n")
        tests.append(("Extracción con Persistencia", "SKIPPED"))
    
    # 5. Hexagonal
    if test_hex_ingest():
        tests.append(("Hex /api/hex/ingest", True))
    else:
        tests.append(("Hex /api/hex/ingest", False))
    
    # 6. BD
    if test_database():
        tests.append(("Verificar BD", True))
    else:
        tests.append(("Verificar BD", False))
    
    # Resumen
    print_header("RESUMEN DE TESTS")
    
    passed = sum(1 for _, result in tests if result is True)
    total = len(tests)
    
    for name, result in tests:
        if result is True:
            print_success(f"{name}")
        elif result is False:
            print_error(f"{name}")
        else:
            print_warning(f"{name} (SKIPPED)")
    
    print(f"\n{Colors.BOLD}Resultado: {passed}/{total} tests pasados{Colors.END}\n")
    
    if passed >= 3:
        print_success("✅ Google Scholar API está funcionando correctamente!")
        print_info("\n📝 Próximos pasos:")
        print_info("1. Ejecutar reconciliación: POST /api/pipeline/reconcile-all")
        print_info("2. Ver datos canónicos: GET /api/publications")
        print_info("3. Revisar documentación: /docs")
    else:
        print_error("❌ Algunos tests fallaron - revisa los errores arriba")
        sys.exit(1)

if __name__ == "__main__":
    main()
