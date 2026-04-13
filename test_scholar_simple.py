#!/usr/bin/env python
"""
🧪 Test Simple - Google Scholar Endpoint Fix

Ejecutar:
    python test_scholar_simple.py
"""

import requests
import json

BASE_URL = "http://localhost:8000"

def test_endpoint():
    """Test del endpoint arreglado"""
    
    print("\n🧪 Testing /api/scholar/extract\n")
    
    # Test 1: Info
    print("1️⃣ GET /api/scholar/test")
    resp = requests.get(f"{BASE_URL}/api/scholar/test")
    print(f"   Status: {resp.status_code}")
    if resp.status_code == 200:
        print(f"   ✅ Endpoint accesible")
    
    # Test 2: Extracción dry-run (JSON Body - Correcto)
    print("\n2️⃣ POST /api/scholar/extract (Dry Run - sin guardar)")
    
    payload = {
        "scholar_ids": ["V94aovUAAAAJ"],
        "year_from": 2020,
        "year_to": 2024,
        "max_results": 5,
        "dry_run": True
    }
    
    print(f"   Payload: {json.dumps(payload, indent=6)}")
    
    resp = requests.post(
        f"{BASE_URL}/api/scholar/extract",
        json=payload,
        headers={"Content-Type": "application/json"}
    )
    
    print(f"   Status: {resp.status_code}")
    data = resp.json()
    
    if data.get("status") == "success":
        print(f"   ✅ Éxito!")
        print(f"   Extraídos: {data.get('extraidos')}")
        print(f"   Guardados: {data.get('guardados')}")
        if data.get('registros'):
            print(f"   Primer registro: {data['registros'][0].get('title', 'N/A')[:50]}")
    else:
        print(f"   ❌ Error: {data.get('error')}")
        print(f"   Tipo: {data.get('tipo')}")
    
    # Test 3: Extracción guardando (JSON Body)
    print("\n3️⃣ POST /api/scholar/extract (CON PERSISTENCIA - guardará en BD)")
    
    payload["dry_run"] = False
    payload["max_results"] = 3  # Menos registros para testing
    
    confirm = input("   ¿Deseas guardar en BD? (s/n): ").lower()
    
    if confirm == "s":
        resp = requests.post(
            f"{BASE_URL}/api/scholar/extract",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        print(f"   Status: {resp.status_code}")
        data = resp.json()
        
        if data.get("status") == "success":
            print(f"   ✅ Éxito!")
            print(f"   Extraídos: {data.get('extraidos')}")
            print(f"   ✅ Guardados en BD: {data.get('guardados')}")
        else:
            print(f"   ❌ Error: {data.get('error')}")
    else:
        print("   ⏭️ Saltando persistencia")

if __name__ == "__main__":
    try:
        test_endpoint()
        print("\n✅ Testing completado\n")
    except requests.exceptions.ConnectionError:
        print("❌ No se puede conectar a API")
        print("   ¿Está corriendo: uvicorn api.main:app --reload?")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
