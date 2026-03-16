#!/usr/bin/env python3
"""Test rápido de la API key de Scopus contra el Serial Title API."""

import requests
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("SCOPUS_API_KEY")
print(f"API Key: {api_key}")

if not api_key:
    print("❌ SCOPUS_API_KEY no configurada en .env")
    exit(1)

# Test: ISSN válido conocido
issn = "09302794"  # Una de las que falló
url = f"https://api.elsevier.com/content/serial/title/issn/{issn}"

headers = {
    "X-ELS-APIKey": api_key,
    "Accept": "application/json",
}

print(f"\n🔍 Probando Serial Title API...")
print(f"   URL: {url}")
print(f"   Headers: {headers}")

try:
    resp = requests.get(url, params={"view": "ENHANCED"}, headers=headers, timeout=10)
    print(f"\n📊 Respuesta:")
    print(f"   Status: {resp.status_code} {resp.reason}")
    if resp.status_code == 401:
        print(f"   ❌ UNAUTHORIZED — La API key es inválida o sin permisos")
    elif resp.status_code == 200:
        print(f"   ✅ OK — La API key funciona!")
        print(f"   Contenido: {resp.json()}")
    else:
        print(f"   ⚠️  Otro error: {resp.text}")
except Exception as e:
    print(f"   ❌ Excepción: {e}")
