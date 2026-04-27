import requests

# Obtener esquema OpenAPI
response = requests.get('http://localhost:8000/openapi.json')
if response.status_code == 200:
    openapi = response.json()
    
    # Buscar el endpoint author-h-index
    if '/api/scopus/author-h-index' in openapi['paths']:
        endpoint = openapi['paths']['/api/scopus/author-h-index']['post']
        print('✅ Endpoint registrado en OpenAPI')
        print(f"   Resumen: {endpoint.get('summary', 'N/A')}")
        print(f"   Descripción: {endpoint.get('description', 'N/A')[:80]}...")
        if endpoint.get('parameters'):
            print(f"   Parámetros: {endpoint.get('parameters')[0].get('name')}")
        print(f"   Respuestas soportadas: {list(endpoint.get('responses', {}).keys())}")
    else:
        print('❌ Endpoint no encontrado en OpenAPI')
        print('Endpoints disponibles en /scopus:')
        for path in openapi['paths']:
            if '/scopus' in path:
                print(f'  - {path}')
else:
    print(f'❌ Error al obtener OpenAPI: {response.status_code}')
