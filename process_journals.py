#!/usr/bin/env python
"""Procesar archivo Excel de revistas con el endpoint de cobertura Scopus."""

import requests
import sys

def main():
    file_path = 'REVISTAS ANALIZAR.xlsx'
    
    print(f'[*] Leyendo archivo: {file_path}')
    try:
        with open(file_path, 'rb') as f:
            files = {'file': (file_path, f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            print(f'[*] Enviando a endpoint...')
            response = requests.post(
                'http://127.0.0.1:8000/api/pipeline/scopus/journal-coverage/bulk-from-file?max_workers=8',
                files=files,
                timeout=120
            )
    except FileNotFoundError:
        print(f'✗ Error: No se encontró el archivo {file_path}')
        return 1
    except Exception as e:
        print(f'✗ Error: {e}')
        return 1

    print(f'[*] Status Code: {response.status_code}')
    
    if response.status_code == 200:
        output_file = 'journal_coverage_result.xlsx'
        with open(output_file, 'wb') as out:
            out.write(response.content)
        print(f'✓ Éxito: Archivo guardado como {output_file}')
        print(f'✓ Tamaño: {len(response.content)} bytes')
        return 0
    else:
        print(f'✗ Error {response.status_code}:')
        print(response.text[:800])
        return 1

if __name__ == '__main__':
    sys.exit(main())
