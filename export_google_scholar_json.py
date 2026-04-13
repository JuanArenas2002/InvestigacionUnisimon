#!/usr/bin/env python
"""
Exportar datos extraídos de Google Scholar a JSON

Uso:
    python export_google_scholar_json.py

Genera un archivo: google_scholar_export.json
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import asdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def export_to_json():
    """Extrae datos y los guarda en JSON"""
    
    try:
        from extractors.google_scholar.extractor import GoogleScholarExtractor
        
        print("\n" + "="*70)
        print("EXPORTANDO DATOS DE GOOGLE SCHOLAR A JSON")
        print("="*70 + "\n")
        
        # Inicializar extractor
        extractor = GoogleScholarExtractor()
        
        # Extraer datos
        print("🔍 Extrayendo publicaciones de V94aovUAAAAJ...")
        records = extractor.extract(
            scholar_ids=["V94aovUAAAAJ"],
            year_from=2015,
            max_results=20
        )
        
        print(f"✅ Se obtuvieron {len(records)} registros\n")
        
        # Convertir a diccionarios
        data = {
            "extractor": "google_scholar",
            "scholar_id": "V94aovUAAAAJ",
            "total_records": len(records),
            "exported_at": datetime.now().isoformat(),
            "records": []
        }
        
        # Procesar cada registro
        for record in records:
            # Convertir a dict (usando asdict de dataclasses)
            record_dict = asdict(record)
            data["records"].append(record_dict)
        
        # Guardar JSON
        output_file = Path("google_scholar_export.json")
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Datos guardados en: {output_file.absolute()}\n")
        
        # Mostrar preview
        print("="*70)
        print("PREVIEW DEL ARCHIVO JSON")
        print("="*70 + "\n")
        
        print(f"Total de registros: {data['total_records']}\n")
        
        # Mostrar primeros 2 registros completos
        for i, record in enumerate(data["records"][:2], 1):
            print(f"\n{'='*70}")
            print(f"REGISTRO {i}")
            print(f"{'='*70}")
            print(json.dumps(record, indent=2, ensure_ascii=False))
        
        print(f"\n\n📊 Resumen:")
        print(f"   - Total de registros: {len(records)}")
        print(f"   - Archivo: {output_file}")
        print(f"   - Tamaño: {output_file.stat().st_size} bytes")
        
        # Estadísticas
        print(f"\n📈 Estadísticas:")
        
        years = [r["publication_year"] for r in data["records"] if r.get("publication_year")]
        if years:
            print(f"   - Años: {min(years)} - {max(years)}")
            print(f"   - Promedio de citas: {sum([r['citation_count'] for r in data['records']]) / len(data['records']):.1f}")
        
        print("\n✅ Exportación completada\n")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = export_to_json()
    exit(0 if success else 1)
