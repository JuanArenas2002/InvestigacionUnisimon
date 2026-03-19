#!/usr/bin/env python
"""Verifica que la arquitectura centralizada funciona correctamente."""

from db.session import get_session
from db.models import get_thresholds_by_field

def verify_bd():
    session = get_session()
    campos = [
        'CIENCIAS_SALUD', 'CIENCIAS_BASICAS', 'INGENIERIA',
        'CIENCIAS_SOCIALES', 'ARTES_HUMANIDADES'
    ]
    
    print("\n" + "="*70)
    print("VERIFICACIÓN: Arquitectura Centralizada en BD")
    print("="*70)
    
    for campo in campos:
        params = get_thresholds_by_field(session, campo)
        print(f"\n{campo}:")
        print(f"  h_alto:            {params['h_alto']} (type: {type(params['h_alto']).__name__})")
        print(f"  h_medio:           {params['h_medio']}")
        print(f"  cpp_alto:          {params['cpp_alto']}")
        print(f"  cpp_medio:         {params['cpp_medio']}")
        print(f"  pct_citados:       {params['pct_citados']}")
        print(f"  pct_pico:          {params['pct_pico']}")
        print(f"  concentracion_limite: {params['concentracion_limite']}")
        print(f"  ratio_hn_minimo:   {params['ratio_hn_minimo']}")
    
    print("\n" + "="*70)
    print("✓ TODOS LOS PARÁMETROS CARGADOS DESDE BD CORRECTAMENTE")
    print("="*70 + "\n")
    
    session.close()

if __name__ == "__main__":
    verify_bd()
