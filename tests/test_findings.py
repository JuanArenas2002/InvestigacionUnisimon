#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test to display improved professional findings."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.analysis import generar_hallazgos, CampoDisciplinar

# Test data for a senior researcher in health sciences
def test_professional_findings():
    """Display professional findings for a senior researcher."""
    
    print("\n" + "="*80)
    print("PROFESSIONAL FINDINGS ANALYSIS - Senior Health Sciences Researcher")
    print("="*80)
    
    # Sample data: researcher with strong impact
    total_arts = 83
    total_citas = 1271
    h_index = 16
    cpp = 15.3
    mediana = 10.2
    pct_citados = 78.3
    años = list(range(2015, 2025))  # 2015-2024
    pubs = [8, 7, 9, 11, 15, 14, 12, 10, 8, 9]  # Publications per year
    cites = [45, 78, 92, 156, 243, 310, 287, 201, 145, 114]  # Citations per year
    año_pico = 2020
    año_max_pub = 2019
    
    # Generate findings
    positivos, negativos, notas = generar_hallazgos(
        total_arts=total_arts,
        total_citas=total_citas,
        h_index=h_index,
        cpp=cpp,
        mediana=mediana,
        pct_citados=pct_citados,
        años=años,
        pubs=pubs,
        cites=cites,
        año_pico=año_pico,
        año_max_pub=año_max_pub,
        campo=CampoDisciplinar.CIENCIAS_SALUD,
        db_session=None  # Using fallback thresholds
    )
    
    # Display findings
    print("\n--- KEY METRICS ---")
    print(f"Total Publications: {total_arts}")
    print(f"Total Citations: {total_citas}")
    print(f"H-Index: {h_index}")
    print(f"CPP (Citations Per Publication): {cpp}")
    print(f"Median Citations: {mediana}")
    print(f"% Cited Articles: {pct_citados}%")
    print(f"Peak Year: {año_pico}")
    
    print("\n" + "─"*80)
    print("POSITIVE FINDINGS (STRENGTHS)")
    print("─"*80)
    for i, hallazgo in enumerate(positivos, 1):
        print(f"\n{i}. {hallazgo}")
    
    print("\n" + "─"*80)
    print("AREAS FOR IMPROVEMENT (CHALLENGES)")
    print("─"*80)
    for i, hallazgo in enumerate(negativos, 1):
        print(f"\n{i}. {hallazgo}")
    
    if notas:
        print("\n" + "─"*80)
        print("CLARIFYING NOTES")
        print("─"*80)
        for i, nota in enumerate(notas, 1):
            print(f"\n{i}. {nota}")
    
    print("\n" + "="*80)
    print(f"Total Positive Findings: {len(positivos)}")
    print(f"Total Areas for Improvement: {len(negativos)}")
    print(f"Clarifying Notes: {len(notas)}")
    print("="*80 + "\n")
    
    return True


if __name__ == "__main__":
    try:
        test_professional_findings()
        print("[SUCCESS] Professional findings generated successfully!")
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
