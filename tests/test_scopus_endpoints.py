#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test Scopus endpoints for PNG and PDF generation."""

import sys
import os

# Configure UTF-8 encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from pathlib import Path
import json
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.chart_generator import generate_investigator_chart_file
from api.routers.charts import generate_investigator_report
from api.schemas.charts import InvestigatorChartRequest

# Test constants
SCOPUS_AU_ID = "57193767797"  # Known test Scopus author ID


def test_scopus_generate():
    """Test /generate endpoint - PNG only without analysis."""
    print("\n" + "="*60)
    print("TEST 1: /generate endpoint (PNG only)")
    print("="*60)
    
    try:
        # Call the chart generation service directly
        chart_data = generate_investigator_chart_file(
            author_id=SCOPUS_AU_ID,
            affiliation_ids=["60106970", "60112687"],  # Default USB affiliations
        )
        
        # Verify PNG was generated
        if 'file_path' not in chart_data:
            print("❌ ERROR: No PNG file path in response")
            return False
        
        png_path = Path(chart_data['file_path'])
        if not png_path.exists():
            print(f"❌ ERROR: PNG file not found at {png_path}")
            return False
        
        png_size = png_path.stat().st_size / 1024  # Size in KB
        
        # Verify statistics are schema-compliant
        stats = chart_data.get('statistics', {})
        required_stats = [
            'total_publications', 'min_year', 'max_year', 'avg_per_year',
            'peak_year', 'peak_publications', 'active_years', 'publications_by_year'
        ]
        
        missing_stats = [s for s in required_stats if s not in stats]
        if missing_stats:
            print(f"❌ ERROR: Missing statistics fields: {missing_stats}")
            return False
        
        print(f"[PASS] PNG generated successfully: {png_path.name}")
        print(f"[PASS] File size: {png_size:.1f} KB")
        print(f"[PASS] Statistics schema: VALID")
        print(f"   - Total publications: {stats.get('total_publications')}")
        print(f"   - H-Index: {chart_data.get('h_index', 'N/A')}")
        print(f"   - CPP: {chart_data.get('cpp', 'N/A')}")
        print(f"   - Percent cited: {chart_data.get('percent_cited', 'N/A'):.1f}%")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_scopus_generate_report():
    """Test /generate-report endpoint - PNG + PDF with analysis."""
    print("\n" + "="*60)
    print("TEST 2: /generate-report endpoint (PNG + PDF with analysis)")
    print("="*60)
    
    try:
        # Create request
        request = InvestigatorChartRequest(
            author_id=SCOPUS_AU_ID,
            affiliation_ids=["60106970", "60112687"],  # Default USB affiliations
        )
        
        # Call the endpoint handler
        response = generate_investigator_report(request)
        
        # Convert Pydantic model to dict for testing
        response_data = response.model_dump() if hasattr(response, 'model_dump') else dict(response)
        
        # Verify PNG path
        if 'file_path' not in response_data:
            print("❌ ERROR: No PNG file path in response")
            return False
        
        png_path = Path(response_data['file_path'])
        if not png_path.exists():
            print(f"❌ ERROR: PNG file not found at {png_path}")
            return False
        
        png_size = png_path.stat().st_size / 1024
        
        # Verify PDF was generated
        if 'pdf_path' not in response_data:
            print("❌ ERROR: No PDF file path in response")
            return False
        
        pdf_path = Path(response_data['pdf_path'])
        if not pdf_path.exists():
            print(f"[ERROR] PDF file not found at {pdf_path}")
            return False
        
        pdf_size = pdf_path.stat().st_size / 1024
        
        # Verify statistics are in response
        if 'statistics' not in response_data:
            print("❌ ERROR: No statistics in response")
            return False
        
        stats = response_data['statistics']
        required_stats = [
            'total_publications', 'min_year', 'max_year', 'avg_per_year',
            'peak_year', 'peak_publications', 'active_years', 'publications_by_year'
        ]
        
        missing_stats = [s for s in required_stats if s not in stats]
        if missing_stats:
            print(f"❌ ERROR: Missing statistics fields: {missing_stats}")
            return False
        
        print(f"[PASS] PNG generated successfully: {png_path.name}")
        print(f"[PASS] PNG file size: {png_size:.1f} KB")
        print(f"[PASS] PDF generated successfully: {pdf_path.name}")
        print(f"[PASS] PDF file size: {pdf_size:.1f} KB")
        print(f"[PASS] Statistics schema: VALID")
        print(f"   - Total publications: {stats.get('total_publications')}")
        print(f"   - Year range: {stats.get('min_year')}-{stats.get('max_year')}")
        print(f"   - Peak year: {stats.get('peak_year')} ({stats.get('peak_publications')} pubs)")
        
        # Summary
        print(f"\n[SUMMARY]")
        print(f"   PNG: Clean chart without analysis")
        print(f"   PDF: Contains embedded PNG + full analysis")
        print(f"   File sizes: PNG {png_size:.0f}KB + PDF {pdf_size:.0f}KB")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("\n" + "="*60)
    print("SCOPUS ENDPOINTS TEST SUITE")
    print("="*60)
    print(f"Test Scopus Author ID: {SCOPUS_AU_ID}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = []
    
    # Run tests
    results.append(("PNG only (no analysis)", test_scopus_generate()))
    results.append(("PNG + PDF with analysis", test_scopus_generate_report()))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for name, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"{status}: {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n[SUCCESS] ALL TESTS PASSED - Scopus endpoints working correctly!")
        sys.exit(0)
    else:
        print("\n[FAILURE] SOME TESTS FAILED - Check errors above")
        sys.exit(1)
