import sys
sys.path.insert(0, 'c:/Users/juan.arenas1/Desktop/CONVOCATORIA')

print("✓ Testing imports...")
try:
    from api.services.scopus_h_index_service import ScopusHIndexService, get_author_h_index
    print("✓ ScopusHIndexService imported successfully")
except Exception as e:
    print(f"✗ Error importing ScopusHIndexService: {e}")
    import traceback
    traceback.print_exc()

try:
    from api.exporters.excel.scopus_h_index import generate_h_index_excel
    print("✓ generate_h_index_excel imported successfully")
except Exception as e:
    print(f"✗ Error importing generate_h_index_excel: {e}")
    import traceback
    traceback.print_exc()

print("\n✓ Testing get_author_h_index function...")
try:
    result = get_author_h_index("57193767797")
    print(f"✓ Result: {result}")
except Exception as e:
    print(f"✗ Error calling get_author_h_index: {e}")
    import traceback
    traceback.print_exc()
