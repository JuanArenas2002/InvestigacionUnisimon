#!/usr/bin/env python
"""
Prueba rápida del Scholar ID: V94aovUAAAAJ
"""

def test_scholar_id():
    try:
        from scholarly import scholarly
        
        print("\n" + "="*70)
        print("PRUEBA DE SCHOLAR ID: V94aovUAAAAJ")
        print("="*70 + "\n")
        
        # Obtener perfil
        print("🔍 Buscando perfil en Google Scholar...")
        author = scholarly.search_author("V94aovUAAAAJ")
        profile = scholarly.fill(author)
        
        print(f"\n✅ Perfil encontrado:")
        print(f"   Nombre: {profile['name']}")
        print(f"   Afiliación: {profile.get('affiliation', 'N/A')}")
        print(f"   Email: {profile.get('email', 'N/A')}")
        print(f"   h-index: {profile.get('hindex', 'N/A')}")
        print(f"   i10-index: {profile.get('i10index', 'N/A')}")
        print(f"   Citas: {profile.get('citationcount', 'N/A')}")
        
        # Contar publicaciones
        publications = list(scholarly.publications_from_citeseer(profile['scholar_id']))
        print(f"\n   Total de publicaciones: {len(publications)}")
        
        if publications:
            print(f"\n   Últimas 3 publicaciones:")
            for i, pub in enumerate(publications[:3], 1):
                print(f"   [{i}] {pub.get('title', 'Sin título')[:60]}...")
        
        print("\n✅ Scholar ID válido y funcionando correctamente")
        return True
        
    except StopIteration:
        print("\n❌ Scholar ID no encontrado o perfil es privado")
        print("   Verifica: https://scholar.google.com/citations?user=V94aovUAAAAJ")
        return False
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_scholar_id()
    exit(0 if success else 1)
