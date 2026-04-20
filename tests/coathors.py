from sqlalchemy import create_engine, text, func
from db.models import Author, PublicationAuthor

# Conectar
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
db = Session()

try:
    # Encontrar pares de autores que compartan publicaciones
    result = db.execute(text("""
        SELECT 
            pa1.author_id as author_1,
            pa2.author_id as author_2,
            COUNT(DISTINCT pa1.publication_id) as shared_pubs
        FROM publication_authors pa1
        JOIN publication_authors pa2 ON pa1.publication_id = pa2.publication_id
        WHERE pa1.author_id < pa2.author_id
        GROUP BY pa1.author_id, pa2.author_id
        ORDER BY shared_pubs DESC
        LIMIT 10
    """)).fetchall()
    
    print("🔍 Pares de autores con publicaciones compartidas:")
    print("=" * 60)
    for row in result:
        print(f"Autor {row[0]} + Autor {row[1]} = {row[2]} publicaciones compartidas")
        
    if result:
        a1, a2, _ = result[0]
        print(f"\n💡 Prueba el endpoint con: ?author_ids={a1},{a2}")
        
finally:
    db.close()