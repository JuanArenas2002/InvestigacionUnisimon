# 🎓 Google Scholar Extractor - Guía de Pruebas

## Descripción

El **Google Scholar Extractor** permite extraer publicaciones científicas desde perfiles públicos de Google Scholar sin necesidad de API oficial. Utiliza la librería `scholarly` para web scraping.

---

## 📋 Requisitos Previos

- Python 3.8+
- Virtual environment activado
- Conexión a internet

---

## 🚀 Instalación

### 1. Instalar dependencia
```bash
pip install scholarly
```

### 2. Verificar instalación
```powershell
python -c "import scholarly; print('scholarly OK')"
```

---

## 🔍 Obtener Scholar IDs

Los Scholar IDs son identificadores únicos de perfiles públicos en Google Scholar.

### Cómo obtener tu Scholar ID:
1. Ve a [Google Scholar](https://scholar.google.com/)
2. Busca un autor o tu propio perfil
3. Abre el perfil público
4. El ID está en la URL: `https://scholar.google.com/citations?user=**Ozm565YAAAAJ**&hl=es`
   - El Scholar ID es: `Ozm565YAAAAJ`

### Ejemplos de Scholar IDs válidos:
- `Ozm565YAAAAJ` (Juan Manuel Ramírez)
- `_xxTOIEAAAAJ`
- `jc0B6ZUAAAAJ`

---

## 💻 Uso Básico

### Script Simple de Prueba

Crea un archivo `test_google_scholar.py` en la raíz del proyecto:

```python
#!/usr/bin/env python
"""
Script de prueba para Google Scholar Extractor
"""
import logging
from extractors.google_scholar.extractor import GoogleScholarExtractor

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Inicializar extractor
extractor = GoogleScholarExtractor()

# Ejemplo 1: Extractor simple
print("=" * 60)
print("PRUEBA 1: Extracción básica")
print("=" * 60)
try:
    records = extractor.extract(
        scholar_ids=["Ozm565YAAAAJ"],  # Reemplaza con un Scholar ID válido
        year_from=2020,
        year_to=2025,
        max_results=10
    )
    
    print(f"\n✅ Se obtuvieron {len(records)} registros\n")
    
    # Mostrar primeros registros
    for i, record in enumerate(records[:3], 1):
        print(f"[{i}] {record.title}")
        print(f"    Año: {record.publication_year}")
        print(f"    Autores: {', '.join(record.authors[:2])}")
        print(f"    Citas: {record.citation_count}")
        print()
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

# Ejemplo 2: Múltiples perfiles
print("\n" + "=" * 60)
print("PRUEBA 2: Múltiples perfiles")
print("=" * 60)
try:
    records = extractor.extract(
        scholar_ids=[
            "Ozm565YAAAAJ",     # Reemplaza
            "_xxTOIEAAAAJ"      # Reemplaza
        ],
        year_from=2022,
        max_results=5
    )
    
    print(f"\n✅ Se obtuvieron {len(records)} registros de múltiples perfiles\n")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
```

### Ejecutar el script
```powershell
# Desde la raíz del proyecto
python test_google_scholar.py
```

---

## 📊 Parámetros del Extractor

```python
extractor.extract(
    scholar_ids: List[str],      # [REQUERIDO] IDs de Google Scholar
    year_from: int = None,       # Año inicial (inclusive)
    year_to: int = None,         # Año final (inclusive)
    max_results: int = None,     # Límite de registros
)
```

### Ejemplos de Parámetros

```python
# Solo publicaciones recientes
records = extractor.extract(
    scholar_ids=["Ozm565YAAAAJ"],
    year_from=2023,
    max_results=20
)

# Alle publicaciones sin límite
records = extractor.extract(
    scholar_ids=["Ozm565YAAAAJ"]
)

# Rango específico
records = extractor.extract(
    scholar_ids=["Ozm565YAAAAJ"],
    year_from=2020,
    year_to=2022,
    max_results=50
)
```

---

## 📦 Estructura de StandardRecord

Cada registro contiene:

```python
{
    "source_name": "google_scholar",
    "source_id": str,                    # ID único en Google Scholar
    "doi": str,                          # DOI si está disponible
    "title": str,                        # Título de la publicación
    "publication_year": int,             # Año de publicación
    "publication_type": str,             # Tipo (article, conference, etc)
    "source_journal": str,               # Nombre del journal/conferencia
    "issn": str,                         # ISSN si está disponible
    "authors": List[str],                # Lista de autores
    "institutional_authors": List[str],  # Afiliaciones institucionales
    "citation_count": int,               # Número de citas
    "url": str,                          # URL del artículo
    "raw_data": dict,                    # Datos crudos de scholarly
}
```

---

## ⚠️ Manejo de Errores

### Errores Comunes

| Error | Causa | Solución |
|-------|-------|----------|
| `scholarly no está instalado` | Dependencia faltante | `pip install scholarly` |
| `Scholar ID no encontrado` | ID inválido o perfil privado | Verifica la URL del perfil |
| `Rate limit exceeded` | Demasiadas solicitudes | Espera 1-2 horas o usa proxy |
| `Connection timeout` | Problema de red | Verifica conexión a internet |

### Script con Manejo de Errores

```python
from extractors.google_scholar.extractor import GoogleScholarExtractor
from extractors.google_scholar._exceptions import GoogleScholarError

extractor = GoogleScholarExtractor()

try:
    records = extractor.extract(
        scholar_ids=["Ozm565YAAAAJ"],
        max_results=10
    )
    print(f"✅ {len(records)} registros extraídos")
    
except GoogleScholarError as e:
    print(f"❌ Error de Google Scholar: {e}")
    
except ValueError as e:
    print(f"❌ Error de validación: {e}")
    
except Exception as e:
    print(f"❌ Error inesperado: {e}")
    import traceback
    traceback.print_exc()
```

---

## 🔐 Proxy (Para Rate Limiting)

Si Google Scholar bloquea tus solicitudes:

```python
from scholarly import scholarly, ProxyGenerator

# Configurar proxy gratuito
pg = ProxyGenerator()
pg.FreeProxies()
scholarly.use_proxy(pg)

# Ahora extraer
extractor = GoogleScholarExtractor()
records = extractor.extract(scholar_ids=["Ozm565YAAAAJ"])
```

---

## 📈 Prueba Completa Paso a Paso

### 1️⃣ **Fase 1: Validación**
```bash
# Verificar instalación
python -c "from scholarly import scholarly; print('OK')"

# Verificar extractor
python -c "from extractors.google_scholar.extractor import GoogleScholarExtractor; print('OK')"
```

### 2️⃣ **Fase 2: Extracción Simple**
```powershell
python -m pytest tests/extractors/test_google_scholar.py -v
# O ejecutar script manual
python test_google_scholar.py
```

### 3️⃣ **Fase 3: Validación de Datos**
```python
records = extractor.extract(scholar_ids=["Ozm565YAAAAJ"], max_results=5)

# Validar estructura
for record in records:
    assert record.title, "Título vacío"
    assert record.publication_year, "Año vacío"
    assert record.authors, "Sin autores"
    print(f"✅ {record.title}")
```

### 4️⃣ **Fase 4: Integración con BD**
```python
from db.models import Publication

for record in records:
    pub = Publication.create_from_standard_record(record)
    print(f"Guardado: {pub.title}")
```

---

## 🧪 Prueba Interactiva en Python REPL

```powershell
# Entrar al Python REPL
python

# Dentro de Python:
>>> from extractors.google_scholar.extractor import GoogleScholarExtractor
>>> ex = GoogleScholarExtractor()
>>> recs = ex.extract(scholar_ids=["Ozm565YAAAAJ"], max_results=3)
>>> len(recs)
3
>>> recs[0].title
'Publication Title...'
>>> recs[0].citation_count
42
```

---

## 📝 Limitaciones Conocidas

1. **Rate Limiting**: Google Scholar aplica límites de solicitudes
2. **Sin API Oficial**: Usa web scraping (puede cambiar)
3. **Datos Incompletos**: Algunos campos pueden estar vacíos
4. **Perfiles Privados**: No se pueden extraer perfiles no públicos
5. **Velocidad**: Más lento que APIs oficiales

---

## 🔗 Referencias

- [Google Scholar](https://scholar.google.com/)
- [scholarly - GitHub](https://github.com/scholarly-python-package/scholarly)
- [scholarly - Documentación](https://github.com/scholarly-python-package/scholarly/blob/main/README.md)

---

## ✅ Checklist de Prueba

- [ ] Instalar `scholarly`
- [ ] Verificar Scholar IDs válidos
- [ ] Ejecutar extracción simple (1 perfil, 5 registros)
- [ ] Ejecutar extracción múltiple (2+ perfiles)
- [ ] Validar estructura de registros
- [ ] Probar manejo de errores
- [ ] Probar con filtros de año
- [ ] Integrar con BD (si aplica)
- [ ] Documentar Scholar IDs de prueba
- [ ] Validar performance (`time python test_google_scholar.py`)

---

## 📞 Soporte

Si encuentras problemas:

1. Verifica Scholar ID en Google Scholar manualmente
2. Revisa los logs: `logging.basicConfig(level=logging.DEBUG)`
3. Prueba con otro Scholar ID
4. Intenta con proxy si hay rate limiting
5. Consulta los archivos en `extractors/google_scholar/_exceptions.py`

