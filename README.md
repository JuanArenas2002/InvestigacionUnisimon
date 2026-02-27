# Convocatoria

API y utilidades para la gestión, extracción y reconciliación de datos científicos: publicaciones, autores y catálogos provenientes de fuentes como OpenAlex, Scopus y Web of Science.

## Estructura

```
convocatoria/
├── api/               # Aplicación FastAPI: routers, esquemas y dependencias
├── db/                # Modelos, migraciones y utilidades de base de datos
├── extractors/        # Conectores a fuentes externas (OpenAlex, Scopus, WoS)
├── reconciliation/    # Lógica de emparejamiento y deduplicación de entidades
├── scripts/           # Scripts de migración y tareas administrativas
├── OpenAlexJson/      # Datos de ejemplo
└── config.py          # Configuración central
```

## Requisitos

- Python 3.8+
- Entorno virtual recomendado

## Instalación

```bash
git clone https://github.com/tu-usuario/convocatoria.git
cd convocatoria
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## Configuración

Ajusta las variables de conexión y credenciales en `config.py`. Para entornos sensibles, se recomienda externalizar los valores mediante variables de entorno o un archivo `.env`.

## Ejecución

```bash
uvicorn api.main:app --reload
```

Para producción:

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4
```

La documentación interactiva estará disponible en `http://localhost:8000/docs` (Swagger UI) y `http://localhost:8000/redoc` (ReDoc).

## Contribuciones

Las contribuciones son bienvenidas. Por favor abre un issue antes de enviar un pull request con cambios significativos.

## Licencia

[MIT](LICENSE)