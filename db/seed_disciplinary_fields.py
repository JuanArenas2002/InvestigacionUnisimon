"""
db/seed_disciplinary_fields.py
=============================
Script para insertar campos disciplinares y parámetros en la base de datos.

FILOSOFÍA: TODOS los parámetros están en BD, no en código.
- Umbrales de evaluación
- Pesos para cálculos
- Coeficientes
- Configuraciones globales

Ejecutar con: python -m db.seed_disciplinary_fields
"""

from sqlalchemy import text
from db.session import get_session
from db.models import DisciplinaryField, FieldParameter

def seed_disciplinary_fields():
    """Inserta campos disciplinares y parámetros iniciales."""
    session = get_session()
    
    try:
        # Verificar si ya existen
        existing = session.query(DisciplinaryField).count()
        if existing > 0:
            print(f"✓ Ya existen {existing} campos disciplinares en BD")
            return
        
        print("Insertando campos disciplinares y parámetros...")
        
        # Definir campos y parámetros
        # IMPORTANTÍSIMO: Todos los parámetros vienen de acá
        campos_parametros = {
            "CIENCIAS_SALUD": {
                "name_es": "Ciencias de la Salud",
                "name_en": "Health Sciences",
                "description": "Medicina, Enfermería, Salud Pública, Farmacología, Odontología",
                "parameters": {
                    "h_alto": ("float", "15.0", "Umbral alto índice H"),
                    "h_medio": ("float", "8.0", "Umbral medio índice H"),
                    "cpp_alto": ("float", "15.0", "Umbral alto citas/artículo"),
                    "cpp_medio": ("float", "7.0", "Umbral medio citas/artículo"),
                    "pct_citados": ("float", "70.0", "Porcentaje mínimo artículos citados"),
                    "pct_pico": ("float", "40.0", "Máxima concentración en año pico (%)"),
                    "concentracion_limite": ("float", "2.5", "Ratio máximo CPP/mediana"),
                    "ratio_hn_minimo": ("float", "0.08", "Ratio mínimo H/N (madurez productividad)"),
                }
            },
            "CIENCIAS_BASICAS": {
                "name_es": "Ciencias Básicas",
                "name_en": "Basic Sciences",
                "description": "Biología, Química, Física, Oceanografía, Ciencias de la Tierra",
                "parameters": {
                    "h_alto": ("float", "20.0", "Umbral alto índice H"),
                    "h_medio": ("float", "10.0", "Umbral medio índice H"),
                    "cpp_alto": ("float", "20.0", "Umbral alto citas/artículo"),
                    "cpp_medio": ("float", "8.0", "Umbral medio citas/artículo"),
                    "pct_citados": ("float", "75.0", "Porcentaje mínimo artículos citados"),
                    "pct_pico": ("float", "35.0", "Máxima concentración en año pico (%)"),
                    "concentracion_limite": ("float", "2.8", "Ratio máximo CPP/mediana"),
                    "ratio_hn_minimo": ("float", "0.10", "Ratio mínimo H/N (madurez productividad)"),
                }
            },
            "INGENIERIA": {
                "name_es": "Ingeniería",
                "name_en": "Engineering",
                "description": "Ingeniería Civil, Eléctrica, Sistemas, Ambiental, de Telecomunicaciones",
                "parameters": {
                    "h_alto": ("float", "10.0", "Umbral alto índice H"),
                    "h_medio": ("float", "5.0", "Umbral medio índice H"),
                    "cpp_alto": ("float", "8.0", "Umbral alto citas/artículo"),
                    "cpp_medio": ("float", "4.0", "Umbral medio citas/artículo"),
                    "pct_citados": ("float", "60.0", "Porcentaje mínimo artículos citados"),
                    "pct_pico": ("float", "40.0", "Máxima concentración en año pico (%)"),
                    "concentracion_limite": ("float", "2.0", "Ratio máximo CPP/mediana"),
                    "ratio_hn_minimo": ("float", "0.06", "Ratio mínimo H/N (madurez productividad)"),
                }
            },
            "CIENCIAS_SOCIALES": {
                "name_es": "Ciencias Sociales",
                "name_en": "Social Sciences",
                "description": "Economía, Psicología, Educación, Sociología, Ciencia Política",
                "parameters": {
                    "h_alto": ("float", "8.0", "Umbral alto índice H"),
                    "h_medio": ("float", "4.0", "Umbral medio índice H"),
                    "cpp_alto": ("float", "8.0", "Umbral alto citas/artículo"),
                    "cpp_medio": ("float", "3.0", "Umbral medio citas/artículo"),
                    "pct_citados": ("float", "55.0", "Porcentaje mínimo artículos citados"),
                    "pct_pico": ("float", "40.0", "Máxima concentración en año pico (%)"),
                    "concentracion_limite": ("float", "1.8", "Ratio máximo CPP/mediana"),
                    "ratio_hn_minimo": ("float", "0.05", "Ratio mínimo H/N (madurez productividad)"),
                }
            },
            "ARTES_HUMANIDADES": {
                "name_es": "Artes y Humanidades",
                "name_en": "Arts and Humanities",
                "description": "Historia, Filosofía, Lingüística, Literatura, Estudios Culturales",
                "parameters": {
                    "h_alto": ("float", "5.0", "Umbral alto índice H"),
                    "h_medio": ("float", "3.0", "Umbral medio índice H"),
                    "cpp_alto": ("float", "5.0", "Umbral alto citas/artículo"),
                    "cpp_medio": ("float", "2.0", "Umbral medio citas/artículo"),
                    "pct_citados": ("float", "40.0", "Porcentaje mínimo artículos citados"),
                    "pct_pico": ("float", "50.0", "Máxima concentración en año pico (%)"),
                    "concentracion_limite": ("float", "1.5", "Ratio máximo CPP/mediana"),
                    "ratio_hn_minimo": ("float", "0.03", "Ratio mínimo H/N (madurez productividad)"),
                }
            }
        }
        
        # Insertar campos y parámetros
        total_params = 0
        for code, data in campos_parametros.items():
            # Crear campo disciplinar
            campo = DisciplinaryField(
                field_code=code,
                field_name_es=data["name_es"],
                field_name_en=data["name_en"],
                description=data["description"]
            )
            session.add(campo)
            session.flush()  # Para obtener el ID antes de commit
            
            # Crear parámetros para este campo
            for param_name, (param_type, param_value, param_desc) in data["parameters"].items():
                param = FieldParameter(
                    field_id=campo.id,
                    parameter_name=param_name,
                    parameter_type=param_type,
                    value=param_value,
                    description=param_desc
                )
                session.add(param)
                total_params += 1
            
            print(f"  ✓ {code}: {len(data['parameters'])} parámetros")
        
        session.commit()
        print(f"\n✓ Seed completado: {len(campos_parametros)} campos con {total_params} parámetros en BD")
        
    except Exception as e:
        session.rollback()
        print(f"✗ Error durante seed: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    seed_disciplinary_fields()
