-- =============================================================
-- MIGRATION V5 UPDATED: DISCIPLINARY FIELDS & FIELD PARAMETERS
-- =============================================================
-- 
-- Reemplaza research_thresholds con field_parameters (más flexible)
-- Almacena TODOS los parámetros en BD:
-- - Umbrales de evaluación (h_alto, h_medio, etc.)
-- - Pesos para cálculos (que no existen aún pero están preparados)
-- - Coeficientes de ajuste
-- - Configuraciones globales
--

-- =============================================================
-- INSERT: DISCIPLINARY FIELDS
-- =============================================================
INSERT INTO disciplinary_fields (field_code, field_name_es, field_name_en, description)
VALUES
    ('CIENCIAS_SALUD', 'Ciencias de la Salud', 'Health Sciences', 
     'Medicina, Enfermería, Salud Pública, Farmacología, Odontología'),
     
    ('CIENCIAS_BASICAS', 'Ciencias Básicas', 'Basic Sciences',
     'Biología, Química, Física, Oceanografía, Ciencias de la Tierra'),
     
    ('INGENIERIA', 'Ingeniería', 'Engineering',
     'Ingeniería Civil, Eléctrica, Sistemas, Ambiental, de Telecomunicaciones'),
     
    ('CIENCIAS_SOCIALES', 'Ciencias Sociales', 'Social Sciences',
     'Economía, Psicología, Educación, Sociología, Ciencia Política'),
     
    ('ARTES_HUMANIDADES', 'Artes y Humanidades', 'Arts and Humanities',
     'Historia, Filosofía, Lingüística, Literatura, Estudios Culturales');


-- =============================================================
-- INSERT: FIELD PARAMETERS - CIENCIAS_SALUD
-- =============================================================
INSERT INTO field_parameters (field_id, parameter_name, parameter_type, value, description)
SELECT id, param_name, param_type, param_value, param_desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'h_alto', 'float', '15.0', 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'h_medio', 'float', '8.0', 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'cpp_alto', 'float', '15.0', 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'cpp_medio', 'float', '7.0', 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'pct_citados', 'float', '70.0', 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'pct_pico', 'float', '40.0', 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'concentracion_limite', 'float', '2.5', 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'ratio_hn_minimo', 'float', '0.08', 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, param_name, param_type, param_value, param_desc);


-- =============================================================
-- INSERT: FIELD PARAMETERS - CIENCIAS_BASICAS
-- =============================================================
INSERT INTO field_parameters (field_id, parameter_name, parameter_type, value, description)
SELECT id, param_name, param_type, param_value, param_desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'h_alto', 'float', '20.0', 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'h_medio', 'float', '10.0', 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'cpp_alto', 'float', '20.0', 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'cpp_medio', 'float', '8.0', 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'pct_citados', 'float', '75.0', 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'pct_pico', 'float', '35.0', 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'concentracion_limite', 'float', '2.8', 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'ratio_hn_minimo', 'float', '0.10', 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, param_name, param_type, param_value, param_desc);


-- =============================================================
-- INSERT: FIELD PARAMETERS - INGENIERIA
-- =============================================================
INSERT INTO field_parameters (field_id, parameter_name, parameter_type, value, description)
SELECT id, param_name, param_type, param_value, param_desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'h_alto', 'float', '10.0', 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'h_medio', 'float', '5.0', 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'cpp_alto', 'float', '8.0', 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'cpp_medio', 'float', '4.0', 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'pct_citados', 'float', '60.0', 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'pct_pico', 'float', '40.0', 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'concentracion_limite', 'float', '2.0', 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'ratio_hn_minimo', 'float', '0.06', 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, param_name, param_type, param_value, param_desc);


-- =============================================================
-- INSERT: FIELD PARAMETERS - CIENCIAS_SOCIALES
-- =============================================================
INSERT INTO field_parameters (field_id, parameter_name, parameter_type, value, description)
SELECT id, param_name, param_type, param_value, param_desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'h_alto', 'float', '8.0', 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'h_medio', 'float', '4.0', 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'cpp_alto', 'float', '8.0', 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'cpp_medio', 'float', '3.0', 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'pct_citados', 'float', '55.0', 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'pct_pico', 'float', '40.0', 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'concentracion_limite', 'float', '1.8', 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'ratio_hn_minimo', 'float', '0.05', 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, param_name, param_type, param_value, param_desc);


-- =============================================================
-- INSERT: FIELD PARAMETERS - ARTES_HUMANIDADES
-- =============================================================
INSERT INTO field_parameters (field_id, parameter_name, parameter_type, value, description)
SELECT id, param_name, param_type, param_value, param_desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'h_alto', 'float', '5.0', 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'h_medio', 'float', '3.0', 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'cpp_alto', 'float', '5.0', 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'cpp_medio', 'float', '2.0', 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'pct_citados', 'float', '40.0', 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'pct_pico', 'float', '50.0', 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'concentracion_limite', 'float', '1.5', 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'ratio_hn_minimo', 'float', '0.03', 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, param_name, param_type, param_value, param_desc);
