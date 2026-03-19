-- =============================================================
-- MIGRATION V5: DISCIPLINARY FIELDS & RESEARCH THRESHOLDS
-- =============================================================
-- 
-- Crea tablas para campos disciplinares y umbrales de evaluación
-- bibliométrica basados en literatura peer-reviewed:
-- - Hirsch (2005): The h-index as a research performance indicator
-- - Bornmann & Daniel (2009): Does the h-index have predictive power?
-- - Minciencias (2022): Evaluación de investigadores
-- - SCImago (2023): Journal ranking by discipline
--
-- Estados: Tabla se crea en models.py via SQLAlchemy ORM
-- Este script inserta datos iniciales

-- =============================================================
-- VALORES DE REFERENCIA (comentarios para auditoría)
-- =============================================================
-- CIENCIAS_SALUD (Medicina, Enfermería, Salud Pública)
--   h_alto=15, h_medio=8, cpp_alto=15, cpp_medio=7, pct_citados=70%, pct_pico=40%
--   concentracion_limite=2.5, ratio_hn_minimo=0.08
--
-- CIENCIAS_BASICAS (Biología, Química, Física, Oceanografía)
--   h_alto=20, h_medio=10, cpp_alto=20, cpp_medio=8, pct_citados=75%, pct_pico=35%
--   concentracion_limite=2.8, ratio_hn_minimo=0.10
--
-- INGENIERIA (Civil, Eléctrica, Sistemas, Ambiental)
--   h_alto=10, h_medio=5, cpp_alto=8, cpp_medio=4, pct_citados=60%, pct_pico=40%
--   concentracion_limite=2.0, ratio_hn_minimo=0.06
--
-- CIENCIAS_SOCIALES (Economía, Psicología, Educación, Sociología)
--   h_alto=8, h_medio=4, cpp_alto=8, cpp_medio=3, pct_citados=55%, pct_pico=40%
--   concentracion_limite=1.8, ratio_hn_minimo=0.05
--
-- ARTES_HUMANIDADES (Historia, Filosofía, Lingüística, Estudios Culturales)
--   h_alto=5, h_medio=3, cpp_alto=5, cpp_medio=2, pct_citados=40%, pct_pico=50%
--   concentracion_limite=1.5, ratio_hn_minimo=0.03
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
-- INSERT: RESEARCH THRESHOLDS - CIENCIAS_SALUD
-- =============================================================
INSERT INTO research_thresholds (field_id, metric_name, value, description)
SELECT id, metric, val, desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'h_alto', 15.0, 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'h_medio', 8.0, 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'cpp_alto', 15.0, 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'cpp_medio', 7.0, 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'pct_citados', 70.0, 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'pct_pico', 40.0, 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'concentracion_limite', 2.5, 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SALUD'), 
         'ratio_hn_minimo', 0.08, 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, metric, val, desc);


-- =============================================================
-- INSERT: RESEARCH THRESHOLDS - CIENCIAS_BASICAS
-- =============================================================
INSERT INTO research_thresholds (field_id, metric_name, value, description)
SELECT id, metric, val, desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'h_alto', 20.0, 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'h_medio', 10.0, 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'cpp_alto', 20.0, 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'cpp_medio', 8.0, 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'pct_citados', 75.0, 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'pct_pico', 35.0, 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'concentracion_limite', 2.8, 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_BASICAS'), 
         'ratio_hn_minimo', 0.10, 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, metric, val, desc);


-- =============================================================
-- INSERT: RESEARCH THRESHOLDS - INGENIERIA
-- =============================================================
INSERT INTO research_thresholds (field_id, metric_name, value, description)
SELECT id, metric, val, desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'h_alto', 10.0, 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'h_medio', 5.0, 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'cpp_alto', 8.0, 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'cpp_medio', 4.0, 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'pct_citados', 60.0, 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'pct_pico', 40.0, 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'concentracion_limite', 2.0, 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='INGENIERIA'), 
         'ratio_hn_minimo', 0.06, 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, metric, val, desc);


-- =============================================================
-- INSERT: RESEARCH THRESHOLDS - CIENCIAS_SOCIALES
-- =============================================================
INSERT INTO research_thresholds (field_id, metric_name, value, description)
SELECT id, metric, val, desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'h_alto', 8.0, 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'h_medio', 4.0, 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'cpp_alto', 8.0, 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'cpp_medio', 3.0, 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'pct_citados', 55.0, 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'pct_pico', 40.0, 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'concentracion_limite', 1.8, 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='CIENCIAS_SOCIALES'), 
         'ratio_hn_minimo', 0.05, 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, metric, val, desc);


-- =============================================================
-- INSERT: RESEARCH THRESHOLDS - ARTES_HUMANIDADES
-- =============================================================
INSERT INTO research_thresholds (field_id, metric_name, value, description)
SELECT id, metric, val, desc FROM (
    VALUES
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'h_alto', 5.0, 'Umbral alto índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'h_medio', 3.0, 'Umbral medio índice H'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'cpp_alto', 5.0, 'Umbral alto citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'cpp_medio', 2.0, 'Umbral medio citas/artículo'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'pct_citados', 40.0, 'Porcentaje mínimo artículos citados'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'pct_pico', 50.0, 'Máxima concentración en año pico (%)'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'concentracion_limite', 1.5, 'Ratio máximo CPP/mediana'),
        ((SELECT id FROM disciplinary_fields WHERE field_code='ARTES_HUMANIDADES'), 
         'ratio_hn_minimo', 0.03, 'Ratio mínimo H/N (madurez productividad)')
) AS t(id, metric, val, desc);
