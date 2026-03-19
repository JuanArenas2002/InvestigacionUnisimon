"""
api/services/analysis.py
=======================
Módulo de análisis bibliométrico automático.
Genera hallazgos positivos y negativos a partir de indicadores.

Utiliza umbrales field-specific almacenados en la base de datos.
Basado en literatura peer-reviewed:
- Hirsch (2005): The h-index as a research performance indicator
- Bornmann & Daniel (2009): Does the h-index have predictive power?
- Minciencias (2022): Evaluación de investigadores
- SCImago (2023): Journal ranking by discipline
"""

from enum import Enum
from matplotlib.patches import FancyBboxPatch


class CampoDisciplinar(str, Enum):
    """Enumeración de campos disciplinares soportados."""
    CIENCIAS_SALUD = "CIENCIAS_SALUD"
    CIENCIAS_BASICAS = "CIENCIAS_BASICAS"
    INGENIERIA = "INGENIERIA"
    CIENCIAS_SOCIALES = "CIENCIAS_SOCIALES"
    ARTES_HUMANIDADES = "ARTES_HUMANIDADES"


def _get_umbrales_default(campo: CampoDisciplinar = CampoDisciplinar.CIENCIAS_SALUD) -> dict:
    """
    DEPRECADO: Se mantiene solo para emergencias extremas.
    
    En una operación normal, TODOS los umbrales vienen de BD.
    Este fallback solo existe si la BD falla completamente.
    """
    # Log de advertencia
    import logging
    logging.warning(
        f"⚠️ FALLBACK A VALORES HARDCODEADOS para {campo.value}. "
        f"La BD debería proporcionar parámetros. Verificar connection."
    )
    
    # ESTOS VALORES NO DEBEN USARSE EN PRODUCCIÓN
    # Son solo para desarrollo/fallback
    defaults = {
        CampoDisciplinar.CIENCIAS_SALUD: {
            "h_alto": 15.0, "h_medio": 8.0,
            "cpp_alto": 15.0, "cpp_medio": 7.0,
            "pct_citados": 70.0, "pct_pico": 40.0,
            "concentracion_limite": 2.5, "ratio_hn_minimo": 0.08
        },
        CampoDisciplinar.CIENCIAS_BASICAS: {
            "h_alto": 20.0, "h_medio": 10.0,
            "cpp_alto": 20.0, "cpp_medio": 8.0,
            "pct_citados": 75.0, "pct_pico": 35.0,
            "concentracion_limite": 2.8, "ratio_hn_minimo": 0.10
        },
        CampoDisciplinar.INGENIERIA: {
            "h_alto": 10.0, "h_medio": 5.0,
            "cpp_alto": 8.0, "cpp_medio": 4.0,
            "pct_citados": 60.0, "pct_pico": 40.0,
            "concentracion_limite": 2.0, "ratio_hn_minimo": 0.06
        },
        CampoDisciplinar.CIENCIAS_SOCIALES: {
            "h_alto": 8.0, "h_medio": 4.0,
            "cpp_alto": 8.0, "cpp_medio": 3.0,
            "pct_citados": 55.0, "pct_pico": 40.0,
            "concentracion_limite": 1.8, "ratio_hn_minimo": 0.05
        },
        CampoDisciplinar.ARTES_HUMANIDADES: {
            "h_alto": 5.0, "h_medio": 3.0,
            "cpp_alto": 5.0, "cpp_medio": 2.0,
            "pct_citados": 40.0, "pct_pico": 50.0,
            "concentracion_limite": 1.5, "ratio_hn_minimo": 0.03
        },
    }
    return defaults.get(campo, defaults[CampoDisciplinar.CIENCIAS_SALUD])


def generar_hallazgos(
    total_arts: int, total_citas: int, h_index: int, cpp: float,
    mediana: float, pct_citados: float, años: list, pubs: list, cites: list,
    año_pico: int, año_max_pub: int,
    campo: CampoDisciplinar = CampoDisciplinar.CIENCIAS_SALUD,
    db_session=None
) -> tuple:
    """
    Genera hallazgos positivos y negativos basados en 8 análisis bibliométricos profesionales.
    
    FILOSOFÍA: Todos los parámetros vienen DESDE LA BASE DE DATOS.
    No hay valores hardcodeados en el código.
    
    Análisis incluidos:
    1. Índice H — trayectoria y consolidación
    2. CPP absoluto — impacto promedio por artículo
    3. Concentración de impacto — distribución vs concentración
    4. Visibilidad general — % artículos citados
    5. Tendencia de producción — crecimiento/decrecimiento en últimos años
    6. Dependencia del año pico — riesgo de concentración temporal
    7. Eficiencia productiva — ratio H/N (núcleo vs total)
    8. Madurez de producción reciente — notas aclaratorias
    
    Referencias (umbral base):
    - Hirsch (2005): An index to quantify an individual's scientific research output
    - Bornmann & Daniel (2009): Does the h index have predictive power?
    - Minciencias (2022): Modelo de medición de investigadores
    - SCImago (2023): Journal ranking by discipline
    
    Parámetros
    ----------
    campo : CampoDisciplinar
        Campo disciplinar para obtener parámetros específicos
    db_session : SQLAlchemy Session
        SI se proporciona: Obtiene parámetros desde BD (RECOMENDADO)
        SI no: Usa fallback (SOLO para desarrollo/emergencias)
    
    Retorna: (positivos, negativos, notas) como tupla de listas de strings
    """
    # PASO 1: OBTENER PARÁMETROS DESDE BD
    # ═════════════════════════════════════════════════════════════════════════
    umbrales = None
    
    if db_session is not None:
        try:
            from db.models import get_thresholds_by_field
            umbrales = get_thresholds_by_field(db_session, campo.value)
            print(f"✓ Parámetros cargados desde BD para {campo.value}")
        except Exception as e:
            print(f"✗ Error cargando parámetros de BD: {e}")
            umbrales = None
    
    # PASO 2: FALLBACK A VALORES HARDCODEADOS (solo si BD falla)
    # ═════════════════════════════════════════════════════════════════════════
    if umbrales is None:
        print(f"⚠️  FALLBACK: Usando parámetros hardcodeados para {campo.value}")
        umbrales = _get_umbrales_default(campo)
    
    # PASO 3: VALIDAR QUE TODOS LOS PARÁMETROS EXISTAN
    # ═════════════════════════════════════════════════════════════════════════
    required_params = [
        "h_alto", "h_medio", "cpp_alto", "cpp_medio",
        "pct_citados", "pct_pico"
    ]
    for param in required_params:
        if param not in umbrales:
            raise ValueError(
                f"Parámetro {param} no encontrado para {campo.value}. "
                f"Params disponibles: {list(umbrales.keys())}"
            )
    
    U = umbrales
    positivos = []
    negativos = []
    notas = []

    # ── 1. Índice H — trayectoria y consistencia ──────────────────────────────
    if h_index >= U["h_alto"]:
        positivos.append(
            f"Índice H = {h_index} (campo {campo.value.replace('_',' ')}): supera el umbral de trayectoria "
            f"consolidada (≥{U['h_alto']}) para su disciplina. Refleja un núcleo amplio y consistente "
            f"de publicaciones con impacto sostenido."
        )
    elif h_index >= U["h_medio"]:
        positivos.append(
            f"Índice H = {h_index}: dentro del rango esperado para investigadores en etapa media "
            f"en {campo.value.replace('_',' ')} (umbral medio: ≥{U['h_medio']}). "
            f"Trayectoria en desarrollo con potencial de consolidación."
        )
    else:
        negativos.append(
            f"Índice H = {h_index}: por debajo del umbral medio para {campo.value.replace('_',' ')} "
            f"(≥{U['h_medio']}). El núcleo de impacto es aún pequeño. Se recomienda fortalecer "
            f"publicación en revistas indexadas Q1–Q2 del área."
        )

    # ── 2. CPP — impacto promedio por artículo ────────────────────────────────
    if cpp >= U["cpp_alto"]:
        positivos.append(
            f"CPP = {cpp:.1f}: supera el umbral de alta visibilidad para el campo "
            f"(≥{U['cpp_alto']} citas/artículo). Cada publicación genera en promedio "
            f"un impacto superior al estándar internacional del área."
        )
    elif cpp >= U["cpp_medio"]:
        positivos.append(
            f"CPP = {cpp:.1f}: dentro del rango aceptable para {campo.value.replace('_',' ')} "
            f"(umbral mínimo: {U['cpp_medio']} citas/artículo). Impacto promedio en línea "
            f"con el comportamiento típico del campo."
        )
    else:
        negativos.append(
            f"CPP = {cpp:.1f}: por debajo del promedio esperado para el campo "
            f"(umbral mínimo: {U['cpp_medio']} citas/artículo). Se recomienda revisar "
            f"estrategia de publicación: revistas con mayor alcance, coautorías "
            f"internacionales y difusión en redes académicas."
        )

    # ── 3. Concentración del impacto — CPP vs mediana ────────────────────────
    ratio_cpp_med = round(cpp / mediana, 1) if mediana > 0 else 0
    if ratio_cpp_med > 3:
        negativos.append(
            f"Concentración de impacto: CPP ({cpp:.1f}) es {ratio_cpp_med}× la mediana ({mediana:.1f}). "
            f"El impacto está concentrado en pocos artículos clave. "
            f"Una estrategia más diversificada reduciría la dependencia de trabajos específicos."
        )
    else:
        positivos.append(
            f"Impacto distribuido: la relación CPP/mediana es {ratio_cpp_med}× (umbral crítico: >3×). "
            f"El impacto está repartido de manera homogénea entre la producción — "
            f"indicador de solidez y no dependencia de artículos extraordinarios."
        )

    # ── 4. % artículos citados — visibilidad general ──────────────────────────
    if pct_citados >= U["pct_citados"]:
        positivos.append(
            f"{pct_citados}% de los artículos han recibido al menos una cita "
            f"(umbral saludable para el campo: ≥{U['pct_citados']}%). "
            f"Alta visibilidad general: la producción está siendo leída y referenciada "
            f"de manera amplia por la comunidad científica."
        )
    elif pct_citados >= U["pct_citados"] - 15:
        negativos.append(
            f"{pct_citados}% de artículos citados: ligeramente por debajo del umbral "
            f"saludable para el campo ({U['pct_citados']}%). Se recomienda mejorar "
            f"la difusión: publicación en acceso abierto, depósito en repositorios "
            f"institucionales y participación en congresos internacionales."
        )
    else:
        negativos.append(
            f"Solo el {pct_citados}% de los artículos ha recibido citas "
            f"(umbral mínimo: {U['pct_citados']}%). Alta proporción de producción sin "
            f"visibilidad. Revisar estrategia de selección de revistas y difusión."
        )

    # ── 5. Tendencia de producción — primeros vs últimos 3 años ──────────────
    n = len(años)
    if n >= 6:
        pubs_ini  = sum(pubs[:3])
        pubs_fin  = sum(pubs[-3:])
        años_ini  = f"{años[0]}–{años[2]}"
        años_fin  = f"{años[-3]}–{años[-1]}"
        tasa      = round((pubs_fin - pubs_ini) / pubs_ini * 100) if pubs_ini else 0

        if pubs_fin > pubs_ini * 1.5:
            positivos.append(
                f"Tendencia de producción creciente: +{tasa}% entre {años_ini} "
                f"({pubs_ini} arts.) y {años_fin} ({pubs_fin} arts.). "
                f"El investigador se encuentra en plena etapa de madurez productiva."
            )
        elif pubs_fin >= pubs_ini:
            positivos.append(
                f"Producción estable o en leve crecimiento entre {años_ini} y {años_fin} "
                f"({pubs_ini} → {pubs_fin} artículos). Actividad investigativa sostenida."
            )
        else:
            negativos.append(
                f"Tendencia de producción decreciente: de {pubs_ini} artículos en "
                f"{años_ini} a {pubs_fin} en {años_fin} (−{abs(tasa)}%). "
                f"Puede indicar transición de rol, reducción de financiación "
                f"o reorientación de líneas de investigación."
            )

    # ── 6. Dependencia del año pico ───────────────────────────────────────────
    if año_pico in años and total_citas > 0:
        idx_pico  = años.index(año_pico)
        pct_pico  = round(cites[idx_pico] / total_citas * 100, 1) if total_citas else 0

        if pct_pico > U["pct_pico"]:
            negativos.append(
                f"Dependencia del año pico: el {pct_pico}% de las citas totales "
                f"proviene de artículos publicados en {año_pico} "
                f"(umbral crítico: >{U['pct_pico']}%). "
                f"El impacto acumulado es vulnerable a la obsolescencia "
                f"de esos trabajos específicos."
            )
        else:
            positivos.append(
                f"Citas bien distribuidas en el tiempo: el año de mayor impacto "
                f"({año_pico}) concentra el {pct_pico}% del total "
                f"(umbral crítico: >{U['pct_pico']}%). "
                f"No hay dependencia excesiva de un período específico."
            )

    # ── 7. Producción reciente — nota aclaratoria ─────────────────────────────
    if len(años) > 0:
        ultimo_año = años[-1]
        if ultimo_año in años:
            idx_ultimo = años.index(ultimo_año)
            cpp_ultimo = round(cites[idx_ultimo] / pubs[idx_ultimo], 1) if pubs[idx_ultimo] else 0

            if cpp_ultimo < U["cpp_medio"]:
                notas.append(
                    f"Nota: Artículos de {ultimo_año} con CPP = {cpp_ultimo}: normal para producción "
                    f"reciente — requieren 18–36 meses para acumular citas. "
                    f"Este valor no debe interpretarse como deterioro del impacto."
                )

    # ── 8. Ratio productividad–impacto ────────────────────────────────────────
    # H/N: qué fracción de la producción sostiene el índice H
    ratio_hn = round(h_index / total_arts * 100, 1) if total_arts else 0
    if ratio_hn >= 25:
        positivos.append(
            f"Eficiencia productiva: el {ratio_hn}% de los artículos conforma "
            f"el núcleo H — alta proporción de trabajos con impacto real "
            f"respecto al total publicado."
        )
    elif ratio_hn < 10:
        negativos.append(
            f"Solo el {ratio_hn}% de los artículos conforma el núcleo H. "
            f"Gran parte de la producción no contribuye al indicador de impacto principal. "
            f"Publicar menos pero en revistas de mayor alcance podría mejorar este ratio."
        )
    else:
        positivos.append(
            f"Eficiencia productiva moderada: {ratio_hn}% de artículos conforman el núcleo H. "
            f"Balance aceptable entre productividad e impacto."
        )

    return positivos, negativos, notas


def dibujar_analisis(ax_ana, positivos: list, negativos: list, C: dict, notas: list = None):
    """
    Dibuja el cuadro de análisis con diseño mejorado.
    Columnas positivas (verde) y negativas (rojo) con fondos destacados.
    Notas aclaratorias en gris.
    
    Parámetros
    ----------
    ax_ana      : matplotlib axis para dibujar
    positivos   : lista de strings con hallazgos positivos
    negativos   : lista de strings con hallazgos negativos
    notas       : lista de strings con notas aclaratorias (opcional)
    C           : diccionario de colores
    """
    if notas is None:
        notas = []
    
    ax_ana.set_xlim(0, 1)
    ax_ana.set_ylim(0, 1)
    ax_ana.axis("off")

    # Fondo principal
    ax_ana.add_patch(FancyBboxPatch(
        (0, 0), 1, 1, boxstyle="square,pad=0",
        facecolor="white", edgecolor=C["GRIS_200"],
        linewidth=0.5, transform=ax_ana.transAxes))

    # Fondos de columnas (sutiles)
    ax_ana.add_patch(FancyBboxPatch(
        (0.01, 0.08), 0.48, 0.91, boxstyle="square,pad=0",
        facecolor="#F0FDF4", edgecolor="none",  # Verde muy claro
        transform=ax_ana.transAxes))
    ax_ana.add_patch(FancyBboxPatch(
        (0.51, 0.08), 0.48, 0.91, boxstyle="square,pad=0",
        facecolor="#FEF2F2", edgecolor="none",  # Rojo muy claro
        transform=ax_ana.transAxes))

    # Encabezados con fondos fuertes
    ax_ana.add_patch(FancyBboxPatch(
        (0.01, 0.85), 0.48, 0.14, boxstyle="square,pad=0",
        facecolor="#E7F5E0", edgecolor="#A8D5A8",
        linewidth=0.8, transform=ax_ana.transAxes))
    ax_ana.add_patch(FancyBboxPatch(
        (0.51, 0.85), 0.48, 0.14, boxstyle="square,pad=0",
        facecolor="#FCE4E4", edgecolor="#E8BCBC",
        linewidth=0.8, transform=ax_ana.transAxes))

    # Iconos y títulos de columnas
    ax_ana.text(0.027, 0.955, "✓",
                fontsize=12, fontweight="bold", color="#22C55E",
                transform=ax_ana.transAxes, va="top", ha="left")
    ax_ana.text(0.055, 0.95, "Aspectos positivos",
                fontsize=8.5, fontweight="bold", color="#166534",
                transform=ax_ana.transAxes, va="top", ha="left")

    ax_ana.text(0.527, 0.955, "⚠",
                fontsize=11, fontweight="bold", color="#EF4444",
                transform=ax_ana.transAxes, va="top", ha="left")
    ax_ana.text(0.555, 0.95, "Aspectos a mejorar",
                fontsize=8.5, fontweight="bold", color="#7F1D1D",
                transform=ax_ana.transAxes, va="top", ha="left")

    # Helper para word-wrap
    def wrap_text(text: str, max_chars: int = 68) -> list:
        """Divide texto en líneas respetando max_chars."""
        words = text.split()
        lines, line = [], ""
        for w in words:
            if len(line) + len(w) + 1 <= max_chars:
                line = (line + " " + w).strip()
            else:
                if line:
                    lines.append(line)
                line = w
        if line:
            lines.append(line)
        return lines

    # Viñetas positivos (columna izquierda)
    y = 0.80
    for idx, item in enumerate(positivos):
        lines = wrap_text(item, max_chars=68)
        if lines:
            # Bullet más destacado
            ax_ana.text(0.035, y, "•",
                        fontsize=9, color="#22C55E", fontweight="bold",
                        transform=ax_ana.transAxes, va="top", ha="left")
            # Primera línea
            ax_ana.text(0.052, y, lines[0],
                        fontsize=7.5, color="#1F2937",
                        transform=ax_ana.transAxes, va="top", ha="left")
            # Líneas adicionales
            y -= 0.108
            for extra in lines[1:]:
                ax_ana.text(0.052, y, extra,
                            fontsize=7.5, color="#4B5563",
                            transform=ax_ana.transAxes, va="top", ha="left")
                y -= 0.108
            y -= 0.02  # Espacio extra entre items
        
        if y < 0.10:
            break

    # Viñetas negativos (columna derecha)
    y = 0.80
    for idx, item in enumerate(negativos):
        lines = wrap_text(item, max_chars=68)
        if lines:
            # Bullet más destacado
            ax_ana.text(0.535, y, "•",
                        fontsize=9, color="#EF4444", fontweight="bold",
                        transform=ax_ana.transAxes, va="top", ha="left")
            # Primera línea
            ax_ana.text(0.552, y, lines[0],
                        fontsize=7.5, color="#1F2937",
                        transform=ax_ana.transAxes, va="top", ha="left")
            # Líneas adicionales
            y -= 0.108
            for extra in lines[1:]:
                ax_ana.text(0.552, y, extra,
                            fontsize=7.5, color="#4B5563",
                            transform=ax_ana.transAxes, va="top", ha="left")
                y -= 0.108
            y -= 0.02  # Espacio extra entre items
        
        if y < 0.10:
            break

    # ═══════════════════════════════════════════════════════════════════════════
    # NOTA: Las notas se incluyen en el INFORME PDF, no en la imagen PNG
    # La imagen PNG se mantiene limpia sin sección de notas
    # ═══════════════════════════════════════════════════════════════════════════
    # (Sección de notas eliminada - ver pdf_reporter.py para informe completo)

