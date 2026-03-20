# 📊 PROMPT PARA GENERAR GRÁFICOS DESDE DATOS DE SCOPUS CON ANÁLISIS COMPLETO

## Contexto
Recibirás datos analíticos completos de publicaciones de un autor extraídos de Scopus, incluyendo análisis profesional automático. Debes convertir estos datos en gráficos claros y profesionales, e integrar los hallazgos en el dashboard.

---

## 1. CÓMO OBTENER LOS DATOS

Haz una petición **POST** al endpoint:

```
POST http://localhost:8000/api/authors/charts/analyze-default?author_id={ID_AUTOR}&year_from={AÑO_INICIO}&year_to={AÑO_FIN}&campo={DISCIPLINA}
```

**Parámetros:**
- `author_id`: ID entero del autor en la BD local **(requerido)**
- `year_from`: Año inicial para filtrar publicaciones (opcional, ej: 2015)
- `year_to`: Año final para filtrar publicaciones (opcional, ej: 2025)
- `campo`: Campo disciplinar para umbrales específicos (opcional, default: CIENCIAS_SALUD):
  - `CIENCIAS_SALUD`
  - `CIENCIAS_BASICAS`
  - `INGENIERIA`
  - `CIENCIAS_SOCIALES`
  - `ARTES_HUMANIDADES`

**Respuesta:** JSON con estructura `ScopusAnalysisResponse`

**Ejemplos:**

```
# Solo autor (sin filtro de años)
POST http://localhost:8000/api/authors/charts/analyze-default?author_id=123

# Con rango de años
POST http://localhost:8000/api/authors/charts/analyze-default?author_id=123&year_from=2015&year_to=2025

# Con campo disciplinar específico
POST http://localhost:8000/api/authors/charts/analyze-default?author_id=123&campo=INGENIERIA

# Completo (recomendado para optimizar)
POST http://localhost:8000/api/authors/charts/analyze-default?author_id=123&year_from=2020&year_to=2025&campo=CIENCIAS_BASICAS
```

---

## 1.5. OPTIMIZACIÓN Y RECOMENDACIONES

### **Impacto de los parámetros en recursos:**

| Parámetro | Impacto | Recomendación |
|-----------|--------|--------------|
| `year_from` / `year_to` | ⬇️ Reduce carga API en 30-70% | **Siempre usar** para períodos específicos |
| `campo` | ✅ Umbrales precisos por disciplina | Seleccionar según especialidad del autor |
| Sin filtros | ⬆️​ Consume máximos recursos | Solo para análisis histórico completo |

### **Estrategias recomendadas:**

1. **Para investigadores activos:** `year_from=2020` (últimos 5 años)
2. **Para evaluación anual:** `year_from={año-1}&year_to={año}` (último año)
3. **Para reportes institucionales:** `year_from=2015` (últimos 10 años, balance)
4. **Para análisis específico:** Usar rango exacto + campo disciplinar

### **Umbrales por disciplina** (aplicados automáticamente):

```json
{
  "CIENCIAS_SALUD": {
    "h_alto": 15,
    "h_medio": 8,
    "cpp_alto": 15,
    "cpp_medio": 7
  },
  "CIENCIAS_BASICAS": {
    "h_alto": 20,
    "h_medio": 10,
    "cpp_alto": 20,
    "cpp_medio": 8
  },
  "INGENIERIA": {
    "h_alto": 10,
    "h_medio": 5,
    "cpp_alto": 8,
    "cpp_medio": 4
  },
  "CIENCIAS_SOCIALES": {
    "h_alto": 8,
    "h_medio": 4,
    "cpp_alto": 8,
    "cpp_medio": 3
  },
  "ARTES_HUMANIDADES": {
    "h_alto": 5,
    "h_medio": 3,
    "cpp_alto": 5,
    "cpp_medio": 2
  }
}
```

---

```json
{
  "success": true,
  "message": "Análisis de Juan Pérez completado: 45 publicaciones, 320 citaciones",
  "query_used": "AU-ID(57193767797)",
  "total_publications": 45,
  "total_citations": 320,
  
  "statistics": {
    "total_publications": 45,
    "total_citations": 320,
    "min_year": 2015,
    "max_year": 2025,
    "avg_per_year": 4.5,
    "peak_year": 2023,
    "peak_publications": 8,
    "active_years": 11,
    "h_index": 12,
    "citation_per_publication": 7.11,
    "percent_cited": 84.5,
    "publications_by_year": [
      {
        "year": 2025,
        "count": 3,
        "percentage": 6.67,
        "citations": 15,
        "avg_citations_per_publication": 5.0
      },
      ...
    ],
    "citations_by_year": [
      {
        "year": 2025,
        "citations": 15,
        "publications": 3
      },
      ...
    ],
    "publications_detail": [
      {
        "id": 1,
        "title": "Machine Learning in Healthcare",
        "year": 2024,
        "doi": "10.1234/example",
        "citations": 45,
        "publication_type": "Article",
        "source_journal": "Nature Medicine",
        "url": "https://...",
        "authors_count": 5,
        "is_open_access": true
      },
      ...
    ]
  },
  
  "publications_by_year": [...],
  "citations_by_year": [...],
  "publications_detail": [...],
  
  "top_cited_publications": [
    // Top 10 publicaciones más citadas
  ],
  
  "publication_types_distribution": {
    "Article": 38,
    "Conference Paper": 5,
    "Review": 2
  },
  
  "journals_distribution": {
    "Nature Medicine": 8,
    "Lancet": 5,
    "JAMA": 4
  },
  
  "findings_positive": [
    "Índice H = 12: supera el umbral de trayectoria consolidada (≥8)...",
    "CPP = 7.11: dentro del rango aceptable para Ciencias de la Salud...",
    "Impacto distribuido: relación CPP/mediana es 2.5× (umbral crítico: >3×)..."
  ],
  
  "findings_negative": [
    "Solo el 70% de los artículos ha recibido citas...",
    "Tendencia de producción decreciente en últimos 3 años..."
  ],
  
  "findings_notes": [
    "Nota: Artículos de 2025 con bajo CPP - normal para producción reciente..."
  ],
  
  "generated_at": "2026-03-20T14:30:45.123456"
}
```

---

## 3. GRÁFICOS RECOMENDADOS

### **Gráfico 1: LÍNEA - Publicaciones por año**
- **Fuente:** `statistics.publications_by_year`
- **Ejes:**
  - X: `year` (años)
  - Y: `count` (número de publicaciones)
- **Estilo:** Línea continua con marcadores (circles)
- **Color:** Azul (#3B82F6)
- **Descripción:** Muestra la productividad del autor a lo largo del tiempo

### **Gráfico 2: ÁREA SUPERPUESTA - Publicaciones y Citaciones por año**
- **Fuente:** `statistics.publications_by_year` y `statistics.citations_by_year`
- **Ejes:**
  - X: `year`
  - Y1: `count` (publicaciones)
  - Y2: `citations` (citaciones)
- **Estilos:**
  - Área de publicaciones: Azul semitransparente (#3B82F6 con 30% opacidad)
  - Línea de citaciones: Rojo sólido (#F87171)
- **Leyenda:** "Publicaciones" y "Citaciones"
- **Descripción:** Correlaciona impacto (citaciones) con productividad

### **Gráfico 3: BARRAS - Top 10 publicaciones más citadas**
- **Fuente:** `top_cited_publications` (ordenado descendente)
- **Ejes:**
  - X: Título truncado (primeros 50 caracteres)
  - Y: `citations` (número de citaciones)
- **Color:** Gradiente verde → rojo (por número de citas)
- **Interacción:** Al pasar el mouse, mostrar:
  - Título completo
  - Año de publicación
  - Journal
  - DOI (si disponible)
- **Descripción:** Identifica el trabajo más impactante

### **Gráfico 4: TORTA - Distribución de tipos de publicación**
- **Fuente:** `publication_types_distribution`
- **Datos:** Pares clave-valor (tipo → cantidad)
- **Colores:** Paleta diferenciada (6-8 colores)
- **Etiquetas:** Mostrar tipo + cantidad + porcentaje
- **Interacción:** Tooltip al pasar el mouse
- **Descripción:** Diversidad de tipos de producción

### **Gráfico 5: BARRAS HORIZONTAL - Top 15 journals**
- **Fuente:** `journals_distribution` (ordenado desc, máximo 15)
- **Ejes:**
  - Y: Nombre del journal (truncado si > 40 caracteres)
  - X: Cantidad de publicaciones
- **Color:** Verde uniforme (#34D399)
- **Interacción:** Tooltip con nombre completo del journal
- **Descripción:** Revistas donde publica con más frecuencia

### **Gráfico 6: KPI CARDS - Indicadores clave**
Mostrar 4 tarjetas con los siguientes valores:

```
┌─────────────────────┐
│ Total Publicaciones  │
│        45           │  
│    (statistics.total_publications)
└─────────────────────┘

┌─────────────────────┐
│  Total Citaciones    │
│        320          │  
│    (statistics.total_citations)
└─────────────────────┘

┌─────────────────────┐
│     H-Index         │
│        12           │  
│    (statistics.h_index)
└─────────────────────┘

┌─────────────────────┐
│ Citas/Publicación   │
│       7.11          │  
│ (statistics.citation_per_publication)
└─────────────────────┘
```

**Estilos KPI:**
- Fondo blanco con borde sutil gris
- Número grande en color azul principal (#3B82F6)
- Etiqueta pequeña en gris oscuro
- Sombra suave para profundidad

---

## 4. SECCIÓN DE ANÁLISIS PROFESIONAL

**Mostrar en una sección clara y prominent del dashboard, bajo los gráficos:**

### **✓ Aspectos Positivos** (Verde)
```html
<div class="findings-positive">
  <h3>✓ Aspectos Positivos</h3>
  <ul>
    {findings_positive.map(item => <li>{item}</li>)}
  </ul>
</div>
```

**Estilos:**
- Fondo: Verde muy claro (#F0FDF4)
- Borde izquierdo: Verde oscuro (#10B981) - 4px
- Icono: ✓ en verde (#22C55E)
- Texto: Gris oscuro (#1F2937), 13px

### **⚠ Aspectos a Mejorar** (Rojo/Naranja)
```html
<div class="findings-negative">
  <h3>⚠ Aspectos a Mejorar</h3>
  <ul>
    {findings_negative.map(item => <li>{item}</li>)}
  </ul>
</div>
```

**Estilos:**
- Fondo: Rojo muy claro (#FEF2F2)
- Borde izquierdo: Rojo oscuro (#DC2626) - 4px
- Icono: ⚠ en rojo (#EF4444)
- Texto: Gris oscuro (#1F2937), 13px

### **📝 Notas Aclaratorias** (Gris)
```html
<div class="findings-notes">
  <h3>📝 Notas Aclaratorias</h3>
  <ul>
    {findings_notes.map(item => <li>{item}</li>)}
  </ul>
</div>
```

**Estilos:**
- Fondo: Gris muy claro (#F9FAFB)
- Borde izquierdo: Gris (#9CA3AF) - 4px
- Icono: 📝 en gris (#6B7280)
- Texto: Gris medio (#4B5563), 13px

**Diseño de layout para hallazgos:**
- Dos columnas en pantallas grandes (>1200px)
  - Izquierda: Positivos
  - Derecha: Negativos
- Una columna en pantallas medianas/pequeñas
- Notas debajo en tamaño completo
- Espaciado: 20px entre secciones
- Máximo ancho: 100% del contenedor, con padding lateral

---

## 5. INSTRUCCIONES ESPECÍFICAS DE FRONTEND

### **A. Validaciones antes de graficar:**
```javascript
// Validar respuesta
if (!response.success) {
    mostrar_error(response.message);
    return null;
}

// Validar datos
if (response.total_publications === 0) {
    mostrar_alerta("Sin publicaciones encontradas para este autor en el período seleccionado");
    return null;
}

if (!response.statistics) {
    mostrar_error("Datos incompletos recibidos del servidor");
    return null;
}

// Validar hallazgos (debe haber al menos análisis)
if (!response.findings_positive && !response.findings_negative) {
    console.warn("Sin análisis disponible");
}
```

### **B. Cómo llamar al endpoint desde el frontend:**

```javascript
// Construir parámetros desde el formulario
const params = new URLSearchParams();
params.append('author_id', authorId);  // requerido

if (yearFrom) {
    params.append('year_from', yearFrom);  // ej: 2020
}
if (yearTo) {
    params.append('year_to', yearTo);  // ej: 2025
}
if (campo) {
    params.append('campo', campo);  // ej: CIENCIAS_SALUD
}

// Hacer la petición
const response = await fetch(
    `http://localhost:8000/api/authors/charts/analyze-default?${params}`,
    { method: 'POST' }
);

if (!response.ok) {
    const error = await response.json();
    console.error(error.detail);
    return;
}

const data = await response.json();
renderizarGraficos(data);
```

**Valores válidos de `campo`:**
```
- CIENCIAS_SALUD (default)
- CIENCIAS_BASICAS
- INGENIERIA
- CIENCIAS_SOCIALES
- ARTES_HUMANIDADES
```

### **C. Títulos y etiquetas dinámicas:**
- **Título principal:** Extraer nombre del autor de `response.message` (antes de "completado")
- **Subtítulo:** Usar `response.message` completo
- **Rango de años:** Mostrar "Período: {statistics.min_year} - {statistics.max_year}"
- **Fecha de actualización:** "Datos actualizados: {generated_at}"
- **Scopus ID:** Mostrar en fine print: "Query utilizada: {query_used}"

### **C. Títulos y etiquetas dinámicas:**
- **Título principal:** Extraer nombre del autor de `response.message` (antes de "completado")
- **Subtítulo:** Usar `response.message` completo
- **Rango de años:** Mostrar "Período: {statistics.min_year} - {statistics.max_year}"
- **Campo disciplinar:** Mostrar "Disciplina: {campo}" en fine print
- **Fecha de actualización:** "Datos actualizados: {generated_at}"
- **Scopus ID:** Mostrar en fine print: "Query utilizada: {query_used}"

### **D. Formulario de parámetros recomendado:**

```html
<form class="analysis-form">
  <div class="form-group">
    <label>ID Autor *</label>
    <input type="number" id="author_id" min="1" required />
  </div>
  
  <div class="form-row">
    <div class="form-group">
      <label>Año Inicio (opcional)</label>
      <input type="number" id="year_from" min="1900" max="2100" placeholder="ej: 2015" />
    </div>
    <div class="form-group">
      <label>Año Fin (opcional)</label>
      <input type="number" id="year_to" min="1900" max="2100" placeholder="ej: 2025" />
    </div>
  </div>
  
  <div class="form-group">
    <label>Campo Disciplinar</label>
    <select id="campo">
      <option value="CIENCIAS_SALUD">Ciencias de la Salud</option>
      <option value="CIENCIAS_BASICAS">Ciencias Básicas</option>
      <option value="INGENIERIA">Ingeniería</option>
      <option value="CIENCIAS_SOCIALES">Ciencias Sociales</option>
      <option value="ARTES_HUMANIDADES">Artes y Humanidades</option>
    </select>
  </div>
  
  <button type="submit" class="btn-primary">Analizar</button>
</form>
```

### **E. Filtros y opciones interactivas:**
- Permitir filtrar datos por rango de años (`min_year`, `max_year`)
- Toggle para mostrar/ocultar cada gráfico individualmente
- Toggle para expandir/contraer secciones de hallazgos (positivos/negativos son collapsed por defecto si hay > 5 items)
- Botones de exportación:
  - "Descargar gráficos como PNG" (todos en un ZIP)
  - "Descargar análisis como PDF" (incluir gráficos + hallazgos)
  - "Copiar datos como JSON" (para integración con otras herramientas)

### **E. Filtros y opciones interactivas:**
- Permitir filtrar datos por rango de años (`year_from`, `year_to`)
- Toggle para mostrar/ocultar cada gráfico individualmente
- Toggle para expandir/contraer secciones de hallazgos
- Selector de campo disciplinar (permite recalcular análisis con otros umbrales)
- Botones de exportación:
  - "Descargar gráficos como PNG" (todos en un ZIP)
  - "Descargar análisis como PDF" (incluir gráficos + hallazgos)
  - "Copiar datos como JSON" (para integración con otras herramientas)

### **F. Formato de números:**
- **Números enteros sin decimales:** 45, 320 (publications, citations)
- **Promedios con 1-2 decimales:** 7.11 (CPP), 4.5 (avg_per_year)
- **H-index como entero:** 12
- **Porcentajes con 1 decimal:** 81.5%, 6.67%
- **Separador de miles:** 1,000+ (si aplica)
- **Años sin separadores:** 2015, 2025

### **F. Formato de números:**
- **Números enteros sin decimales:** 45, 320 (publications, citations)
- **Promedios con 1-2 decimales:** 7.11 (CPP), 4.5 (avg_per_year)
- **H-index como entero:** 12
- **Porcentajes con 1 decimal:** 81.5%, 6.67%
- **Separador de miles:** 1,000+ (si aplica)
- **Años sin separadores:** 2015, 2025

### **G. Responsividad:**
- **Desktop (>1200px):** 2 columnas de gráficos, hallazgos lado a lado
- **Tablet (768px-1199px):** 2 columnas gráficos, hallazgos stacked
- **Mobile (<768px):** 1 columna todo, stacked verticalmente
- **KPI Cards:** 2x2 grid en desktop, 1x4 en mobile

---

## 6. FLUJO DE RENDERIZADO

```
1. Recibir ID de autor (de parámetro, input, o seleccionar)
   ↓
2. Validar que ID sea número válido
   ↓
3. Mostrar loading spinner
   ↓
4. Llamar POST /analyze-default?author_id=ID
   ↓
5. Recibir respuesta JSON
   ↓
6. Validar respuesta (success=true, tiene datos)
   ↓
7. Extraer datos principales:
   • statistics: todos los KPIs, arrays de datos
   • findings_positive/negative/notes: análisis profesional
   • publications_by_year: para gráfico línea
   • citations_by_year: para gráfico área
   • top_cited_publications: para barras horizontales
   • publication_types_distribution: para torta
   • journals_distribution: para barras horizontal
   ↓
8. Renderizar en el dashboard:
   ├─ Header con nombre y período
   ├─ 4 KPI Cards
   ├─ 6 Gráficos en grid responsivo
   ├─ Sección Análisis Profesional (3 columnas)
   └─ Footer con fecha y opciones de descarga
   ↓
9. Permitir exportación:
   • PNG individual de cada gráfico
   • PDF con todos los gráficos + hallazgos
   • JSON raw para integración
   ↓
10. Implementar interactividad:
   • Tooltips en todos los gráficos
   • Click para expandir detalles
   • Filtros por año funcionales
   • Deep linking (compartir URL con filtros)
```

---

## 7. MANEJO DE ERRORES

### **Si `success=false`:**
```json
{
  "detail": "El autor 'Juan Pérez' no tiene ID Scopus (AU-ID) registrado. Por favor, enriquezca el perfil del autor con su ID de Scopus."
}
```

**Acciones:**
- Mostrar el mensaje en un modal/alert
- Ofrecer enlace a "Editar perfil del autor"
- Sugerir cargar ID Scopus desde Scopus.com

### **Códigos HTTP esperados:**
- `200`: OK, datos completos listos para graficar
- `404`: Autor no encontrado o sin Scopus ID registrado
- `500`: Error interno en Scopus API o servidor

### **Mensajes de error personalizados:**
```javascript
const errorMessages = {
  404: "Autor no encontrado o sin ID Scopus. Verifique el ID y que el perfil esté completamente configurado.",
  500: "Error al consultar Scopus. Por favor intente nuevamente en unos minutos.",
  networkError: "Error de red. Verifique su conexión e intente de nuevo."
};
```

---

## 8. EJEMPLO DE HALLAZGOS REALES

### **Findings: Positive**
```
✓ Índice H = 12: supera el umbral de trayectoria consolidada (≥8) para su disciplina. 
  Refleja un núcleo amplio y consistente de publicaciones con impacto sostenido.

✓ CPP = 7.11: dentro del rango aceptable para Ciencias de la Salud 
  (umbral mínimo: 7.0 citas/artículo). Impacto promedio en línea 
  con el comportamiento típico del campo.

✓ Impacto distribuido: la relación CPP/mediana es 2.5× (umbral crítico: >3×). 
  El impacto está repartido de manera homogénea entre la producción, 
  indicador de solidez y no dependencia de artículos extraordinarios.

✓ Eficiencia productiva: el 26.7% de los artículos conforma 
  el núcleo H — alta proporción de trabajos con impacto real 
  respecto al total publicado.

✓ Producción estable en últimos 3 años (2023-2025): 22 artículos. 
  Actividad investigativa sostenida.
```

### **Findings: Negative**
```
⚠ Solo el 70% de los artículos ha recibido citas (umbral saludable: ≥75%). 
  Se recomienda mejorar la difusión: publicación en acceso abierto, 
  depósito en repositorios institucionales.

⚠ Concentración de impacto: CPP (7.11) es 3.5× la mediana (2.0). 
  El impacto está concentrado en pocos artículos clave. 
  Una estrategia más diversificada reduciría la dependencia de trabajos específicos.

⚠ Dependencia del año pico: el 35% de las citas totales 
  proviene de artículos publicados en 2020 (umbral crítico: >30%). 
  El impacto acumulado es vulnerable a la obsolescencia de esos trabajos específicos.
```

### **Findings: Notes**
```
📝 Nota: Artículos de 2025 con CPP = 3.2 (bajo). Normal para producción reciente 
  — requieren 18–36 meses para acumular citas. Este valor no debe interpretarse 
  como deterioro del impacto.

📝 Metodología: Los indicadores H-index, CPP y mediana se calculan 
  según estándares de Hirsch (2005) y Bornmann & Daniel (2009). 
  Los umbrales son discipline-specific de Minciencias (2022).
```

---

## 9. LAYOUT RECOMENDADO (ASCII)

```
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║  📊 Perfil de Publicaciones: Juan Pérez                        ║
║  Período: 2015-2025  |  Scopus ID: 57193767797                ║
║  𝟿 Datos actualizados: 20 Mar 2026                            ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝

┌──────────┬──────────┬──────────┬──────────────┐
│   45     │   320    │    12    │    7.11      │
│   Pubs   │   Cit.   │ H-Index  │    CPP       │
└──────────┴──────────┴──────────┴──────────────┘

┌────────────────────────────┬────────────────────────────┐
│  Publicaciones por Año     │  Citaciones por Año        │
│  (Línea)                   │  (Área superpuesta)        │
│ [GRÁFICO 1]                │ [GRÁFICO 2]                │
└────────────────────────────┴────────────────────────────┘

┌────────────────────────────┬────────────────────────────┐
│  Top 10 Más Citadas        │  Tipo de Publicación       │
│  (Barras)                  │  (Torta)                   │
│ [GRÁFICO 3]                │ [GRÁFICO 4]                │
└────────────────────────────┴────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│  Top 20 Journals                                       │
│  (Barras Horizontal)                                   │
│  [GRÁFICO 5]                                           │
└────────────────────────────────────────────────────────┘

┌────────────────────────────┬────────────────────────────┐
│ ✓ POSITIVOS (verde)        │ ⚠ A MEJORAR (rojo)         │
│                            │                            │
│ • Aspecto 1                │ • Aspecto 1                │
│ • Aspecto 2                │ • Aspecto 2                │
│ • Aspecto 3                │ • Aspecto 3                │
│ • Aspecto 4                │ • Aspecto 4                │
│ • Aspecto 5                │                            │
│                            │                            │
│ [+] Expandir               │ [+] Expandir               │
└────────────────────────────┴────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│ 📝 NOTAS ACLARATORIAS (gris)                           │
│                                                        │
│ • Nota sobre datos recientes (2025)...                │
│ • Aclaración sobre interpretación de H-index...       │
│ • Nota sobre limitaciones de datos...                 │
│                                                        │
│ [+] Expandir                                           │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│  Opciones de Descarga:                                 │
│  [↓ PNG] [↓ PDF] [≡ JSON]                              │
└────────────────────────────────────────────────────────┘
```

---

## 10. INTEGRACIONES Y CASOS DE USO

### **Casos de Uso Recomendados:**
1. **Dashboard de Investigadores:** Mostrar este análisis en perfil público de cada investigador
2. **Reportes Institucionales:** Exportar PDF para incluir en reportes de productividad
3. **Comparativas:** Mostrar lado a lado análisis de múltiples investigadores
4. **Seguimiento Longitudinal:** Guardar snapshots anuales para ver evolución
5. **Evaluación:** Usar hallazgos como input para evaluación de desempeño

### **APIs que pueden consumir este endpoint:**
- Power BI / Tableau (para dashboards avanzados)
- Sistemas de gestión de investigación
- Plataformas de evaluación académica
- Portales de transparencia institucional
- Sistemas de alertas personalizadas

---

## 11. CHECKLIST DE IMPLEMENTACIÓN

```
□ Crear página/componente para mostrar análisis
□ Implementar llamada POST a /analyze-default
□ Validar respuesta y manejar errores
□ Crear KPI Cards con estilos
□ Implementar 6 gráficos (usar Chart.js, Recharts o D3.js)
□ Crear sección de hallazgos (positivos/negativos/notas)
□ Aplicar estilos (colores, fuentes, espaciado)
□ Hacer responsivo (mobile, tablet, desktop)
□ Implementar tooltips en gráficos
□ Agregar filtros por año
□ Implementar exportación PNG/PDF/JSON
□ Agregar loading state
□ Crear manejo de errores
□ Testing (happy path + error cases)
□ Documentar componentes para otros devs
□ Publicar en producción
```

---

## 12. PALETA DE COLORES RECOMENDADA

```
Primarios:
- Azul Principal:    #3B82F6 (líneas, barras)
- Rojo Citaciones:   #F87171 (citaciones)
- Verde Positivo:    #34D399 (success, positivos)

Acentos:
- Verde Oscuro:      #10B981 (bordes positivos)
- Rojo Oscuro:       #DC2626 (bordes negativos)
- Morado CPP:        #A78BFA (línea punteada)

Neutros:
- Gris 900:          #111827 (títulos)
- Gris 600:          #4B5563 (texto)
- Gris 400:          #9CA3AF (labels)
- Gris 200:          #E5E7EB (borders)
- Gris 100:          #F3F4F6 (backgrounds)

Fondos Análisis:
- Verde claro:       #F0FDF4 (positivos)
- Rojo claro:        #FEF2F2 (negativos)
- Gris claro:        #F9FAFB (notas)
```

---

## 13. REFERENCIAS Y METODOLOGÍA

**Papers citados:**
- Hirsch, J. E. (2005). "An index to quantify an individual's scientific research output." PNAS
- Bornmann, L., & Daniel, H. D. (2009). "Does the h index have predictive power?" JASIS&T
- Minciencias (2022). "Modelo de medición de investigadores colombianos"
- SCImago (2023). "Journal Ranking by Discipline"

**Umbrales disciplinares:**
- Ciencias de la Salud: H ≥ 8 (alto ≥ 15), CPP ≥ 7.0
- Ciencias Básicas: H ≥ 10 (alto ≥ 20), CPP ≥ 8.0
- Ingeniería: H ≥ 5 (alto ≥ 10), CPP ≥ 4.0
- Ciencias Sociales: H ≥ 4 (alto ≥ 8), CPP ≥ 3.0
- Artes y Humanidades: H ≥ 3 (alto ≥ 5), CPP ≥ 2.0

---

**¡Documento completo listo para implementación!** 🚀

Última actualización: 20 de marzo de 2026
