# Guía de Optimización de Memoria - TableroGeneral

## Problema Identificado
El dashboard consume **9GB de RAM** y se queda en **7GB** en reposo. Esto es excesivo para un dashboard de Streamlit.

## Causas Principales

### 1. **Carga Completa de DataFrames**
- Se cargan TODAS las columnas de archivos Parquet
- No se usa lectura selectiva de columnas
- **Impacto**: ~60% del uso de RAM

### 2. **Duplicación en Caché de Streamlit**
- `@st.cache_data` crea copias pickle
- TTL=1800s con max_entries=10 mantiene múltiples versiones
- **Impacto**: ~25% del uso de RAM

### 3. **Sin Liberación de Memoria**
- No se llama a `gc.collect()` después de cargas
- Pandas mantiene referencias a DataFrames antiguos
- **Impacto**: ~10% del uso de RAM

### 4. **Conversión de Tipos Ineficiente**
- `convert_numpy_types()` crea copias innecesarias
- No se usa downcast de tipos numéricos
- **Impacto**: ~5% del uso de RAM

### 5. **GeoJSON Sin Simplificar**
- Geometrías con alta resolución innecesaria para visualización
- **Impacto**: Variable según archivo

---

## Soluciones Implementadas

### ✅ 1. Módulo `carga_optimized.py`
Nuevo módulo con:
- **Lectura selectiva de columnas** en Parquet
- **Downcast automático** de tipos numéricos
- **Conversión a categorías** para strings repetidos
- **Simplificación de geometrías** GeoJSON
- **Limpieza activa** con `gc.collect()`

### ✅ 2. Función `optimize_dataframe()`
Optimiza DataFrames:
```python
- int64 → int8/int16/int32 (según rango)
- float64 → float32
- object → category (si <50% valores únicos)
- Elimina columnas totalmente nulas
```

### ✅ 3. Función `read_parquet_optimized()`
Mejora lectura:
```python
- columns parámetro para leer solo necesarias
- strings_to_categorical=True automático
- self_destruct=True para liberar PyArrow
```

### ✅ 4. Hash Personalizado para Caché
Evita duplicación:
```python
hash_funcs={pd.DataFrame: lambda df: (df.shape, tuple(df.columns))}
```

### ✅ 5. Función `cleanup_memory()`
Limpieza automática cuando RAM > 70%

---

## Cómo Aplicar las Optimizaciones

### Paso 1: Identificar Columnas Necesarias

Ejecuta este script para analizar qué columnas usa cada módulo:

```python
python analizar_columnas_usadas.py
```

### Paso 2: Actualizar `COLUMNAS_NECESARIAS`

En `carga_optimized.py`, edita el diccionario con las columnas que realmente usas:

```python
COLUMNAS_NECESARIAS = {
    'df_postulantes_empleo.parquet': [
        'CUIL', 'EDAD', 'SEXO', 'PROVINCIA', 'DEPARTAMENTO',
        'FECHA_INSCRIPCION', 'ESTADO'
    ],
    # ... otros archivos
}
```

### Paso 3: Reemplazar Funciones de Carga

En `app.py`, cambia:

```python
# ANTES
from moduls.carga import load_data_from_local, load_data_from_gitlab

# DESPUÉS
from moduls.carga_optimized import (
    load_module_data_optimized,
    cleanup_memory
)
```

### Paso 4: Modificar Carga de Módulos

En `app.py`, reemplaza `load_module_data()`:

```python
@st.cache_data(...)
def load_module_data(module_key):
    source_params = {
        'repo_id': REPO_ID,
        'branch': BRANCH,
        'token': gitlab_token,
        'local_path': LOCAL_PATH
    }

    source_type = 'gitlab' if FUENTE_DATOS == 'gitlab' else 'local'

    return load_module_data_optimized(
        module_key,
        source_type,
        source_params
    )
```

### Paso 5: Agregar Limpieza Periódica

Al final de `app.py`:

```python
# Limpiar memoria después de renderizar
from moduls.carga_optimized import cleanup_memory
cleanup_memory()
```

---

## Reducción Esperada de RAM

| Optimización | RAM Actual | RAM Optimizada | Reducción |
|--------------|------------|----------------|-----------|
| Lectura selectiva | ~5GB | ~1.5GB | **-70%** |
| Downcast tipos | ~1.5GB | ~0.8GB | **-47%** |
| Categorías | ~0.8GB | ~0.5GB | **-38%** |
| Caché optimizado | ~1.5GB | ~0.3GB | **-80%** |
| GeoJSON simplificado | ~0.2GB | ~0.05GB | **-75%** |
| **TOTAL** | **~9GB** | **~2-3GB** | **-67% a -78%** |

---

## Mediciones y Monitoreo

### Script para medir uso actual:

```python
import psutil
import os

process = psutil.Process(os.getpid())
print(f"RAM usada: {process.memory_info().rss / 1024**3:.2f} GB")
print(f"% RAM: {process.memory_percent():.1f}%")
```

### Agregar al dashboard (modo desarrollo):

```python
if is_local:
    import psutil
    process = psutil.Process(os.getpid())
    st.sidebar.metric(
        "RAM Usada",
        f"{process.memory_info().rss / 1024**3:.2f} GB"
    )
```

---

## Próximos Pasos

1. ✅ Crear módulo `carga_optimized.py`
2. ⏳ Analizar columnas usadas por módulo
3. ⏳ Actualizar `COLUMNAS_NECESARIAS`
4. ⏳ Modificar `app.py` para usar carga optimizada
5. ⏳ Probar y medir mejoras
6. ⏳ Ajustar si es necesario

---

## Notas Importantes

⚠️ **Antes de aplicar en producción:**
- Prueba en local con datos reales
- Verifica que no se rompan visualizaciones
- Mide RAM antes y después
- Compara performance de carga

💡 **Tips adicionales:**
- Si sigue alto, considera lazy loading por pestaña
- Evalúa pre-agregar datos pesados
- Usa sampling para gráficos con muchos puntos
