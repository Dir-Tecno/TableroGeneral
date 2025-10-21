# Análisis Técnico Detallado - Aplicación Streamlit TableroGeneral

## 1. Resumen Ejecutivo

**Posibles causas prioritarias del alto consumo de recursos:**
- **CPU 130%**: Procesamiento intensivo de múltiples archivos Parquet grandes desde GitLab API con reintentos y conversiones de tipos NumPy/Pandas
- **RAM 80%**: Carga simultánea de DataFrames en memoria con caché de Streamlit (TTL 3600s) sin límites de tamaño
- **Escritura disco 4GB**: Logs extensivos de Sentry, caché de Streamlit, y posibles archivos temporales de procesamiento de datos
- **Tráfico RX 60Mb**: Descarga continua de archivos .parquet/.geojson desde GitLab API con reintentos automáticos y streaming por chunks

## 2. Descripción de Arquitectura

### Topología
```
[Contenedor Docker] 
├── Streamlit App (Puerto 8501)
├── Conexiones Externas:
│   ├── GitLab API (gitlab.com/api/v4)
│   ├── Sentry SDK (monitoreo errores)
│   ├── Google Analytics (tracking)
│   └── MinIO (almacenamiento alternativo)
└── Volúmenes/Montajes:
    ├── .streamlit/secrets.toml
    └── Datos locales (modo desarrollo)
```

### Mapa de Componentes
- **Streamlit UI**: Interfaz principal con 4 pestañas (Empleo, CBA Capacita, Banco Gente, Escrituración)
- **Módulo de Carga** (`moduls/carga.py`): Descarga y procesamiento de archivos desde GitLab/MinIO/Local
- **Workers/Background**: No hay workers dedicados, todo en hilo principal de Streamlit
- **Schedulers**: No hay schedulers, carga bajo demanda con caché TTL
- **Endpoints HTTP**: Solo Streamlit server, no APIs REST adicionales
- **Conexiones DB**: No hay bases de datos tradicionales, solo archivos Parquet como fuente

### Diagrama Lógico
```
Usuario → Streamlit → load_all_data() → GitLab API → Procesamiento Parquet → Caché → UI Tabs
                                    ↓
                               Sentry Logging → Escritura Disco
```

## 3. Inventario Técnico

### Variables/Flags de Configuración
```bash
# Variables críticas en st.secrets
FUENTE_DATOS=gitlab|minio|local
REPO_ID=Dir-Tecno/df_ministerio  
BRANCH=main
LOCAL_PATH=""
gitlab.token=<token_gitlab>
sentry.dsn=<sentry_dsn>
sentry_environment=production

# Variables de entorno (.env.local)
STREAMLIT_SERVER_MAX_UPLOAD_SIZE=200
STREAMLIT_SERVER_RUN_ON_SAVE=true
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
```

### Bibliotecas Clave
```python
# Dependencias críticas con versiones
streamlit==1.37.1          # Framework principal
pandas==2.2.2              # Procesamiento datos
pyarrow>=15.0.0           # Lectura Parquet
plotly==5.23.0            # Visualizaciones
requests==2.32.3          # API calls GitLab
sentry-sdk==1.43.0        # Monitoreo errores
folium==0.17.0            # Mapas geoespaciales
geopandas                 # Datos geoespaciales
duckdb==1.1.3            # Análisis datos
minio                     # Almacenamiento objeto
```

### Bases de Datos y Acceso
- **Tipo**: Archivos Parquet como "base de datos"
- **Driver**: PyArrow + Pandas
- **Pool**: No hay pool, conexiones directas HTTP
- **URIs**: `https://gitlab.com/api/v4/projects/{repo_id}/repository/files/{file}/raw`

### Manejo de Secretos
- **Método**: Streamlit secrets (`st.secrets`)
- **Ubicación**: `.streamlit/secrets.toml` (no versionado)
- **Tipos**: GitLab tokens, Sentry DSN, configuraciones

## 4. Análisis de Código

### Puntos Débiles Identificados

#### A. Carga Masiva Sin Control de Memoria
**Archivo**: `moduls/carga.py:64-107`
```python
@st.cache_data(ttl=3600, show_spinner="Cargando datos del dashboard...")
def load_all_data():
    # Carga TODOS los archivos simultáneamente sin límites
```
**Consecuencia**: RAM 80% - Múltiples DataFrames grandes en memoria simultáneamente
**Solución**: 
1. Implementar carga lazy/paginada por módulo
2. Reducir TTL caché a 1800s
3. Añadir `max_entries=5` al decorador cache

#### B. Reintentos Agresivos en GitLab API
**Archivo**: `moduls/carga.py:495-529`
```python
max_retries = 3
timeout = 60  # 60 segundos
for intento in range(max_retries):
    response = requests.get(url, headers=headers, params=params, timeout=timeout, stream=True)
```
**Consecuencia**: CPU 130% + RX 60Mb - Reintentos consumen CPU y ancho de banda
**Solución**:
1. Implementar backoff exponencial: `time.sleep(2**intento)`
2. Reducir timeout a 30s
3. Añadir circuit breaker después de 5 fallos

#### C. Conversión Ineficiente de Tipos NumPy
**Archivo**: `moduls/carga.py:19-38`
```python
def convert_numpy_types(df):
    for col in df.columns:
        if df[col].dtype.kind in 'iufc':
            df[col] = df[col].apply(convert_value)  # Muy lento
```
**Consecuencia**: CPU 130% - Conversión elemento por elemento
**Solución**:
1. Usar `pd.api.types.infer_dtype()` 
2. Conversión vectorizada: `df[col].astype('int64')`
3. Solo convertir columnas necesarias

#### D. Logging Excesivo de Sentry
**Archivo**: `utils/sentry_utils.py:43-56`
```python
traces_sample_rate=0.2,  # 20% de transacciones
profiles_sample_rate=0.1  # 10% de perfiles
```
**Consecuencia**: Escritura disco 4GB - Logs y traces excesivos
**Solución**:
1. Reducir `traces_sample_rate=0.05` (5%)
2. Reducir `profiles_sample_rate=0.02` (2%)
3. Añadir filtros por severidad

#### E. Caché Sin Límites de Tamaño
**Archivo**: `app.py:64`
```python
@st.cache_data(ttl=3600, show_spinner="Cargando datos del dashboard...")
```
**Consecuencia**: RAM 80% - Caché crece indefinidamente
**Solución**:
1. Añadir `max_entries=3`
2. Implementar `st.cache_data.clear()` periódico
3. Monitorear tamaño con `sys.getsizeof()`

## 5. Diagnóstico de Recursos

### Hipótesis CPU 130% (por orden de probabilidad)
1. **Conversión tipos NumPy/Pandas** (80% probabilidad)
   - Procesamiento elemento por elemento en `convert_numpy_types()`
   - **Comando**: `py-spy top --pid <streamlit_pid> --duration 60`

2. **Reintentos GitLab API** (60% probabilidad)  
   - Múltiples requests simultáneos con timeouts largos
   - **Comando**: `strace -p <pid> -e trace=network -c`

3. **Renderizado Plotly/Folium** (40% probabilidad)
   - Gráficos complejos con muchos puntos de datos
   - **Comando**: `perf top -p <pid> -g`

### Hipótesis RAM 80% (por orden de probabilidad)
1. **Caché Streamlit sin límites** (90% probabilidad)
   - Múltiples DataFrames grandes en `st.cache_data`
   - **Comando**: `memory_profiler` en `load_all_data()`

2. **Carga simultánea archivos Parquet** (70% probabilidad)
   - Todos los módulos cargan datos al mismo tiempo
   - **Comando**: `ps aux --sort=-%mem | head -20`

### Hipótesis Escritura Disco 4GB (por orden de probabilidad)
1. **Logs Sentry excesivos** (85% probabilidad)
   - Breadcrumbs y traces con sample rate alto
   - **Comando**: `iotop -p <pid> -a`

2. **Caché Streamlit en disco** (60% probabilidad)
   - Archivos temporales de caché
   - **Comando**: `lsof +D /tmp | grep streamlit`

3. **Archivos temporales Pandas** (40% probabilidad)
   - Procesamiento archivos grandes
   - **Comando**: `find /tmp -name "*.parquet" -size +10M`

### Hipótesis Tráfico RX 60Mb (por orden de probabilidad)
1. **Descarga archivos GitLab** (95% probabilidad)
   - Archivos Parquet/GeoJSON grandes con reintentos
   - **Comando**: `nethogs -p <pid>`

2. **Polling GitLab API** (30% probabilidad)
   - Verificaciones periódicas de archivos
   - **Comando**: `netstat -p | grep <pid>`

## 6. Plan de Mitigación

### Inmediato (Bajo Riesgo)
1. **Reducir sample rates Sentry**
   ```python
   traces_sample_rate=0.05,
   profiles_sample_rate=0.02
   ```

2. **Añadir límites caché**
   ```python
   @st.cache_data(ttl=1800, max_entries=3)
   ```

3. **Implementar backoff exponencial**
   ```python
   import time
   time.sleep(min(2**intento, 30))
   ```

4. **Optimizar conversión tipos**
   ```python
   df = df.convert_dtypes()  # Más eficiente
   ```

### Mediano Plazo (Cambios Arquitecturales)
1. **Carga lazy por módulo**
   - Solo cargar datos del tab activo
   - Implementar `load_single_module()`

2. **Cache distribuido**
   - Redis/Memcached para caché compartido
   - Evitar recarga en cada instancia

3. **Pre-procesamiento datos**
   - ETL batch para optimizar Parquet
   - Reducir tamaño archivos

4. **Monitoreo recursos**
   - Métricas CPU/RAM/Disco en tiempo real
   - Alertas automáticas

## 7. Prioridad e Impacto

| Cambio | Prioridad | Impacto CPU | Impacto RAM | Impacto I/O | Esfuerzo |
|--------|-----------|-------------|-------------|-------------|----------|
| Límites caché | Alta | -20% | -40% | -10% | 1h |
| Sample rates Sentry | Alta | -5% | -5% | -60% | 30min |
| Backoff exponencial | Media | -30% | -5% | -20% | 2h |
| Conversión tipos optimizada | Media | -40% | -10% | 0% | 4h |
| Carga lazy módulos | Baja | -25% | -50% | -30% | 16h |

## 8. Puntos de Verificación

### 1 Hora
- **CPU**: `top -p <pid>` < 100%
- **RAM**: `ps -o pid,vsz,rss,pmem -p <pid>` < 70%
- **Logs Sentry**: Verificar reducción en dashboard

### 24 Horas  
- **Estabilidad**: Sin reinicios contenedor
- **Tiempo respuesta**: `curl -w "@curl-format.txt"` < 5s
- **Errores**: Logs aplicación sin timeouts GitLab

### 7 Días
- **Tendencia recursos**: Gráficos métricas sin crecimiento
- **Performance usuarios**: Feedback tiempo carga
- **Costos**: Reducción uso CPU/RAM en facturación

### Métricas Objetivo
- **CPU**: < 80% promedio
- **RAM**: < 60% del límite contenedor  
- **Escritura disco**: < 1GB/día
- **Tiempo carga**: < 10 segundos primera vez, < 3s con caché
- **Disponibilidad**: > 99.5% uptime


