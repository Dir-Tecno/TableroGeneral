# 📊 TableroGeneral - Dashboard Ministerio de Desarrollo Social

Dashboard interactivo desarrollado en Streamlit para visualizar y analizar datos del Ministerio de Desarrollo Social y Promoción del Empleo.

## 🚀 Características

- **4 Módulos Principales**: Programas de Empleo, CBA Me Capacita, Banco de la Gente, Escrituración
- **Carga Lazy**: Optimización de memoria con carga bajo demanda por módulo
- **Múltiples Fuentes**: Soporte para GitLab, MinIO y archivos locales
- **Monitoreo**: Integración con Sentry para tracking de errores
- **Analytics**: Google Analytics integrado
- **Visualizaciones**: Mapas interactivos, gráficos Plotly, KPIs dinámicos

## 📋 Requisitos

- Python 3.8+
- Streamlit 1.37.1
- Dependencias listadas en `requirements.txt`

## 🛠️ Instalación

### 1. Clonar el repositorio
```bash
git clone <url-del-repositorio>
cd TableroGeneral
```

### 2. Crear entorno virtual
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o
venv\Scripts\activate     # Windows
```

### 3. Instalar dependencias

#### 🆕 Instalación automática en Windows:
```bash
# Ejecutar el instalador automático
install_windows.bat
```
ℹ️ Este script instala todo automáticamente y configura el entorno.

#### Instalación manual en Windows:
```bash
# 1. Eliminar entorno anterior si existe
rmdir /s venv

# 2. Crear nuevo entorno
python -m venv venv
venv\Scripts\activate

# 3. Actualizar herramientas de construcción
python -m pip install --upgrade pip setuptools wheel

# 4. Instalar numpy (usando wheels precompilados)
pip install "numpy>=1.21.0,<2.0.0" --only-binary=all

# 5. Instalar dependencias geoespaciales
pip install --only-binary=all "shapely>=1.8.0,<3.0.0" "fiona>=1.8.0,<2.0.0" "pyproj>=3.3.0,<4.0.0"
pip install "geopandas>=0.12.0,<0.15.0" --only-binary=all

# 6. Instalar el resto
pip install -r requirements.txt
```

#### En Linux/Mac:
```bash
pip install -r requirements.txt
```

#### Si tienes problemas con geopandas:
```bash
# Opción alternativa con conda
conda install geopandas
pip install -r requirements.txt --no-deps
```

## ⚙️ Configuración

### Archivo de Secretos (.streamlit/secrets.toml)

Crear el directorio `.streamlit/` y el archivo `secrets.toml`:

```toml
# Configuración principal
FUENTE_DATOS = "gitlab"  # Opciones: "gitlab", "minio", "local"
REPO_ID = "Dir-Tecno/df_ministerio"
BRANCH = "main"
LOCAL_PATH = ""  # Solo para modo local

# Token GitLab (requerido para FUENTE_DATOS="gitlab")
[gitlab]
token = "glpat-xxxxxxxxxxxxxxxxxxxx"

# Configuración Sentry (opcional)
[sentry]
dsn = "https://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@sentry.io/xxxxxxx"
environment = "production"  # o "development"

# Configuración MinIO (opcional, para FUENTE_DATOS="minio")
[minio]
endpoint = "minio.ejemplo.com"
access_key = "tu_access_key"
secret_key = "tu_secret_key"
bucket = "nombre-bucket"
secure = true

# Google Analytics (opcional)
[analytics]
tracking_id = "G-XXXXXXXXXX"
```

### Variables de Entorno (.env.local)

Para desarrollo local, crear archivo `.env.local`:

```bash
# Variables de entorno para desarrollo local
PYTHONPATH=${workspaceFolder}

# Opciones Streamlit
STREAMLIT_SERVER_RUN_ON_SAVE=true
STREAMLIT_SERVER_MAX_UPLOAD_SIZE=200
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Configuración de desarrollo
STREAMLIT_SERVER_PORT=8501
STREAMLIT_SERVER_ADDRESS=localhost
```

## 🚀 Uso

### Modo Local (Desarrollo)

#### Opción A: Sin datos (Testing de interfaz)
1. **Configurar secrets.toml**:
   ```toml
   FUENTE_DATOS = "local"
   LOCAL_PATH = "./data"  # Carpeta vacía o inexistente
   ```

2. **Ejecutar aplicación**:
   ```bash
   streamlit run app.py
   ```
   
   ℹ️ La app funcionará mostrando "No se encontraron datos" pero permitirá navegar por toda la interfaz.

#### Opción B: Con datos de ejemplo (Funcionalidad completa)
1. **Generar datos de ejemplo**:
   ```bash
   python create_sample_data.py
   ```
   
   Esto creará:
   - 📊 1,000 postulantes de empleo
   - 🏢 200 empresas registradas
   - 🎓 800 estudiantes CBA Me Capacita
   - 🏦 1,200 créditos Banco de la Gente
   - 🗺️ Mapa georreferenciado de departamentos

2. **Configurar secrets.toml**:
   ```toml
   FUENTE_DATOS = "local"
   LOCAL_PATH = "./data"
   ```

3. **Ejecutar aplicación**:
   ```bash
   streamlit run app.py
   ```

4. **Acceder**: http://localhost:8501

### Modo GitLab (Producción)

1. **Obtener token GitLab**:
   - Ir a GitLab → Settings → Access Tokens
   - Crear token con scope `read_repository`

2. **Configurar secrets.toml**:
   ```toml
   FUENTE_DATOS = "gitlab"
   REPO_ID = "tu-namespace/tu-proyecto"
   BRANCH = "main"
   
   [gitlab]
   token = "tu_token_aqui"
   ```

3. **Ejecutar**:
   ```bash
   streamlit run app.py
   ```

### Modo MinIO (Almacenamiento Objeto)

1. **Configurar secrets.toml**:
   ```toml
   FUENTE_DATOS = "minio"
   
   [minio]
   endpoint = "tu-minio-server.com"
   access_key = "tu_access_key"
   secret_key = "tu_secret_key"
   bucket = "datos-dashboard"
   ```

2. **Ejecutar**:
   ```bash
   streamlit run app.py
   ```

## 🐳 Despliegue con Docker

### Dockerfile
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

### Docker Compose
```yaml
version: '3.8'
services:
  tablero:
    build: .
    ports:
      - "8501:8501"
    volumes:
      - ./.streamlit:/app/.streamlit
    environment:
      - STREAMLIT_SERVER_MAX_UPLOAD_SIZE=200
```

### Comandos Docker
```bash
# Construir imagen
docker build -t tablero-general .

# Ejecutar contenedor
docker run -p 8501:8501 -v $(pwd)/.streamlit:/app/.streamlit tablero-general

# Con Docker Compose
docker-compose up -d
```

## ☁️ Despliegue en la Nube

### Streamlit Cloud

1. **Conectar repositorio** en [share.streamlit.io](https://share.streamlit.io)
2. **Configurar secrets** en la interfaz web:
   ```
   FUENTE_DATOS = "gitlab"
   REPO_ID = "tu-repo"
   
   [gitlab]
   token = "tu_token"
   ```
3. **Deploy automático** desde main branch

### Heroku

1. **Crear Procfile**:
   ```
   web: sh setup.sh && streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
   ```

2. **Crear setup.sh**:
   ```bash
   mkdir -p ~/.streamlit/
   echo "[server]
   port = $PORT
   enableCORS = false
   headless = true
   " > ~/.streamlit/config.toml
   ```

3. **Deploy**:
   ```bash
   heroku create tu-app-name
   git push heroku main
   ```

### AWS/GCP/Azure

- Usar servicios de contenedores (ECS, Cloud Run, Container Instances)
- Configurar variables de entorno en lugar de secrets.toml
- Ejemplo para Cloud Run:
  ```bash
  gcloud run deploy tablero-general \
    --image gcr.io/tu-proyecto/tablero \
    --platform managed \
    --region us-central1 \
    --set-env-vars FUENTE_DATOS=gitlab
  ```

## 📁 Estructura del Proyecto

```
TableroGeneral/
├── app.py                 # Aplicación principal
├── requirements.txt       # Dependencias Python
├── README.md             # Este archivo
├── manual-desarrollo.md  # Análisis técnico detallado
├── create_sample_data.py # 🆕 Generador de datos de ejemplo
├── install_windows.bat  # 🆕 Instalador automático para Windows
├── .env.local           # Variables desarrollo
├── .gitignore           # Archivos ignorados
├── moduls/              # Módulos de la aplicación
│   ├── carga.py         # Carga de datos
│   ├── empleo.py        # Módulo Programas de Empleo
│   ├── bco_gente.py     # Módulo Banco de la Gente
│   ├── cbamecapacita.py # Módulo CBA Me Capacita
│   └── escrituracion.py # Módulo Escrituración
├── utils/               # Utilidades
│   ├── sentry_utils.py  # Integración Sentry
│   ├── ui_components.py # Componentes UI
│   ├── styles.py        # Estilos CSS
│   ├── map_utils.py     # Utilidades mapas
│   └── data_cleaning.py # Limpieza datos
└── .streamlit/          # Configuración Streamlit
    └── secrets.toml     # Secretos (no versionado)
```

## 📊 Archivos de Datos Esperados

### Módulo Empleo
- `df_postulantes_empleo.parquet`
- `df_inscriptos_empleo.parquet`
- `df_empresas.parquet`
- `capa_departamentos_2010.geojson`

### Módulo CBA Me Capacita
- `df_postulantes_cbamecapacita.parquet`
- `df_alumnos.parquet`
- `df_cursos.parquet`

### Módulo Banco de la Gente
- `df_global_banco.parquet`
- `df_global_pagados.parquet`

## 🔧 Desarrollo

### Modo Desarrollo
```bash
# Activar modo desarrollo
export STREAMLIT_SERVER_RUN_ON_SAVE=true

# Limpiar caché
# Usar el botón en la sidebar o:
streamlit cache clear
```

### Debugging
```bash
# Ver logs detallados
streamlit run app.py --logger.level=debug

# Profiling de memoria
pip install memory_profiler
python -m memory_profiler app.py
```

### Testing
```bash
# Instalar dependencias de testing
pip install pytest streamlit-testing

# Ejecutar tests
pytest tests/
```

## 🚨 Troubleshooting

### Problemas Comunes

1. **Errores de instalación: "Cannot import setuptools.build_meta", "numpy.core.multiarray", etc.**
   ```bash
   # Solución RÁPIDA - Ejecutar el instalador automático:
   install_windows.bat
   
   # Solución MANUAL completa:
   # 1. Eliminar entorno virtual
   rmdir /s venv
   
   # 2. Crear nuevo entorno limpio
   python -m venv venv
   venv\Scripts\activate
   
   # 3. Actualizar herramientas de construcción PRIMERO
   python -m pip install --upgrade pip setuptools wheel
   
   # 4. Usar wheels precompilados (evita compilación)
   pip install "numpy>=1.21.0,<2.0.0" --only-binary=all
   pip install --only-binary=all "shapely>=1.8.0" "fiona>=1.8.0" "pyproj>=3.3.0"
   pip install "geopandas>=0.12.0" --only-binary=all
   pip install -r requirements.txt
   
   # Alternativa CONDA (más estable para Windows):
   conda create -n tablero python=3.9
   conda activate tablero
   conda install geopandas numpy pandas
   pip install streamlit plotly sentry-sdk requests folium
   ```

2. **Error "Token de GitLab no configurado"**
   - Verificar que `secrets.toml` existe y tiene el token correcto
   - Token debe tener permisos `read_repository`

3. **Alto uso de memoria**
   - Verificar que la optimización lazy loading está activa
   - Limpiar caché: botón en sidebar o `st.cache_data.clear()`

4. **Archivos no encontrados**
   - Verificar que los archivos existen en la fuente configurada
   - Revisar logs en el expander "Debug" de cada módulo

5. **Error de conexión MinIO/GitLab**
   - Verificar conectividad de red
   - Validar credenciales en `secrets.toml`

6. **Errores de instalación en Windows**
   ```bash
   # Si pip falla, usar conda:
   conda create -n tablero python=3.9
   conda activate tablero
   conda install geopandas streamlit plotly
   pip install -r requirements.txt --no-deps
   ```

### Logs y Monitoreo

- **Sentry**: Errores automáticamente reportados si está configurado
- **Streamlit logs**: Visible en consola durante desarrollo
- **Debug info**: Expandir secciones de debug en cada módulo

## 🤝 Contribución

1. Fork del repositorio
2. Crear branch feature: `git checkout -b feature/nueva-funcionalidad`
3. Commit cambios: `git commit -am 'Añadir nueva funcionalidad'`
4. Push branch: `git push origin feature/nueva-funcionalidad`
5. Crear Pull Request

## 📝 Licencia

[Especificar licencia del proyecto]

## 📞 Soporte

- **Issues**: Crear issue en el repositorio
- **Documentación técnica**: Ver `manual-desarrollo.md`
- **Contacto**: [Especificar contacto del equipo]

---

## 🔄 Optimizaciones Recientes

### Carga Lazy por Módulo
- ✅ TTL caché reducido: 3600s → 1800s
- ✅ Límites de caché: max_entries=5-10
- ✅ Carga bajo demanda por pestaña
- 📈 **Mejora esperada**: -40% RAM, -20% CPU

### Próximas Optimizaciones
- [ ] Backoff exponencial en reintentos GitLab
- [ ] Optimización conversión tipos NumPy
- [ ] Reducción sample rates Sentry
- [ ] Cache distribuido con Redis
