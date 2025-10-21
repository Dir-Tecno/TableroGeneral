@echo off
echo ========================================
echo   INSTALADOR TABLERO GENERAL - WINDOWS
echo ========================================
echo.

REM Verificar si Python está instalado
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Python no está instalado o no está en el PATH
    echo Por favor instala Python desde: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo ✅ Python detectado
python --version

echo.
echo 🔍 Verificando compatibilidad de Python...
for /f "tokens=2" %%i in ('python --version') do set PYTHON_VERSION=%%i
echo Versión detectada: %PYTHON_VERSION%

echo %PYTHON_VERSION% | findstr /r "3\.12" >nul
if %errorlevel% equ 0 (
    echo ⚠️  ADVERTENCIA: Python 3.12 detectado - usando versiones compatibles
    set USE_MODERN_VERSIONS=1
) else (
    echo ✅ Versión de Python compatible
    set USE_MODERN_VERSIONS=0
)

echo.
echo 🗑️  Eliminando entorno virtual anterior (si existe)...
if exist venv rmdir /s /q venv

echo.
echo 📦 Creando nuevo entorno virtual...
python -m venv venv
if %errorlevel% neq 0 (
    echo ERROR: No se pudo crear el entorno virtual
    pause
    exit /b 1
)

echo.
echo 🔧 Activando entorno virtual...
call venv\Scripts\activate.bat

echo.
echo ⬆️  Actualizando herramientas de construcción...
python -m pip install --upgrade pip setuptools wheel
if %errorlevel% neq 0 (
    echo ERROR: No se pudieron actualizar las herramientas
    pause
    exit /b 1
)

echo.
echo 🔢 Instalando numpy (usando wheels precompilados)...
if %USE_MODERN_VERSIONS%==1 (
    echo    - Usando numpy compatible con Python 3.12...
    pip install "numpy>=1.26.0" --only-binary=all
) else (
    echo    - Usando numpy estándar...
    pip install "numpy>=1.21.0,<2.0.0" --only-binary=all
)

if %errorlevel% neq 0 (
    echo ADVERTENCIA: Error con numpy, intentando sin restricciones...
    pip install numpy
    if %errorlevel% neq 0 (
        echo ERROR: No se pudo instalar numpy
        echo.
        echo 💡 SUGERENCIA: Considera usar Python 3.11 en lugar de 3.12
        echo    Descarga desde: https://www.python.org/downloads/release/python-3119/
        pause
        exit /b 1
    )
)

echo.
echo 🗺️  Instalando dependencias geoespaciales...
echo    - Instalando dependencias geoespaciales (usando wheels)...
pip install --only-binary=all "shapely>=1.8.0,<3.0.0" "fiona>=1.8.0,<2.0.0" "pyproj>=3.3.0,<4.0.0"
if %errorlevel% neq 0 (
    echo ADVERTENCIA: Error con wheels, intentando instalación normal...
    pip install shapely fiona pyproj
    if %errorlevel% neq 0 (
        echo ERROR: No se pudieron instalar dependencias geoespaciales
        echo.
        echo 💡 SOLUCIÓN ALTERNATIVA CON CONDA:
        echo    1. Instala Miniconda: https://docs.conda.io/en/latest/miniconda.html
        echo    2. Ejecuta: conda create -n tablero python=3.9
        echo    3. Ejecuta: conda activate tablero
        echo    4. Ejecuta: conda install geopandas
        echo    5. Ejecuta: pip install streamlit plotly sentry-sdk
        pause
        exit /b 1
    )
)

echo    - Instalando geopandas...
pip install "geopandas>=0.12.0,<0.15.0" --only-binary=all
if %errorlevel% neq 0 (
    echo ADVERTENCIA: Error con geopandas wheel, intentando instalación normal...
    pip install geopandas
    if %errorlevel% neq 0 (
        echo ERROR: No se pudo instalar geopandas
        pause
        exit /b 1
    )
)

echo.
echo 📋 Instalando resto de dependencias...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo ERROR: No se pudieron instalar todas las dependencias
    echo Revisa el archivo requirements.txt
    pause
    exit /b 1
)

echo.
echo ✅ ¡INSTALACIÓN COMPLETADA!
echo.
echo 📁 Creando directorio .streamlit...
if not exist .streamlit mkdir .streamlit

echo.
echo 📝 Creando archivo de configuración básico...
echo [server] > .streamlit\config.toml
echo port = 8501 >> .streamlit\config.toml
echo address = "localhost" >> .streamlit\config.toml

if not exist .streamlit\secrets.toml (
    echo.
    echo 📄 Creando archivo secrets.toml de ejemplo...
    echo # Configuración para TableroGeneral > .streamlit\secrets.toml
    echo FUENTE_DATOS = "local" >> .streamlit\secrets.toml
    echo LOCAL_PATH = "./data" >> .streamlit\secrets.toml
    echo. >> .streamlit\secrets.toml
    echo # Para usar GitLab, descomenta y configura: >> .streamlit\secrets.toml
    echo # [gitlab] >> .streamlit\secrets.toml
    echo # token = "tu_token_aqui" >> .streamlit\secrets.toml
)

echo.
echo 🎯 PRÓXIMOS PASOS:
echo.
echo 1. Para generar datos de ejemplo:
echo    python create_sample_data.py
echo.
echo 2. Para ejecutar la aplicación:
echo    streamlit run app.py
echo.
echo 3. Para configurar GitLab:
echo    - Edita .streamlit\secrets.toml
echo    - Añade tu token de GitLab
echo.
echo 💡 La aplicación se abrirá en: http://localhost:8501
echo.
pause
