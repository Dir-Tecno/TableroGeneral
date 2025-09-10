FROM python:3.12

# Configurar variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    DEBIAN_FRONTEND=noninteractive

# Instalar dependencias del sistema (sin libatlas-base-dev ni python3-gdal)
RUN apt-get update && apt-get install -y \
    git \
    ca-certificates \
    build-essential \
    libgeos-dev \
    libproj-dev \
    gdal-bin \
    gfortran \
    pkg-config \
    python3-dev \
    libopenblas-dev \
    liblapack-dev \
    && rm -rf /var/lib/apt/lists/*

# Variables de entorno para GDAL
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Crear directorio de la aplicación
WORKDIR /app

# Copiar solo requirements.txt primero para aprovechar el caché
COPY requirements.txt /app/

# ACTUALIZAR PIP Y LUEGO INSTALAR LAS DEPENDENCIAS CON MÁS VERBOSIDAD
RUN pip install --upgrade pip

# Instalar dependencias geoespaciales desde un índice de paquetes compatible
RUN pip install fiona pyproj rasterio shapely geopandas --find-links https://fiona.rivr.nl/wheelhouse/

# Instalar el resto de las dependencias
RUN pip install --no-cache-dir -v -r requirements.txt

# Copiar el resto de la aplicación
COPY . /app/

# Crear entrypoint para configuración en tiempo de ejecución
RUN echo '#!/bin/sh\n\
mkdir -p /app/.streamlit\n\
cat > /app/.streamlit/secrets.toml <<EOF\n\
[gitlab]\n\
token = "${GITLAB_TOKEN}"\n\
\n\
[slack]\n\
webhook_url = "${SLACK_WEBHOOK_URL}"\n\
\n\
[sentry]\n\
dsn = "${SENTRY_DSN}"\n\
EOF\n\
exec "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# Variables de entorno para tiempo de ejecución
ENV GITLAB_TOKEN=""
ENV SLACK_WEBHOOK_URL=""
ENV SENTRY_DSN=""

# Configuración final
EXPOSE 8501
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
