FROM python:3.9-slim

# 1. Configuración previa para evitar errores de repositorios
RUN echo "Acquire::Check-Valid-Until \"false\";" > /etc/apt/apt.conf.d/10no--check-valid-until && \
    echo "Acquire::Check-Date \"false\";" >> /etc/apt/apt.conf.d/10no--check-valid-until

# 2. Actualización de paquetes con reintentos
RUN apt-get update --fix-missing || apt-get update --fix-missing

# 3. Instalación en bloques con dependencias explícitas
RUN apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get install -y --no-install-recommends \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# 4. Instalación limpia de dependencias restantes
RUN apt-get install -y --no-install-recommends \
    git \
    libgomp1 \
    libspatialindex-dev \
    libarrow-dev \
    libgeos-dev \
    libproj-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*
    
# Argumentos de build (se pasan desde docker-compose)
ARG GITHUB_TOKEN
ARG GITHUB_REPO
ARG GITHUB_BRANCH

# Clona el repositorio privado
RUN git clone -b ${GITHUB_BRANCH} https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git /app

WORKDIR /app

# Instala dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Variables de entorno (se setean desde docker-compose)
ENV SLACK_WEBHOOK_URL=""

EXPOSE 8501

CMD ["streamlit", "run", "app_principal.py", "--server.port=8501", "--server.address=0.0.0.0"]
