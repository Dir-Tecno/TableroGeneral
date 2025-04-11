FROM python:3.9-slim

# 1. Configurar fuentes de paquetes correctas y prioridades
RUN echo "deb http://deb.debian.org/debian buster main" > /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian buster-updates main" >> /etc/apt/sources.list && \
    echo "deb http://security.debian.org/debian-security buster/updates main" >> /etc/apt/sources.list

# 2. Actualizar índices con reintentos
RUN apt-get update -o Acquire::Retries=3 --fix-missing

# 3. Instalar dependencias esenciales primero
RUN apt-get install -y --no-install-recommends \
    zlib1g-dev \
    libjpeg-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. Instalar las librerías problemáticas con dependencias explícitas
RUN apt-get install -y --no-install-recommends \
    libfreetype6-dev=2.9.1-3+deb10u3 \
    libpng-dev=1.6.36-6 \
    pkg-config=0.29-6 \
    && rm -rf /var/lib/apt/lists/*

# 5. Continuar con el resto de las instalaciones
RUN apt-get install -y --no-install-recommends \
    git \
    gcc \
    g++ \
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
