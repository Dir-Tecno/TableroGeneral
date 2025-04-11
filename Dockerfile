FROM python:3.9-slim

# 1. Actualiza primero las listas de paquetes solas
RUN apt-get update || apt-get update

# 2. Instala dependencias en bloques separados con manejo de errores
RUN apt-get install -y --no-install-recommends \
    git \
    libgomp1 \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libfreetype6-dev \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get install -y --no-install-recommends \
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
