FROM python:3.9-slim

# Instala dependencias del sistema
RUN apt-get update && apt-get install -y \
    git \
    libgomp1 \
    libspatialindex-dev \
    gcc \
    g++ \
    libfreetype6-dev \
    libpng-dev \
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
