# Stage 1: Builder
FROM python:3.9-slim as builder

# Argumentos de build
ARG GITHUB_TOKEN
ARG REPO_URL
ARG BRANCH=main

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

# Clona el repositorio privado
RUN git clone -b ${BRANCH} https://${GITHUB_TOKEN}@github.com/${REPO_URL##*/} /app

WORKDIR /app

# Instala dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Runtime
FROM python:3.9-slim
WORKDIR /app

# Copia desde el builder
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /usr/lib /usr/lib
COPY --from=builder /app /app

# Configuración final
RUN mkdir -p /app/streamlit && \
    chmod +x /app/entrypoint.sh

EXPOSE 8501

ENTRYPOINT ["/app/entrypoint.sh"]
