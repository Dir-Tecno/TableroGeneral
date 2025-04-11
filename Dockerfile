FROM python:3.9

# 1. Configurar fuentes de paquetes confiables
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian bullseye-updates main" >> /etc/apt/sources.list && \
    echo "deb http://security.debian.org/debian-security bullseye-security main" >> /etc/apt/sources.list

# 2. Actualización robusta del sistema
RUN apt-get update || (rm -rf /var/lib/apt/lists/* && apt-get update)

# 3. Instalación de paquetes esenciales en bloques
RUN apt-get install -y --no-install-recommends \
    ca-certificates \
    apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# 4. Reintentar actualización con certificados instalados
RUN apt-get update -o Acquire::Retries=3 -o Acquire::http::Timeout=30

# 5. Instalar dependencias principales
RUN apt-get install -y --no-install-recommends \
    build-essential \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# 6. Instalar paquetes gráficos
RUN apt-get install -y --no-install-recommends \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Resto de tus instalaciones...
    
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
