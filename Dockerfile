FROM python:3.9

# 1. Instalar dependencias esenciales
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# 2. Variables de construcción (se pasan desde docker-compose)
ARG GITHUB_TOKEN
ARG GITHUB_REPO
ARG GITHUB_BRANCH=main

# 3. Clonación segura del repositorio
RUN mkdir -p /app && \
    git config --global url."https://${GITHUB_TOKEN}@github.com".insteadOf "https://github.com" && \
    git clone -c http.sslVerify=true -b ${GITHUB_BRANCH} https://github.com/${GITHUB_REPO}.git /app || \
    { echo "Error al clonar el repositorio"; exit 1; }

WORKDIR /app

# 4. Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# 5. Configuración final
ENV PYTHONUNBUFFERED=1
EXPOSE 8501
CMD ["streamlit", "run", "app_principal.py"]
