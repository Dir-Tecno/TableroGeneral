FROM python:3.9

# 1. Instalar git y dependencias básicas
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Clonación usando ARG (más fiable que secrets para builds)
ARG GITHUB_TOKEN
ARG GITHUB_REPO
ARG GITHUB_BRANCH=main

RUN mkdir -p /app && \
    git clone -b ${GITHUB_BRANCH} \
    https://${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git /app || \
    { echo "ERROR: Fallo al clonar repositorio"; exit 1; }

WORKDIR /app

# 3. Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# 4. Configuración final
EXPOSE 8501
CMD ["streamlit", "run", "app.py"]
