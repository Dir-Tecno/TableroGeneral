FROM python:3.9

# 1. Instalar dependencias esenciales con manejo de errores
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Configuración de Git (sin usar variables todavía)
RUN git config --global http.sslVerify true && \
    git config --global url."https://github.com".insteadOf "git@github.com:"

# 3. Etapa de clonación (usando BuildKit para secrets)
RUN --mount=type=secret,id=github_token \
    --mount=type=secret,id=github_repo \
    --mount=type=secret,id=github_branch \
    mkdir -p /app && \
    REPO_URL="https://$(cat /run/secrets/github_token)@github.com/$(cat /run/secrets/github_repo).git" && \
    git clone -b "$(cat /run/secrets/github_branch)" "$REPO_URL" /app || \
    { echo "ERROR: Fallo al clonar repositorio"; ls -la /run/secrets/; exit 1; }

WORKDIR /app

# 4. Instalación de dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# 5. Configuración final
EXPOSE 8501
CMD ["streamlit", "run", "app_principal.py"]
