FROM python:3.9

# 1. Instalar git y dependencias básicas
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 1. Crear entrypoint que genera secrets.toml al iniciar
RUN echo '#!/bin/sh\n\
mkdir -p /app/.streamlit\n\
cat > /app/.streamlit/secrets.toml <<EOF\n\
[slack]\n\
webhook_url = "$SLACK_WEBHOOK_URL"\n\
\n\
[github]\n\
token = "$GITHUB_TOKEN"\n\
EOF\n\
exec "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

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

# 2. Variables esperadas (se setean al ejecutar el contenedor)
ENV SLACK_WEBHOOK_URL=""
ENV GITHUB_TOKEN=""

# 4. Configuración final
EXPOSE 8501
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["streamlit", "run", "app.py"]
