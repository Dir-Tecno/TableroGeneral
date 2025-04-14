FROM python:3.9

# 1. Instalar dependencias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Variables para BUILD (solo clonación)
ARG GITHUB_BUILD_TOKEN  # Token solo para clonar
ARG GITHUB_REPO
ARG GITHUB_BRANCH=main

# 3. Clonar repo (usando token de build)
RUN mkdir -p /app && \
    git clone -b ${GITHUB_BRANCH} \
    https://${GITHUB_BUILD_TOKEN}@github.com/${GITHUB_REPO}.git /app || \
    { echo "ERROR: Fallo al clonar repositorio"; exit 1; }

WORKDIR /app

# 4. Crear entrypoint para RUNTIME
RUN echo '#!/bin/sh\n\
mkdir -p /app/.streamlit\n\
cat > /app/.streamlit/secrets.toml <<EOF\n\
[slack]\n\
webhook_url = "${SLACK_WEBHOOK_URL}"\n\
\n\
[github]\n\
token = "${GITHUB_RUNTIME_TOKEN}"\n\
EOF\n\
exec "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# 5. Variables para RUNTIME (app)
ENV SLACK_WEBHOOK_URL=""
ENV GITHUB_RUNTIME_TOKEN=""  # Diferente del build token

# 6. Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# 7. Configuración final
EXPOSE 8501
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["streamlit", "run", "app.py"]
