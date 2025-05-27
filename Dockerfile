FROM python:3.9

# Configurar variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_HEADLESS=true \
    DEBIAN_FRONTEND=noninteractive

# Instalar dependencias en una sola capa
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
        build-essential \
        libgeos-dev \
        libproj-dev \
        gdal-bin && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /app

# Instalar dependencias Python primero
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de la aplicación
COPY . /app/

# Configurar secrets
RUN echo '#!/bin/sh\n\
mkdir -p /app/.streamlit && \
cat > /app/.streamlit/secrets.toml <<-"EOF"
[gitlab]\token="${GITLAB_TOKEN}"\[slack]\webhook_url="${SLACK_WEBHOOK_URL}"\EOF\
exec "$@"' > /app/entrypoint.sh && \
chmod +x /app/entrypoint.sh

# Variables de entorno runtime
ENV GITLAB_TOKEN="" \
    SLACK_WEBHOOK_URL=""

EXPOSE 8501
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]