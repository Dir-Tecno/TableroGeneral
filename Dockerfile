FROM python:3.9 

# Configurar variables de entorno para Python
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_HEADLESS=true

# 1. Instalar dependencias del sistema
# 1. Instalar dependencias del sistema (separadas en múltiples pasos)
RUN apt-get update && \
    apt-get install -y --no-install-recommends git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y --no-install-recommends libgeos-dev && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y --no-install-recommends libproj-dev && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y --no-install-recommends gdal-bin && \
    rm -rf /var/lib/apt/lists/*

# 2. Crear directorio de la aplicación
WORKDIR /app

# 3. Copiar archivos de la aplicación
COPY . /app/

# 4. Crear entrypoint para configuración en tiempo de ejecución
RUN echo '#!/bin/sh\n\
mkdir -p /app/.streamlit\n\
cat > /app/.streamlit/secrets.toml <<EOF\n\
[gitlab]\n\
token = "${GITLAB_TOKEN}"\n\
\n\
[slack]\n\
webhook_url = "${SLACK_WEBHOOK_URL}"\n\
EOF\n\
exec "$@"' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

# 5. Variables de entorno para tiempo de ejecución
ENV GITLAB_TOKEN=""
ENV SLACK_WEBHOOK_URL=""

# 6. Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# 7. Configuración final
EXPOSE 8501
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]