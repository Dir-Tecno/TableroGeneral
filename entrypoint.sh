#!/bin/bash
set -e

echo "Verificando secrets.toml..."
if [ ! -f /app/streamlit/secrets.toml ]; then
    echo "Error: secrets.toml no encontrado!" >&2
    exit 1
fi

echo "Iniciando aplicación Streamlit..."
exec streamlit run tu_app_principal.py \
    --server.port=${STREAMLIT_SERVER_PORT} \
    --server.address=${STREAMLIT_SERVER_ADDRESS}
