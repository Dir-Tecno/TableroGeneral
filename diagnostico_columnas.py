"""
Script de diagnóstico para verificar columnas en archivos parquet
"""
import pandas as pd
import pyarrow.parquet as pq
import requests
import io

# Configuración GitLab
REPO_ID = "Dir-Tecno/df_ministerio"
BRANCH = "main"
TOKEN = "glpat-aTvm561h3yzN4Qzg1sTqz286MQp1OmRncW4wCw.01.1218hkwzn"

def obtener_archivo_gitlab(archivo):
    """Obtiene un archivo desde GitLab"""
    url = f"https://gitlab.com/api/v4/projects/{REPO_ID.replace('/', '%2F')}/repository/files/{archivo.replace('/', '%2F')}/raw"
    headers = {"PRIVATE-TOKEN": TOKEN}
    params = {"ref": BRANCH}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.content
    return None

# Archivos a verificar
archivos = [
    'df_postulantes_empleo.parquet',
    'df_inscriptos_empleo.parquet',
    'df_empresas.parquet',
    'df_global_banco.parquet',
    'df_global_pagados.parquet',
    'df_postulantes_cbamecapacita.parquet',
    'df_alumnos.parquet',
    'df_cursos.parquet'
]

print("=" * 80)
print("DIAGNÓSTICO DE COLUMNAS EN ARCHIVOS PARQUET")
print("=" * 80)
print()

from moduls.carga_optimized import COLUMNAS_NECESARIAS

for archivo in archivos:
    print(f"\n{archivo}")
    print("-" * 80)

    try:
        contenido = obtener_archivo_gitlab(archivo)
        if contenido:
            # Leer esquema del parquet
            table = pq.read_table(io.BytesIO(contenido))
            columnas_reales = table.column_names

            print(f"Columnas reales ({len(columnas_reales)}):")
            print(f"  {', '.join(sorted(columnas_reales)[:10])}...")

            # Verificar columnas solicitadas
            columnas_solicitadas = COLUMNAS_NECESARIAS.get(archivo, None)

            if columnas_solicitadas:
                print(f"\nColumnas solicitadas ({len(columnas_solicitadas)}):")
                print(f"  {', '.join(sorted(columnas_solicitadas)[:10])}...")

                # Verificar cuáles faltan
                faltantes = set(columnas_solicitadas) - set(columnas_reales)
                if faltantes:
                    print(f"\n⚠️  COLUMNAS FALTANTES ({len(faltantes)}):")
                    for col in sorted(faltantes):
                        print(f"    - {col}")
                else:
                    print("\n✓ Todas las columnas solicitadas existen")
            else:
                print("\nNo hay filtro de columnas (se cargan todas)")
        else:
            print("ERROR: No se pudo descargar el archivo")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "=" * 80)
print("FIN DEL DIAGNÓSTICO")
print("=" * 80)
