#!/usr/bin/env python
"""
Script para analizar qué columnas se usan en cada módulo del dashboard.
Esto ayuda a identificar qué columnas cargar de los archivos Parquet para optimizar RAM.

Uso:
    python analizar_columnas_usadas.py
"""

import re
import os
from pathlib import Path
from collections import defaultdict

# Mapeo de módulos a archivos
MODULE_FILES = {
    'empleo.py': [
        'df_postulantes_empleo.parquet',
        'df_inscriptos_empleo.parquet',
        'df_empresas.parquet',
        'capa_departamentos_2010.geojson'
    ],
    'bco_gente.py': [
        'df_global_banco.parquet',
        'df_global_pagados.parquet'
    ],
    'cbamecapacita.py': [
        'df_postulantes_cbamecapacita.parquet',
        'df_alumnos.parquet',
        'df_cursos.parquet'
    ]
}

# Patrones para encontrar acceso a columnas
COLUMN_PATTERNS = [
    r"df\[['\"]([\w_]+)['\"]\]",  # df['columna']
    r"df\[['\"]([\w_]+)['\"]\]\s*=",  # df['columna'] =
    r"\.groupby\(\s*['\"]([\w_]+)['\"]\s*\)",  # .groupby('columna')
    r"\.groupby\(\s*\[(.*?)\]\s*\)",  # .groupby(['col1', 'col2'])
    r"\.sort_values\(\s*['\"]([\w_]+)['\"]\s*\)",  # .sort_values('columna')
    r"\.sort_values\(\s*\[(.*?)\]\s*\)",  # .sort_values(['col1', 'col2'])
    r"x\s*=\s*['\"]([\w_]+)['\"]",  # x='columna' (en gráficos)
    r"y\s*=\s*['\"]([\w_]+)['\"]",  # y='columna'
    r"color\s*=\s*['\"]([\w_]+)['\"]",  # color='columna'
    r"size\s*=\s*['\"]([\w_]+)['\"]",  # size='columna'
    r"hover_data\s*=\s*\[(.*?)\]",  # hover_data=['col1', 'col2']
    r"\.rename\(columns\s*=\s*\{.*?['\"]([\w_]+)['\"]",  # rename columnas
    r"\.fillna\(\s*\{['\"]([\w_]+)['\"]",  # fillna por columna
    r"\.drop\(\s*['\"]([\w_]+)['\"]\s*\)",  # drop columna
    r"\.drop\(columns\s*=\s*\[(.*?)\]\)",  # drop multiple
]


def extract_columns_from_line(line: str) -> set:
    """Extrae nombres de columnas de una línea de código"""
    columns = set()

    for pattern in COLUMN_PATTERNS:
        matches = re.findall(pattern, line)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0] if match else ""

            if match:
                # Si es una lista de columnas separadas por comas
                if ',' in match:
                    cols = re.findall(r"['\"]([\w_]+)['\"]", match)
                    columns.update(cols)
                else:
                    columns.add(match)

    return columns


def analyze_module(module_path: Path) -> dict:
    """
    Analiza un módulo y extrae las columnas usadas.

    Returns:
        dict: {'columnas': set(), 'archivos_dataframes': list}
    """
    columns = set()
    dataframe_vars = []

    try:
        with open(module_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')

        # Buscar definiciones de DataFrames
        for line in lines:
            # Buscar asignaciones de datos
            if re.search(r"(df|gdf)\s*=\s*data(?:_for_module)?\[", line):
                match = re.search(r"\['(.*?)'\]", line)
                if match:
                    dataframe_vars.append(match.group(1))

        # Extraer columnas usadas
        for line in lines:
            line_columns = extract_columns_from_line(line)
            columns.update(line_columns)

    except Exception as e:
        print(f"Error analizando {module_path}: {e}")

    return {
        'columnas': columns,
        'archivos_dataframes': dataframe_vars
    }


def main():
    """Función principal"""
    print("=" * 80)
    print("ANÁLISIS DE COLUMNAS USADAS POR MÓDULO")
    print("=" * 80)
    print()

    moduls_dir = Path(__file__).parent / 'moduls'
    results = {}

    for module_file, data_files in MODULE_FILES.items():
        module_path = moduls_dir / module_file

        if not module_path.exists():
            print(f"WARNING: Módulo no encontrado: {module_file}")
            continue

        print(f"Analizando: {module_file}")
        print(f"   Archivos de datos: {', '.join(data_files)}")

        analysis = analyze_module(module_path)
        results[module_file] = {
            'data_files': data_files,
            'analysis': analysis
        }

        print(f"   Columnas encontradas: {len(analysis['columnas'])}")

        if analysis['columnas']:
            sorted_cols = sorted(analysis['columnas'])
            print(f"   Lista: {', '.join(sorted_cols[:10])}", end='')
            if len(sorted_cols) > 10:
                print(f" ... (+{len(sorted_cols) - 10} más)")
            else:
                print()
        print()

    # Generar configuración sugerida
    print("=" * 80)
    print("CONFIGURACIÓN SUGERIDA PARA carga_optimized.py")
    print("=" * 80)
    print()
    print("COLUMNAS_NECESARIAS = {")

    for module_file, data in results.items():
        print(f"    # Módulo: {module_file}")
        for data_file in data['data_files']:
            cols = sorted(data['analysis']['columnas'])
            if cols:
                # Filtrar columnas que parecen reales (no variables temporales)
                real_cols = [c for c in cols if len(c) > 1 and c.isupper() or '_' in c]
                if real_cols:
                    print(f"    '{data_file}': [")
                    for col in real_cols:
                        print(f"        '{col}',")
                    print(f"    ],")
                else:
                    print(f"    '{data_file}': None,  # TODO: Revisar manualmente")
            else:
                print(f"    '{data_file}': None,  # No se encontraron columnas - cargar todas")
        print()

    print("}")
    print()

    # Guardar resultados en archivo
    output_file = Path(__file__).parent / 'columnas_analizadas.txt'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("ANÁLISIS DE COLUMNAS USADAS POR MÓDULO\n")
        f.write("=" * 80 + "\n\n")

        for module_file, data in results.items():
            f.write(f"Módulo: {module_file}\n")
            f.write(f"Archivos: {', '.join(data['data_files'])}\n")
            f.write(f"Columnas encontradas:\n")
            for col in sorted(data['analysis']['columnas']):
                f.write(f"  - {col}\n")
            f.write("\n")

    print(f"OK: Resultados guardados en: {output_file}")
    print()
    print("PROXIMOS PASOS:")
    print("   1. Revisa 'columnas_analizadas.txt' para ver todas las columnas")
    print("   2. Copia la configuración sugerida a carga_optimized.py")
    print("   3. Ajusta manualmente si es necesario")
    print("   4. Prueba la carga optimizada")


if __name__ == '__main__':
    main()
