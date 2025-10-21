"""
Script de prueba para verificar que la carga optimizada funciona correctamente
"""
import os
from moduls.carga import load_data_from_local

# Configuración
LOCAL_PATH = r"D:\Github\DirTecno-Proyects\df_ministerio"

modules = {
    'bco_gente': ['df_global_banco.parquet', 'df_global_pagados.parquet'],
    'cba_capacita': ['df_postulantes_cbamecapacita.parquet','df_alumnos.parquet', 'df_cursos.parquet'],
    'empleo': ['df_postulantes_empleo.parquet','df_inscriptos_empleo.parquet', 'df_empresas.parquet','capa_departamentos_2010.geojson'],
}

# Probar carga de un módulo
print("=" * 80)
print("PROBANDO CARGA OPTIMIZADA")
print("=" * 80)
print()

for module_key in ['empleo', 'bco_gente', 'cba_capacita']:
    print(f"Módulo: {module_key}")
    print("-" * 80)

    temp_modules = {module_key: modules[module_key]}

    try:
        all_data, all_dates, logs = load_data_from_local(LOCAL_PATH, temp_modules)

        print(f"Archivos cargados: {len(all_data)}")

        for nombre, df in all_data.items():
            if hasattr(df, 'shape'):
                print(f"  {nombre}:")
                print(f"    Shape: {df.shape}")
                print(f"    Columnas: {list(df.columns)[:5]}..." if len(df.columns) > 5 else f"    Columnas: {list(df.columns)}")
                print(f"    Memoria: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")

        if logs.get("warnings"):
            print(f"  Warnings: {logs['warnings']}")
        if logs.get("info"):
            print(f"  Info: {logs['info']}")

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()

    print()

print("=" * 80)
print("FIN DE PRUEBA")
print("=" * 80)
