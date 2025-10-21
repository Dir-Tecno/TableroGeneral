#!/usr/bin/env python3
"""
Script para generar datos de ejemplo para TableroGeneral
Ejecutar: python create_sample_data.py
"""

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
import os
from datetime import datetime, timedelta
import random

def create_sample_data():
    """Crea archivos de datos de ejemplo para todos los módulos"""
    
    # Crear directorio data si no existe
    os.makedirs('data', exist_ok=True)
    print("📁 Creando directorio 'data'...")
    
    # 1. MÓDULO EMPLEO
    print("👥 Generando datos de Empleo...")
    
    # df_postulantes_empleo.parquet
    n_postulantes = 1000
    postulantes_empleo = pd.DataFrame({
        'id_postulante': range(1, n_postulantes + 1),
        'nombre': [f'Postulante_{i}' for i in range(1, n_postulantes + 1)],
        'edad': np.random.randint(18, 65, n_postulantes),
        'genero': np.random.choice(['M', 'F'], n_postulantes),
        'departamento': np.random.choice(['Córdoba', 'Río Cuarto', 'San Justo', 'Marcos Juárez', 'Unión'], n_postulantes),
        'programa': np.random.choice(['EMPLEO +26', 'PPP - PROGRAMA PRIMER PASO [2024]'], n_postulantes),
        'estado': np.random.choice(['Validado', 'Pendiente', 'Rechazado'], n_postulantes, p=[0.6, 0.3, 0.1]),
        'fecha_postulacion': pd.date_range(start='2024-01-01', end='2024-12-31', periods=n_postulantes),
        'telefono': [f'351{random.randint(1000000, 9999999)}' for _ in range(n_postulantes)],
        'email': [f'postulante{i}@email.com' for i in range(1, n_postulantes + 1)]
    })
    postulantes_empleo.to_parquet('data/df_postulantes_empleo.parquet', index=False)
    
    # df_inscriptos_empleo.parquet
    n_inscriptos = 600
    inscriptos_empleo = pd.DataFrame({
        'id_inscripto': range(1, n_inscriptos + 1),
        'id_postulante': np.random.choice(postulantes_empleo['id_postulante'], n_inscriptos),
        'empresa': [f'Empresa_{i}' for i in range(1, n_inscriptos + 1)],
        'puesto': np.random.choice(['Administrativo', 'Vendedor', 'Operario', 'Técnico', 'Auxiliar'], n_inscriptos),
        'salario': np.random.randint(50000, 150000, n_inscriptos),
        'fecha_inscripcion': pd.date_range(start='2024-02-01', end='2024-12-31', periods=n_inscriptos),
        'estado_inscripcion': np.random.choice(['Activo', 'Finalizado', 'Suspendido'], n_inscriptos, p=[0.7, 0.2, 0.1])
    })
    inscriptos_empleo.to_parquet('data/df_inscriptos_empleo.parquet', index=False)
    
    # df_empresas.parquet
    n_empresas = 200
    empresas = pd.DataFrame({
        'id_empresa': range(1, n_empresas + 1),
        'razon_social': [f'Empresa S.A. {i}' for i in range(1, n_empresas + 1)],
        'cuit': [f'20{random.randint(10000000, 99999999)}1' for _ in range(n_empresas)],
        'departamento': np.random.choice(['Córdoba', 'Río Cuarto', 'San Justo', 'Marcos Juárez', 'Unión'], n_empresas),
        'cantidad_empleados': np.random.randint(1, 500, n_empresas),
        'sector': np.random.choice(['Comercio', 'Industria', 'Servicios', 'Construcción', 'Agro'], n_empresas),
        'adherido': np.random.choice(['EMPLEO +26', 'PPP - PROGRAMA PRIMER PASO [2024]'], n_empresas),
        'fecha_adhesion': pd.date_range(start='2023-01-01', end='2024-12-31', periods=n_empresas),
        'empleador': np.random.choice(['S', 'N'], n_empresas, p=[0.8, 0.2])
    })
    empresas.to_parquet('data/df_empresas.parquet', index=False)
    
    # 2. MÓDULO CBA ME CAPACITA
    print("🎓 Generando datos de CBA Me Capacita...")
    
    # df_postulantes_cbamecapacita.parquet
    n_postulantes_cba = 800
    postulantes_cba = pd.DataFrame({
        'id_postulante': range(1, n_postulantes_cba + 1),
        'nombre': [f'Estudiante_{i}' for i in range(1, n_postulantes_cba + 1)],
        'edad': np.random.randint(16, 60, n_postulantes_cba),
        'genero': np.random.choice(['M', 'F'], n_postulantes_cba),
        'departamento': np.random.choice(['Córdoba', 'Río Cuarto', 'San Justo', 'Marcos Juárez', 'Unión'], n_postulantes_cba),
        'nivel_educativo': np.random.choice(['Primario', 'Secundario', 'Terciario', 'Universitario'], n_postulantes_cba),
        'estado': np.random.choice(['Inscripto', 'En Curso', 'Finalizado', 'Abandonó'], n_postulantes_cba, p=[0.3, 0.4, 0.2, 0.1]),
        'fecha_postulacion': pd.date_range(start='2024-01-01', end='2024-12-31', periods=n_postulantes_cba)
    })
    postulantes_cba.to_parquet('data/df_postulantes_cbamecapacita.parquet', index=False)
    
    # df_alumnos.parquet
    n_alumnos = 500
    alumnos = pd.DataFrame({
        'id_alumno': range(1, n_alumnos + 1),
        'id_postulante': np.random.choice(postulantes_cba['id_postulante'], n_alumnos),
        'id_curso': np.random.randint(1, 51, n_alumnos),
        'fecha_inicio': pd.date_range(start='2024-03-01', end='2024-11-30', periods=n_alumnos),
        'fecha_fin': pd.date_range(start='2024-06-01', end='2024-12-31', periods=n_alumnos),
        'calificacion': np.random.randint(1, 11, n_alumnos),
        'asistencia': np.random.randint(60, 101, n_alumnos),
        'certificado': np.random.choice(['Sí', 'No'], n_alumnos, p=[0.8, 0.2])
    })
    alumnos.to_parquet('data/df_alumnos.parquet', index=False)
    
    # df_cursos.parquet
    n_cursos = 50
    cursos = pd.DataFrame({
        'id_curso': range(1, n_cursos + 1),
        'nombre_curso': [f'Curso de {skill}' for skill in np.random.choice([
            'Programación', 'Diseño Gráfico', 'Marketing Digital', 'Contabilidad', 
            'Electricidad', 'Plomería', 'Cocina', 'Peluquería', 'Mecánica', 'Soldadura'
        ], n_cursos)],
        'modalidad': np.random.choice(['Presencial', 'Virtual', 'Semi-presencial'], n_cursos),
        'duracion_horas': np.random.randint(20, 200, n_cursos),
        'cupo_maximo': np.random.randint(15, 50, n_cursos),
        'instructor': [f'Instructor_{i}' for i in range(1, n_cursos + 1)],
        'fecha_inicio': pd.date_range(start='2024-01-01', end='2024-12-31', periods=n_cursos),
        'departamento': np.random.choice(['Córdoba', 'Río Cuarto', 'San Justo', 'Marcos Juárez', 'Unión'], n_cursos)
    })
    cursos.to_parquet('data/df_cursos.parquet', index=False)
    
    # 3. MÓDULO BANCO DE LA GENTE
    print("🏦 Generando datos de Banco de la Gente...")
    
    # df_global_banco.parquet
    n_creditos = 1200
    banco = pd.DataFrame({
        'id_credito': range(1, n_creditos + 1),
        'beneficiario': [f'Beneficiario_{i}' for i in range(1, n_creditos + 1)],
        'dni': [random.randint(10000000, 45000000) for _ in range(n_creditos)],
        'monto_solicitado': np.random.randint(50000, 500000, n_creditos),
        'monto_aprobado': np.random.randint(30000, 500000, n_creditos),
        'departamento': np.random.choice(['Córdoba', 'Río Cuarto', 'San Justo', 'Marcos Juárez', 'Unión'], n_creditos),
        'localidad': [f'Localidad_{i}' for i in range(1, n_creditos + 1)],
        'rubro': np.random.choice(['Comercio', 'Servicios', 'Producción', 'Gastronomía', 'Textil'], n_creditos),
        'estado': np.random.choice(['Aprobado', 'En Evaluación', 'Rechazado', 'Pagado'], n_creditos, p=[0.4, 0.2, 0.1, 0.3]),
        'fecha_solicitud': pd.date_range(start='2023-01-01', end='2024-12-31', periods=n_creditos),
        'genero': np.random.choice(['M', 'F'], n_creditos),
        'edad': np.random.randint(18, 70, n_creditos)
    })
    banco.to_parquet('data/df_global_banco.parquet', index=False)
    
    # df_global_pagados.parquet
    n_pagados = 400
    pagados = pd.DataFrame({
        'id_pago': range(1, n_pagados + 1),
        'id_credito': np.random.choice(banco[banco['estado'] == 'Pagado']['id_credito'], n_pagados),
        'monto_pagado': np.random.randint(30000, 500000, n_pagados),
        'fecha_pago': pd.date_range(start='2024-01-01', end='2024-12-31', periods=n_pagados),
        'cuotas_pagadas': np.random.randint(1, 24, n_pagados),
        'cuotas_totales': np.random.randint(6, 24, n_pagados),
        'interes': np.random.uniform(0, 15, n_pagados).round(2),
        'estado_pago': np.random.choice(['Al Día', 'Mora', 'Finalizado'], n_pagados, p=[0.6, 0.2, 0.2])
    })
    pagados.to_parquet('data/df_global_pagados.parquet', index=False)
    
    # 4. ARCHIVO GEOJSON (Mapa de departamentos)
    print("🗺️ Generando archivo GeoJSON...")
    
    # Crear polígonos simples para departamentos de Córdoba
    departamentos_geom = []
    departamentos_names = ['Córdoba', 'Río Cuarto', 'San Justo', 'Marcos Juárez', 'Unión']
    
    for i, dept in enumerate(departamentos_names):
        # Crear un polígono simple (cuadrado) para cada departamento
        x_base = -64 + i * 0.5
        y_base = -32 + i * 0.3
        
        polygon = Polygon([
            (x_base, y_base),
            (x_base + 0.4, y_base),
            (x_base + 0.4, y_base + 0.4),
            (x_base, y_base + 0.4),
            (x_base, y_base)
        ])
        
        departamentos_geom.append({
            'geometry': polygon,
            'departamento': dept,
            'codigo': f'14{str(i+1).zfill(3)}',
            'poblacion': random.randint(50000, 1500000),
            'superficie_km2': random.randint(1000, 25000)
        })
    
    gdf = gpd.GeoDataFrame(departamentos_geom)
    gdf.to_file('data/capa_departamentos_2010.geojson', driver='GeoJSON')
    
    print("\n✅ ¡Datos de ejemplo creados exitosamente!")
    print("\n📊 Archivos generados:")
    print("  📁 data/")
    print("    ├── df_postulantes_empleo.parquet")
    print("    ├── df_inscriptos_empleo.parquet") 
    print("    ├── df_empresas.parquet")
    print("    ├── df_postulantes_cbamecapacita.parquet")
    print("    ├── df_alumnos.parquet")
    print("    ├── df_cursos.parquet")
    print("    ├── df_global_banco.parquet")
    print("    ├── df_global_pagados.parquet")
    print("    └── capa_departamentos_2010.geojson")
    
    print(f"\n📈 Estadísticas:")
    print(f"  • {n_postulantes:,} postulantes de empleo")
    print(f"  • {n_inscriptos:,} inscriptos en programas")
    print(f"  • {n_empresas:,} empresas registradas")
    print(f"  • {n_postulantes_cba:,} postulantes CBA Me Capacita")
    print(f"  • {n_alumnos:,} alumnos en cursos")
    print(f"  • {n_cursos:,} cursos disponibles")
    print(f"  • {n_creditos:,} créditos del Banco de la Gente")
    print(f"  • {n_pagados:,} créditos con pagos registrados")
    print(f"  • {len(departamentos_names)} departamentos georreferenciados")
    
    print("\n🚀 Ahora puedes ejecutar:")
    print("   streamlit run app.py")
    print("\n💡 Configura FUENTE_DATOS='local' y LOCAL_PATH='./data' en secrets.toml")

if __name__ == "__main__":
    try:
        create_sample_data()
    except ImportError as e:
        print(f"❌ Error: Falta instalar dependencia: {e}")
        print("💡 Ejecuta: pip install geopandas shapely")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
