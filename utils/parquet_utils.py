"""
Utilidades para el manejo de archivos parquet, incluyendo deduplicación y optimización.
"""
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime


def deduplicate_parquet(file_path: Path, create_backup=True) -> dict:
    """
    Elimina registros duplicados de un archivo parquet.
    
    Args:
        file_path: Ruta al archivo parquet
        create_backup: Si True, crea una copia de respaldo antes de modificar
        
    Returns:
        dict con estadísticas del proceso:
            - original: número de filas originales
            - deduped: número de filas después de deduplicación
            - removed: número de filas eliminadas
            - status: 'ok' o mensaje de error
    """
    try:
        logging.info(f"Deduplicando archivo: {file_path}")

        # Crear backup si se solicita
        if create_backup:
            backup = file_path.with_suffix(file_path.suffix + '.bak.parquet')
            if not backup.exists():
                logging.info(f"Creando backup: {backup}")
                Path(file_path).replace(backup)
                backup.replace(file_path)
            else:
                logging.info(f"Backup ya existe: {backup}")

        # Leer y deduplicar
        df = pd.read_parquet(file_path)
        original_count = len(df)
        df_dedup = df.drop_duplicates()
        dedup_count = len(df_dedup)
        removed = original_count - dedup_count

        if removed > 0:
            # Solo guardar si realmente se eliminaron duplicados
            df_dedup.to_parquet(file_path, index=False)
            
        return {
            'file': str(file_path),
            'original': original_count,
            'deduped': dedup_count,
            'removed': removed,
            'status': 'ok'
        }

    except Exception as e:
        logging.error(f"Error al deduplicar {file_path}: {e}")
        return {
            'file': str(file_path),
            'original': None,
            'deduped': None, 
            'removed': None,
            'status': f'error: {e}'
        }


def optimize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimiza un DataFrame para reducir uso de memoria:
    - Convierte tipos numéricos al menor tipo posible 
    - Convierte strings a category si hay menos de 50% valores únicos
    - Convierte datetime a fecha eficiente
    """
    result = df.copy()
    
    for col in result.columns:
        # Optimizar tipos numéricos
        if pd.api.types.is_numeric_dtype(result[col]):
            result[col] = pd.to_numeric(result[col], downcast='integer')
        
        # Convertir strings a category si hay pocos valores únicos
        elif pd.api.types.is_string_dtype(result[col]):
            nunique = result[col].nunique()
            if nunique / len(result) < 0.5:  # Menos de 50% valores únicos
                result[col] = result[col].astype('category')
        
        # Optimizar fechas
        elif pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = pd.to_datetime(result[col], utc=True)
    
    return result