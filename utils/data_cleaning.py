import pandas as pd

def clean_thousand_separator(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia el separador de miles (",") en todas las columnas tipo string que parezcan numéricas y convierte a tipo numérico.
    Devuelve el DataFrame modificado (inplace).
    """
    if df is not None:
        for col in df.columns:
            if df[col].dtype == object:
                # Si hay string con ',' y parecen números, limpiar
                if df[col].str.contains(r'^-?\d{1,3}(,\d{3})*(\.\d+)?$', na=False).any():
                    df[col] = df[col].str.replace(',', '', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

def convert_decimal_separator(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    """
    Convierte separadores decimales de coma a punto en columnas específicas o todas.
    Reemplaza las comas por puntos en valores que parecen decimales,
    siguiendo el enfoque de bco_gente.py.
    
    Args:
        df: DataFrame a procesar
        columns: Lista opcional de columnas a procesar (None = todas)
        
    Returns:
        DataFrame procesado
    """
    if df is None:
        return None
        
    cols_to_process = columns if columns else df.columns
    
    for col in cols_to_process:
        if col in df.columns and df[col].dtype == object:
            # Reemplazar comas por puntos en valores que podrían ser decimales
            df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
            # Intentar convertir a numérico
            df[col] = pd.to_numeric(df[col], errors='ignore')
    
    return df
