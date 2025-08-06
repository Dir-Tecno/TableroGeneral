import pandas as pd

def convert_decimal_separator(df, columns):
    """
    Convierte el separador decimal de coma a punto en las columnas especificadas
    y convierte la columna a tipo numérico.
    """
    for col in columns:
        if col in df.columns:
            # Reemplazar coma por punto y convertir a numérico.
            # Usar errors='coerce' para convertir valores no válidos en NaN,
            # lo cual es la práctica recomendada en lugar de 'ignore'.
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.', regex=False), errors='coerce')
    return df

def clean_thousand_separator(df, columns=None):
    """
    Limpia el separador de miles (punto) de las columnas especificadas
    y las convierte a tipo numérico.
    """
    if columns is None:
        columns = df.select_dtypes(include=['object']).columns

    for col in columns:
        if col in df.columns and df[col].dtype == 'object':
            if df[col].str.contains(r'\.', na=False).any():
                df[col] = df[col].str.replace('.', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')
    return df