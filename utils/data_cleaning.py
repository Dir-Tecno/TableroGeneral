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
