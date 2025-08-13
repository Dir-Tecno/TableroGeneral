"""
Utilidades para integración con DuckDB en el proyecto TableroGeneral.
Proporciona funciones helper para migrar operaciones de pandas a DuckDB.
"""

import duckdb
import pandas as pd
import streamlit as st
from typing import Dict, Any, Optional, Union
import logging

# Configurar logging
logger = logging.getLogger(__name__)

class DuckDBProcessor:
    """
    Clase para manejar operaciones DuckDB de manera eficiente y reutilizable.
    """
    
    def __init__(self, connection: Optional[duckdb.DuckDBPyConnection] = None):
        """
        Inicializa el procesador DuckDB.
        
        Args:
            connection: Conexión DuckDB existente. Si es None, crea una nueva.
        """
        self.conn = connection or duckdb.connect()
        self._registered_tables = set()
    
    def register_dataframe(self, name: str, df: pd.DataFrame) -> None:
        """
        Registra un DataFrame como tabla temporal en DuckDB.
        
        Args:
            name: Nombre de la tabla en DuckDB
            df: DataFrame de pandas a registrar
        """
        if df is not None and not df.empty:
            self.conn.register(name, df)
            self._registered_tables.add(name)
            logger.info(f"Tabla '{name}' registrada con {len(df)} filas")
        else:
            logger.warning(f"DataFrame '{name}' está vacío o es None")
    
    def execute_query(self, query: str, return_df: bool = True) -> Union[pd.DataFrame, Any]:
        """
        Ejecuta una consulta SQL en DuckDB.
        
        Args:
            query: Consulta SQL a ejecutar
            return_df: Si True, retorna un DataFrame de pandas
            
        Returns:
            DataFrame de pandas o resultado DuckDB según return_df
        """
        try:
            result = self.conn.execute(query)
            if return_df:
                return result.df()
            return result
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {e}")
            logger.error(f"Consulta: {query}")
            raise
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        Obtiene información sobre una tabla registrada.
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            DataFrame con información de columnas
        """
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query)
    
    def close(self) -> None:
        """Cierra la conexión DuckDB."""
        if self.conn:
            self.conn.close()
            logger.info("Conexión DuckDB cerrada")

def create_duckdb_processor(data: Dict[str, pd.DataFrame], table_mapping: Dict[str, str]) -> DuckDBProcessor:
    """
    Crea un procesador DuckDB y registra los DataFrames necesarios.
    
    Args:
        data: Diccionario con DataFrames cargados
        table_mapping: Mapeo de nombres de archivo a nombres de tabla DuckDB
        
    Returns:
        Procesador DuckDB configurado
    """
    processor = DuckDBProcessor()
    
    for file_name, table_name in table_mapping.items():
        df = data.get(file_name)
        if df is not None:
            processor.register_dataframe(table_name, df)
        else:
            logger.warning(f"No se encontró el archivo '{file_name}' en los datos")
    
    return processor

# Consultas SQL predefinidas para operaciones comunes
COMMON_QUERIES = {
    'departamentos_validos': """
        CASE 
            WHEN N_DEPARTAMENTO IN (
                'CAPITAL', 'CALAMUCHITA', 'COLON', 'CRUZ DEL EJE', 'GENERAL ROCA', 
                'GENERAL SAN MARTIN', 'ISCHILIN', 'JUAREZ CELMAN', 'MARCOS JUAREZ', 
                'MINAS', 'POCHO', 'PRESIDENTE ROQUE SAENZ PEÑA', 'PUNILLA', 
                'RIO CUARTO', 'RIO PRIMERO', 'RIO SECO', 'RIO SEGUNDO', 
                'SAN ALBERTO', 'SAN JAVIER', 'SAN JUSTO', 'SANTA MARIA', 
                'SOBREMONTE', 'TERCERO ARRIBA', 'TOTORAL', 'TULUMBA', 'UNION'
            ) THEN N_DEPARTAMENTO 
            ELSE 'OTROS' 
        END as N_DEPARTAMENTO_NORM
    """,
    
    'zonas_favorecidas': """
        CASE 
            WHEN N_DEPARTAMENTO IN (
                'PRESIDENTE ROQUE SAENZ PEÑA', 'GENERAL ROCA', 'RIO SECO', 'TULUMBA', 
                'POCHO', 'SAN JAVIER', 'SAN ALBERTO', 'MINAS', 'CRUZ DEL EJE', 
                'TOTORAL', 'SOBREMONTE', 'ISCHILIN'
            ) THEN 'ZONA NOC Y SUR'
            ELSE 'ZONA REGULAR'
        END as ZONA
    """,
    
    'corregir_capital': """
        CASE 
            WHEN N_DEPARTAMENTO = 'CAPITAL' THEN 'CORDOBA'
            ELSE N_LOCALIDAD
        END as N_LOCALIDAD_CORR
    """
}

def get_performance_stats(processor: DuckDBProcessor, query: str) -> Dict[str, Any]:
    """
    Obtiene estadísticas de rendimiento de una consulta.
    
    Args:
        processor: Procesador DuckDB
        query: Consulta SQL
        
    Returns:
        Diccionario con estadísticas de rendimiento
    """
    import time
    
    start_time = time.time()
    result = processor.execute_query(query)
    end_time = time.time()
    
    return {
        'execution_time': end_time - start_time,
        'rows_returned': len(result) if isinstance(result, pd.DataFrame) else 0,
        'memory_usage': result.memory_usage(deep=True).sum() if isinstance(result, pd.DataFrame) else 0
    }

@st.cache_data(ttl=3600)
def cached_duckdb_query(query: str, data_hash: str) -> pd.DataFrame:
    """
    Ejecuta una consulta DuckDB con cache de Streamlit.
    
    Args:
        query: Consulta SQL
        data_hash: Hash de los datos para invalidar cache
        
    Returns:
        DataFrame resultado
    """
    # Esta función se implementará específicamente para cada módulo
    # ya que necesita acceso a los datos registrados
    pass
