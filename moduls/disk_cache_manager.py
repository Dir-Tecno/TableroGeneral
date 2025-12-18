"""
Gestor de caché en disco para archivos de datos del dashboard.
Descarga archivos solo cuando se necesitan y verifica actualizaciones periódicamente.
"""
import os
import json
import datetime
import time
import hashlib
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple, Any
import pandas as pd
import geopandas as gpd
import requests

# Funciones stub para reemplazar Sentry (removido)
def capture_exception(e=None, extra_data=None): pass
def add_breadcrumb(category=None, message=None, data=None, level=None): pass

# Configuración del caché
CACHE_DIR = Path("./cache_datos")
METADATA_FILE = CACHE_DIR / "metadata.json"
CHECK_INTERVAL = 600  # 10 minutos en segundos


class DiskCacheManager:
    """Gestiona la caché en disco de archivos de datos"""

    def __init__(self):
        """Inicializa el gestor de caché"""
        self.cache_dir = CACHE_DIR
        self.metadata_file = METADATA_FILE
        self.metadata = {}
        self.check_thread = None
        self.stop_checking = threading.Event()

        # Crear directorio de caché si no existe
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Cargar metadata existente
        self._load_metadata()

    def _load_metadata(self):
        """Carga metadata desde archivo JSON"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    self.metadata = json.load(f)
            except Exception as e:
                capture_exception(e, extra_data={"action": "load_metadata"})
                self.metadata = {}
        else:
            self.metadata = {}

    def _save_metadata(self):
        """Guarda metadata a archivo JSON"""
        try:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=2, default=str)
        except Exception as e:
            capture_exception(e, extra_data={"action": "save_metadata"})

    def _get_cache_path(self, filename: str) -> Path:
        """Obtiene la ruta del archivo en caché"""
        return self.cache_dir / filename

    def _get_remote_etag(self, repo_id: str, branch: str, filename: str, token: str) -> Optional[str]:
        """
        Obtiene el ETag o última modificación del archivo remoto sin descargarlo.
        Usa HEAD request para obtener solo los headers.
        """
        try:
            repo_id_encoded = requests.utils.quote(str(repo_id), safe='')
            file_path_encoded = requests.utils.quote(filename, safe='')

            url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/files/{file_path_encoded}'
            headers = {'PRIVATE-TOKEN': token}
            params = {'ref': branch}

            # Usar GET con metadata para obtener info del archivo
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                # Usar el commit_id del último commit como identificador
                return data.get('last_commit_id', data.get('blob_id', ''))
            return None
        except Exception as e:
            add_breadcrumb(
                category="cache",
                message=f"Error al obtener ETag remoto: {filename}",
                data={"error": str(e)}
            )
            return None

    def _get_commit_date(self, repo_id: str, branch: str, filename: str, token: str) -> Optional[datetime.datetime]:
        """
        Obtiene la fecha del último commit de un archivo específico en GitLab.
        """
        try:
            repo_id_encoded = requests.utils.quote(str(repo_id), safe='')

            # Obtener commits para el archivo específico
            url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/commits'
            headers = {'PRIVATE-TOKEN': token}
            params = {'ref': branch, 'path': filename, 'per_page': 1}

            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                commits = response.json()
                if commits:
                    commit_date = commits[0]['committed_date']
                    # Convertir la fecha ISO a datetime
                    fecha_commit = datetime.datetime.fromisoformat(commit_date.replace('Z', '+00:00'))
                    return fecha_commit
            return None
        except Exception as e:
            add_breadcrumb(
                category="cache",
                message=f"Error al obtener fecha de commit: {filename}",
                data={"error": str(e)}
            )
            return None

    def is_cached(self, filename: str) -> bool:
        """Verifica si un archivo existe en caché"""
        cache_path = self._get_cache_path(filename)
        return cache_path.exists() and filename in self.metadata

    def download_and_cache(self, filename: str, repo_id: str, branch: str, token: str) -> Tuple[bool, Optional[str]]:
        """
        Descarga un archivo y lo guarda en caché.
        Si el archivo es un .parquet, aplica deduplicación antes de guardarlo.

        Returns:
            Tuple[bool, Optional[str]]: (éxito, error_mensaje)
        """
        try:
            add_breadcrumb(
                category="cache",
                message=f"Descargando archivo a caché: {filename}",
                data={"repo_id": repo_id, "branch": branch}
            )

            # Descargar archivo
            repo_id_encoded = requests.utils.quote(str(repo_id), safe='')
            file_path_encoded = requests.utils.quote(filename, safe='')

            url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/files/{file_path_encoded}/raw'
            headers = {'PRIVATE-TOKEN': token}
            params = {'ref': branch}

            response = requests.get(url, headers=headers, params=params, timeout=60, stream=True)

            if response.status_code != 200:
                return False, f"Error HTTP {response.status_code}"

            # Guardar a disco con deduplicación para archivos parquet
            cache_path = self._get_cache_path(filename)
            with open(cache_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Aplicar deduplicación si es un archivo parquet
            if filename.endswith('.parquet'):
                from utils.parquet_utils import deduplicate_parquet
                result = deduplicate_parquet(cache_path, create_backup=True)
                if result['status'] != 'ok':
                    return False, f"Error en deduplicación: {result['status']}"
                if result['removed'] > 0:
                    add_breadcrumb(
                        category="cache",
                        message=f"Deduplicación aplicada a {filename}",
                        data={"removed_rows": result['removed']}
                    )

            # Obtener ETag remoto
            remote_etag = self._get_remote_etag(repo_id, branch, filename, token)

            # Obtener fecha del commit
            commit_date = self._get_commit_date(repo_id, branch, filename, token)

            # Actualizar metadata
            self.metadata[filename] = {
                'downloaded_at': datetime.datetime.now().isoformat(),
                'last_checked': datetime.datetime.now().isoformat(),
                'remote_etag': remote_etag,
                'commit_date': commit_date.isoformat() if commit_date else None,
                'repo_id': repo_id,
                'branch': branch,
                'size': cache_path.stat().st_size
            }
            self._save_metadata()

            add_breadcrumb(
                category="cache",
                message=f"Archivo descargado y cacheado: {filename}",
                data={"size": cache_path.stat().st_size, "commit_date": commit_date}
            )

            return True, None

        except Exception as e:
            error_msg = f"Error al descargar {filename}: {str(e)}"
            capture_exception(e, extra_data={
                "filename": filename,
                "repo_id": repo_id,
                "branch": branch
            })
            return False, error_msg

    def get_cached_file(self, filename: str) -> Optional[Path]:
        """
        Obtiene la ruta del archivo en caché.

        Returns:
            Path si existe, None si no está en caché
        """
        if self.is_cached(filename):
            return self._get_cache_path(filename)
        return None

    def get_commit_date(self, filename: str) -> Optional[datetime.datetime]:
        """
        Obtiene la fecha del commit guardada en la metadata para un archivo en caché.

        Returns:
            datetime si existe en metadata, None si no
        """
        if filename in self.metadata:
            commit_date_str = self.metadata[filename].get('commit_date')
            if commit_date_str:
                try:
                    return datetime.datetime.fromisoformat(commit_date_str)
                except (ValueError, TypeError):
                    return None
        return None

    def update_commit_date(self, filename: str, commit_date: datetime.datetime):
        """
        Actualiza la fecha del commit en la metadata para un archivo en caché.
        """
        if filename in self.metadata:
            self.metadata[filename]['commit_date'] = commit_date.isoformat()
            self._save_metadata()

    def check_for_updates(self, filename: str, token: str) -> bool:
        """
        Verifica si hay actualizaciones disponibles para un archivo.
        NO descarga el archivo, solo verifica.

        Returns:
            True si hay actualización disponible, False si no
        """
        if filename not in self.metadata:
            return True  # No está en caché, necesita descarga

        try:
            meta = self.metadata[filename]
            repo_id = meta.get('repo_id')
            branch = meta.get('branch')
            cached_etag = meta.get('remote_etag')

            if not repo_id or not branch:
                return True  # Metadata incompleta, mejor re-descargar

            # Obtener ETag actual del remoto
            remote_etag = self._get_remote_etag(repo_id, branch, filename, token)

            # Actualizar última verificación
            self.metadata[filename]['last_checked'] = datetime.datetime.now().isoformat()
            self._save_metadata()

            # Comparar ETags
            has_update = (remote_etag != cached_etag) if remote_etag else False

            if has_update:
                add_breadcrumb(
                    category="cache",
                    message=f"Actualización disponible para: {filename}",
                    data={"cached_etag": cached_etag, "remote_etag": remote_etag}
                )

            return has_update

        except Exception as e:
            capture_exception(e, extra_data={
                "filename": filename,
                "action": "check_for_updates"
            })
            return False  # En caso de error, no forzar descarga

    def start_background_checker(self, token: str):
        """
        Inicia un thread en background que verifica actualizaciones cada 10 min.
        """
        if self.check_thread and self.check_thread.is_alive():
            return  # Ya está corriendo

        self.stop_checking.clear()

        def check_loop():
            while not self.stop_checking.is_set():
                try:
                    # Esperar 10 minutos (o hasta que se detenga)
                    if self.stop_checking.wait(CHECK_INTERVAL):
                        break

                    # Verificar cada archivo en caché
                    for filename in list(self.metadata.keys()):
                        if self.stop_checking.is_set():
                            break

                        has_update = self.check_for_updates(filename, token)

                        if has_update:
                            meta = self.metadata.get(filename, {})
                            repo_id = meta.get('repo_id')
                            branch = meta.get('branch')

                            if repo_id and branch:
                                add_breadcrumb(
                                    category="cache",
                                    message=f"Re-descargando archivo actualizado: {filename}"
                                )
                                self.download_and_cache(filename, repo_id, branch, token)

                except Exception as e:
                    capture_exception(e, extra_data={"action": "background_checker"})

        self.check_thread = threading.Thread(target=check_loop, daemon=True)
        self.check_thread.start()

        add_breadcrumb(
            category="cache",
            message="Background checker iniciado",
            data={"interval": CHECK_INTERVAL}
        )

    def stop_background_checker(self):
        """Detiene el thread de verificación en background"""
        self.stop_checking.set()
        if self.check_thread:
            self.check_thread.join(timeout=5)

    def get_cache_info(self) -> Dict[str, Any]:
        """Obtiene información sobre el estado de la caché"""
        total_size = 0
        file_count = 0

        for filename in self.metadata.keys():
            cache_path = self._get_cache_path(filename)
            if cache_path.exists():
                total_size += cache_path.stat().st_size
                file_count += 1

        return {
            'file_count': file_count,
            'total_size_mb': total_size / (1024 * 1024),
            'cache_dir': str(self.cache_dir),
            'files': list(self.metadata.keys())
        }

    def clear_cache(self, filename: Optional[str] = None):
        """
        Limpia la caché. Si se especifica filename, solo limpia ese archivo.
        Si no, limpia todo.
        """
        try:
            if filename:
                cache_path = self._get_cache_path(filename)
                if cache_path.exists():
                    cache_path.unlink()
                if filename in self.metadata:
                    del self.metadata[filename]
            else:
                # Limpiar todo
                for file in self.cache_dir.glob('*'):
                    if file != self.metadata_file:
                        file.unlink()
                self.metadata = {}

            self._save_metadata()

            add_breadcrumb(
                category="cache",
                message="Caché limpiada",
                data={"filename": filename or "all"}
            )
        except Exception as e:
            capture_exception(e, extra_data={
                "filename": filename,
                "action": "clear_cache"
            })


# Instancia global del gestor
_cache_manager = None


def get_cache_manager() -> DiskCacheManager:
    """Obtiene la instancia global del gestor de caché"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = DiskCacheManager()
    return _cache_manager
