"""
Standalone downloader to fetch files from GitLab into the local `data/` folder.

This script replicates the same behavior used by the app:
- Reads configuration and token from `.streamlit/secrets.toml` (same keys as the app)
- Uses GitLab API endpoints `/repository/tree` and `/repository/files/.../raw`
- Downloads the files defined in `modules` mapping and saves them into `data/` preserving directory structure

Usage:
    python tools/download_from_gitlab.py

Make sure `.streamlit/secrets.toml` contains:
    FUENTE_DATOS = "gitlab"
    REPO_ID = "namespace/project"
    BRANCH = "main"
    [gitlab]
    token = "glpat..."

"""
import os
import sys
import time
import requests
from pathlib import Path
import tomli
from urllib.parse import quote

# Copy of the modules mapping used by the app (files expected per module)
MODULES = {
    'bco_gente': ['df_global_banco.parquet', 'df_global_pagados.parquet'],
    'cba_capacita': ['df_postulantes_cbamecapacita.parquet','df_alumnos.parquet', 'df_cursos.parquet'],
    'empleo': ['df_postulantes_empleo.parquet','df_inscriptos_empleo.parquet', 'df_empresas.parquet','capa_departamentos_2010.geojson'],
}

DEFAULT_DATA_DIR = 'data'


def load_secrets(secrets_path='.streamlit/secrets.toml'):
    p = Path(secrets_path)
    if not p.exists():
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}")
    with open(p, 'rb') as f:
        cfg = tomli.load(f)

    # Top-level keys
    fuente = cfg.get('FUENTE_DATOS', 'gitlab')
    repo_id = cfg.get('REPO_ID')
    branch = cfg.get('BRANCH', 'main')

    # Token under [gitlab]
    gitlab_section = cfg.get('gitlab', {})
    token = gitlab_section.get('token')

    return {
        'fuente': fuente,
        'repo_id': repo_id,
        'branch': branch,
        'token': token,
    }


def obtener_lista_archivos_gitlab(repo_id, branch, token):
    if not token:
        raise ValueError('GitLab token not provided')
    repo_id_encoded = quote(str(repo_id), safe='')
    url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/tree'
    headers = {'PRIVATE-TOKEN': token}
    params = {'ref': branch, 'recursive': True}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    items = resp.json()
    files = [item['path'] for item in items if item.get('type') == 'blob']
    return files


def descargar_archivo_gitlab(repo_id, branch, file_path, token, save_root=DEFAULT_DATA_DIR, max_retries=3):
    if not token:
        raise ValueError('GitLab token not provided')

    repo_id_encoded = quote(str(repo_id), safe='')
    file_path_encoded = quote(file_path, safe='')
    url = f'https://gitlab.com/api/v4/projects/{repo_id_encoded}/repository/files/{file_path_encoded}/raw'

    headers = {'PRIVATE-TOKEN': token}
    params = {'ref': branch}

    timeout = 60
    for intento in range(max_retries):
        try:
            with requests.get(url, headers=headers, params=params, timeout=timeout, stream=True) as r:
                if r.status_code == 200:
                    # Ensure save directory exists
                    local_path = Path(save_root) / Path(file_path)
                    local_path.parent.mkdir(parents=True, exist_ok=True)

                    # Stream to file
                    with open(local_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    return str(local_path)
                else:
                    text_snippet = (r.text or '')[:200]
                    raise RuntimeError(f'HTTP {r.status_code}: {text_snippet}')
        except requests.exceptions.Timeout:
            if intento < max_retries - 1:
                time.sleep(1 + intento)
                continue
            else:
                raise
        except requests.exceptions.ConnectionError as e:
            if intento < max_retries - 1:
                time.sleep(1 + intento)
                continue
            else:
                raise

    raise RuntimeError('Failed to download after retries')


def main():
    try:
        config = load_secrets()
    except Exception as e:
        print(f"Error loading secrets: {e}")
        sys.exit(1)

    if config['fuente'] != 'gitlab':
        print("FUENTE_DATOS is not set to 'gitlab' in secrets. Exiting.")
        sys.exit(1)

    repo_id = config['repo_id']
    branch = config['branch']
    token = config['token']

    if not repo_id or not token:
        print('repo_id or token missing in secrets. Please configure `.streamlit/secrets.toml`.')
        sys.exit(1)

    print(f"Repo: {repo_id}  Branch: {branch}")

    # Build set of desired files from MODULES
    desired_files = set()
    for files in MODULES.values():
        for f in files:
            desired_files.add(f.replace('\\', '/'))

    # List all files available in gitlab repo
    try:
        print('Listing files in GitLab repo...')
        available_files = obtener_lista_archivos_gitlab(repo_id, branch, token)
    except Exception as e:
        print(f"Error listing files in GitLab: {e}")
        sys.exit(1)

    # Map desired files to actual paths in repo (exact match or by filename)
    to_download = []
    for desired in desired_files:
        if desired in available_files:
            to_download.append(desired)
        else:
            # try to find by filename suffix
            name = desired.split('/')[-1]
            matches = [a for a in available_files if a.endswith('/' + name) or a == name]
            if matches:
                to_download.append(matches[0])
            else:
                print(f"Warning: Could not find file in repo: {desired}")

    if not to_download:
        print('No files found to download. Exiting.')
        sys.exit(0)

    print(f"Found {len(to_download)} files to download:")
    for f in to_download:
        print(' -', f)

    # Download each file into data/
    saved = []
    for f in to_download:
        try:
            print(f"Downloading {f} ...")
            local_path = descargar_archivo_gitlab(repo_id, branch, f, token, save_root=DEFAULT_DATA_DIR)
            saved.append(local_path)
            print(f"Saved to: {local_path}")
        except Exception as e:
            print(f"Error downloading {f}: {e}")

    print('\nDownload complete. Summary:')
    for s in saved:
        print(' *', s)


if __name__ == '__main__':
    main()
