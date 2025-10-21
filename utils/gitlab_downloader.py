import os
import requests
import tomli
from pathlib import Path
from typing import List, Dict

class GitLabDownloader:
    def __init__(self, gitlab_url: str, private_token: str):
        """
        Initialize GitLab downloader
        
        Args:
            gitlab_url (str): Base URL of GitLab instance
            private_token (str): GitLab private access token
        """
        self.gitlab_url = gitlab_url.rstrip('/')
        self.headers = {'PRIVATE-TOKEN': private_token}
        
    def get_project_files(self, project_id: int, ref: str = 'main', path: str = '') -> List[Dict]:
        """
        Get list of files in a project repository
        
        Args:
            project_id (int): GitLab project ID
            ref (str): Branch name or commit SHA
            path (str): Path to list files from
            
        Returns:
            List of file information dictionaries
        """
        url = f"{self.gitlab_url}/api/v4/projects/{project_id}/repository/tree"
        params = {'ref': ref, 'path': path}
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json()
    
    def download_file(self, project_id: int, file_path: str, ref: str = 'main', 
                     save_dir: str = 'data') -> str:
        """
        Download a single file from GitLab repository
        
        Args:
            project_id (int): GitLab project ID
            file_path (str): Path to file in repository
            ref (str): Branch name or commit SHA
            save_dir (str): Local directory to save file
            
        Returns:
            Path to downloaded file
        """
        url = f"{self.gitlab_url}/api/v4/projects/{project_id}/repository/files/{file_path}/raw"
        params = {'ref': ref}
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        # Create save directory if it doesn't exist
        os.makedirs(save_dir, exist_ok=True)
        
        # Get filename from path and create save path
        filename = os.path.basename(file_path)
        save_path = os.path.join(save_dir, filename)
        
        # Save file
        with open(save_path, 'wb') as f:
            f.write(response.content)
            
        return save_path
    
    def download_repository(self, project_id: int, ref: str = 'main', 
                          base_path: str = '', save_dir: str = 'data') -> List[str]:
        """
        Download all files from a repository or specific path
        
        Args:
            project_id (int): GitLab project ID
            ref (str): Branch name or commit SHA
            base_path (str): Base path in repository to download from
            save_dir (str): Local directory to save files
            
        Returns:
            List of downloaded file paths
        """
        downloaded_files = []
        files = self.get_project_files(project_id, ref, base_path)
        
        for file_info in files:
            if file_info['type'] == 'blob':  # It's a file
                try:
                    file_path = self.download_file(
                        project_id,
                        file_info['path'],
                        ref,
                        save_dir
                    )
                    downloaded_files.append(file_path)
                except Exception as e:
                    print(f"Error downloading {file_info['path']}: {str(e)}")
            
            elif file_info['type'] == 'tree':  # It's a directory
                # Create subdirectory
                subdir = os.path.join(save_dir, file_info['name'])
                os.makedirs(subdir, exist_ok=True)
                
                # Recursively download files in subdirectory
                sub_files = self.download_repository(
                    project_id,
                    ref,
                    file_info['path'],
                    subdir
                )
                downloaded_files.extend(sub_files)
        
        return downloaded_files

if __name__ == '__main__':
    # Load configuration from .streamlit/secrets.toml if it exists
    config_path = Path('.streamlit/secrets.toml')
    if config_path.exists():
        try:
            with open(config_path, 'rb') as f:
                config = tomli.load(f)
                gitlab_url = config.get('gitlab_url', '')
                gitlab_token = config.get('gitlab_token', '')
                gitlab_project_id = config.get('gitlab_project_id', 0)
        except Exception as e:
            print(f"Error reading config file: {str(e)}")
            exit(1)
    else:
        # Default values or environment variables could be used here
        gitlab_url = os.getenv('GITLAB_URL', '')
        gitlab_token = os.getenv('GITLAB_TOKEN', '')
        gitlab_project_id = int(os.getenv('GITLAB_PROJECT_ID', 0))

    if not all([gitlab_url, gitlab_token, gitlab_project_id]):
        print("Please configure GitLab settings in .streamlit/secrets.toml or environment variables")
        print("Required settings:")
        print("  gitlab_url = 'https://your-gitlab-instance.com'")
        print("  gitlab_token = 'your-private-token'")
        print("  gitlab_project_id = your-project-id")
        exit(1)

    # Initialize downloader
    downloader = GitLabDownloader(gitlab_url, gitlab_token)
    
    try:
        # Download all files from the data directory in the repository
        print("Starting download of repository files...")
        downloaded = downloader.download_repository(gitlab_project_id, base_path='data')
        print(f"\nSuccessfully downloaded {len(downloaded)} files to data directory:")
        for file in downloaded:
            print(f"- {file}")
    except Exception as e:
        print(f"Error: {str(e)}")