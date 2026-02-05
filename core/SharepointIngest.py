import os
import io
import re
import toml
import base64
import requests
import unicodedata
import pandas as pd

from pathlib import Path
from urllib.parse import urlparse

# Typing
from typing import Any, Dict, Optional

# Azure
from azure.identity import ClientSecretCredential

# Databricks
from databricks.sdk import WorkspaceClient

# PySpark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

secrets = {}
paths = [
    Path(os.getcwd()).parent / "secrets" / "secrets.toml",
    Path(os.getcwd()) / "secrets" / "secrets.toml"
]
for path in paths:
    if path.exists():
        secrets = toml.load(path)
        break

class SharepointIngest:
    
    _host = os.environ.get("DATABRICKS_HOST", secrets.get("databricks", {}).get("host"))
    _token = os.environ.get("DATABRICKS_TOKEN", secrets.get("databricks", {}).get("token"))

    def __init__(self) -> None:

        # Initialize Workspace Client
        self.client = WorkspaceClient(
            host = self._host,
            token = self._token,
        )

        # Retrieve secrets
        self.tenant_id = os.environ.get("SHAREPOINT_TENANT_ID", secrets.get("sharepoint", {}).get("tenant_id"))
        self.client_id = os.environ.get("SHAREPOINT_CLIENT_ID", secrets.get("sharepoint", {}).get("client_id"))
        self.client_secret = os.environ.get("SHAREPOINT_CLIENT_SECRET", secrets.get("sharepoint", {}).get("client_secret"))

        # Credential
        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        # Azure Token
        self.token = self.credential.get_token("https://graph.microsoft.com/.default").token

        # Params
        self.df = pd.DataFrame([])
        self.filepath = ""
    
    @staticmethod
    def decode64(input:str) -> str:
        """
        Decode a base64-encoded string.
        """
        return base64.b64decode(input).decode('utf-8').strip().replace('"', '')
    
    @staticmethod
    def encode_sharepoint_url(url: str) -> str:
        encoded = base64.b64encode(url.encode("utf-8")).decode("utf-8")
        encoded = encoded.rstrip("=").replace("/", "_").replace("+", "-")
        return f"u!{encoded}"

    @staticmethod
    def normalize_name(name:str) -> str:
        name = unicodedata.normalize("NFKD", name)
        name = re.sub(r"[^\x00-\x7F]", "", name).lower()
        name = re.sub(r"[.\-]", "_", name)
        name = re.sub(r"[^a-z0-9_]", "_", name)
        name = re.sub(r"_+", "_", name).strip("_")
        return name.lower()
    
    def resolve_drive_item(self, file_url: str) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        encoded_url = self.encode_sharepoint_url(file_url)
        response = requests.get(
            f"https://graph.microsoft.com/v1.0/shares/{encoded_url}/driveItem",
            headers=headers
        )
        response.raise_for_status()
        drive_item = response.json()
        return drive_item
    
    def create_folder(self, name: str) -> str:
        volume_base_path = "/Volumes/hive_uc/transient/sharepoint/datasource"
        folder_name = self.normalize_name(name)
        folder_path = f"{volume_base_path}/{folder_name}"
        try:
            self.client.files.get_directory_metadata(directory_path=folder_path)
        except Exception:
            self.client.files.create_directory(directory_path=folder_path)
        return folder_path

    def download_and_save_file(self, file_url: str) -> str:

        # Resolve metadata
        file_metadata = self.resolve_drive_item(file_url=file_url)
        folder_name = self.normalize_name(file_url.split('/')[-2])
        file_name = file_metadata.get("name")
        drive_id = file_metadata.get("parentReference")["driveId"]
        item_id = file_metadata.get("id")

        # Create folder
        destination_folder = self.create_folder(name=folder_name)

        # Download from sharepoint
        download_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(download_url, headers=headers)
        response.raise_for_status()
        file_stream = io.BytesIO(response.content)
        
        # Final path
        destination_file_path = f"{destination_folder}/{file_name}"
        
        # Save in databricks
        try:
            self.client.files.upload(destination_file_path, file_stream, overwrite=True)
            return destination_file_path
        except Exception as e:
            raise Exception(f"Erro ao salvar no Volume do Databricks: {e}")

    def read_file(self, filepath: str) -> pd.DataFrame:
        try:
            response = self.client.files.download(filepath)
            file_bytes = response.contents.read()
            if filepath.lower().endswith('.csv'):
                df = pd.read_csv(io.BytesIO(file_bytes))
            elif filepath.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(file_bytes))
            elif filepath.lower().endswith('.parquet'):
                df = pd.read_parquet(io.BytesIO(file_bytes))
            return df
        except Exception as e:
            return f"Erro ao ler arquivo do Volume: {e}"
    
    def execute(self, file_url:str) -> DataFrame:

        # Download and Save File
        filepath = self.download_and_save_file(file_url=file_url)
        
        # Read file
        df = self.read_file(filepath=filepath)

        # Store params
        self.df = df
        self.filepath = filepath
        return df

if __name__=="__main__":

    file_url="https://cpflenergia.sharepoint.com/:x:/s/CARE-Subtransmisso/IQA-p1JEpAVZTL8kAVZM2NKtAb1B-RdKraS_jle9Bh0kcZs?e=itU36h"
    # file_url = "https://cpflenergia.sharepoint.com/:x:/s/BigDataJourney/IQCr3yuphE7gTLy3L9N_3bKlATUEnkE7PIcgdskE2yRO85w?e=jjgot6"
    engine = SharepointIngest()

    df = engine.execute(
        file_url=file_url,
    )

