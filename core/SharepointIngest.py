import os
import io
import re
import base64
import requests
import unicodedata
import pandas as pd

# Typing
from typing import Any, Dict, Optional

# Azure
from azure.identity import ClientSecretCredential

# Databricks
from databricks.sdk import WorkspaceClient

# PySpark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# Streamlit
import streamlit as st

# Warnings
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=Warning)

class SharepointIngest:

    def __init__(self) -> None:
        
        def process_secret(key:str) -> str:
            value = self.db_client.secrets.get_secret(scope=scope, key=key).value.strip()
            if "-" not in value and len(value) > 20:
                try:
                    return base64.b64decode(value).decode('utf-8').strip()
                except:
                    return value
            return value

        # Clean env
        for var in ["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]:
            os.environ.pop(var, None)

        # Secrets (API)
        scope = "adb-secret-scope"
        self.db_client = WorkspaceClient(
            host=st.secrets["databricks"]["host"],
            token=st.secrets["databricks"]["token"]
        )

        # Credential
        self.credential = ClientSecretCredential(
            tenant_id=process_secret("akv-sharepoint-tenant-id"),
            client_id=process_secret("akv-sharepoint-client-id"),
            client_secret=process_secret("akv-sharepoint-client-secret")
        )

        # Token
        try:
            self.token = self.credential.get_token("https://graph.microsoft.com/.default").token
        except Exception as e:
            st.error(f"Falha na obtenção do Token Graph: {e}")
            raise e

        # Dataframe
        self.df = None

        # Folder path
        self.folder_path = None
    
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
            self.db_client.files.get_directory_metadata(directory_path=folder_path)
        except Exception:
            self.db_client.files.create_directory(directory_path=folder_path)
        return folder_path

    def download_and_save_file(self, file_url: str) -> str:
        # Resolve metadata
        file_metadata = self.resolve_drive_item(file_url=file_url)
        file_name = file_metadata["name"]
        drive_id = file_metadata["parentReference"]["driveId"]
        item_id = file_metadata["id"]

        # Create folder
        destination_folder = self.create_folder(name=file_name)

        # Download from sharepoint
        download_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(download_url, headers=headers)
        response.raise_for_status()
        
        # Final path
        destination_file_path = f"{destination_folder}/{file_name}"
        
        # Save in databricks
        try:
            self.db_client.files.upload(destination_file_path, response.content, overwrite=True)
            return destination_file_path
        except Exception as e:
            raise Exception(f"Erro ao salvar no Volume do Databricks: {e}")

    def read_file(self, filepath: str) -> pd.DataFrame:
        try:
            # 1. Baixa o conteúdo binário do arquivo via API (SDK)
            # Isso funciona para qualquer formato no Volume
            response = self.db_client.files.download(filepath)
            file_bytes = response.contents.read()
            
            # 2. Identifica a extensão e lê com Pandas puro
            if filepath.lower().endswith('.csv'):
                df = pd.read_csv(io.BytesIO(file_bytes))
                
            elif filepath.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(io.BytesIO(file_bytes))
                
            elif filepath.lower().endswith('.parquet'):
                df = pd.read_parquet(io.BytesIO(file_bytes))
                
            else:
                st.error(f"Formato de arquivo não suportado: {filepath}")
                return None

            # 3. Guarda no session_state do Streamlit
            st.session_state.df_preview = df
            st.toast(f"Arquivo salvo em `{filepath}`")
            return df

        except Exception as e:
            st.error(f"Erro ao ler arquivo do Volume: {e}")
            return None

    def save(self, db_table:str) -> None:
        self.df.write.mode("overwrite")\
                .format("delta")\
                .option("overwriteSchema", "true")\
                .format("delta")\
                .saveAsTable(db_table)
        return self.df  
    
    def execute(self, file_url:str, db_table:Optional[str]=None) -> DataFrame:

        # Download and Save File
        filepath = self.download_and_save_file(file_url=file_url)
        
        # Read file
        self.df = self.read_file(filepath=filepath)

        # Write table into UnitCatalog
        self.folder_path = filepath
        if db_table:
            self.save(db_table=db_table)
            print(f"Table {db_table} created in the UnitCatalog.")
        return self.df