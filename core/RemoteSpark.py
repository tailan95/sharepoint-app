import os
import toml
import pandas as pd
from pathlib import Path
from typing import List, Union, Tuple

# Streamlit
import streamlit as st

# Databricks remote Spark Session
from databricks.connect import DatabricksSession

cluster_id = "0223-180802-ufyybjgl"

os.environ.pop("DATABRICKS_CLIENT_ID", None)
os.environ.pop("DATABRICKS_CLIENT_SECRET", None)

def remote_session():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if token:
        return DatabricksSession.builder.remote(
            host=host,
            token=token,
            cluster_id=cluster_id
        ).getOrCreate()

class SparkComponent:

    def __init__(self, spark) -> None:
        self.spark = spark

    def username(self):
        try:
            user_df = self.spark.sql("SELECT current_user() as user")
            return user_df.collect()[0]["user"]
        except Exception:
            return "desconhecido"

    def get_catalogs(self) -> List[str]:
        df = self.spark.sql("SHOW CATALOGS").toPandas()
        df = df["catalog"].tolist() 
        return [x for x in df if not x.startswith("__") and x not in ["acs_test"]]

    def get_schemas(self, catalog:str) -> List[str]:
        df = self.spark.sql(f"SHOW SCHEMAS IN {catalog}").toPandas()
        df = df["databaseName"].tolist()
        return [x for x in df if x not in ["default", "information_schema"]]

    def existing_tables(self, catalog: str, schema: str) -> list:
        df = self.spark.sql(f"SHOW TABLES IN {catalog}.{schema}").toPandas()
        return df["tableName"].tolist()

    def create_control_table(self) -> None:
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS hive_uc.default.controle_processamento_sharepoint_app (
                -- process_id BIGINT,
                inserted_by STRING,
                db_catalog STRING,
                db_schema STRING,
                db_table STRING,
                insert_mode STRING,
                transient STRING,
                file_name STRING,
                file_format STRING,
                sheet STRING,
                n_rows INT,
                n_columns INT,
                dh_insertion STRING
            )
            USING DELTA;
            """
            )    
        return None

    def save_table(self, df_pandas:pd.DataFrame, full_name: str, overwrite: bool = True) -> Tuple[bool, str]:
        try:
            df_spark = self.spark.createDataFrame(df_pandas)
            df_spark.write \
                .format("delta") \
                .option("mergeSchema", "true") \
                .mode("overwrite" if overwrite else "append") \
                .saveAsTable(full_name)
            return True, f"Tabela {full_name} salva com sucesso!"
        except Exception as e:
            return False, f"Erro ao salvar no Unity Catalog: {str(e)}"

if __name__=="__main__":

    spark = remote_session()
    engine = SparkComponent(spark)

    
