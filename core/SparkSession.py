import os
import pandas as pd
import streamlit as st
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from core.SharepointIngest import SharepointIngest

from typing import List, Union, Tuple

@st.cache_resource
def get_spark_session():
    for var in ["DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET"]:
        os.environ.pop(var, None)
    try:
        db_config = st.secrets.get("databricks", {})
        host_url = str(db_config.get("host"))
        token_val = str(db_config.get("token"))
        cluster_id = str(db_config.get("cluster_id"))
        return DatabricksSession.builder.remote(
            host=host_url,
            token=token_val,
            cluster_id=cluster_id
        ).getOrCreate()
    except Exception as e:
        st.error(f"Falha ao conectar no Spark: {e}")
        return None

spark = get_spark_session()

@st.cache_data(ttl=3600)
def get_catalogs(_spark) -> List[str]:
    df = _spark.sql("SHOW CATALOGS").toPandas()
    df = df["catalog"].tolist()
    return df

@st.cache_data(ttl=3600)
def get_schemas(_spark, catalog:str) -> List[str]:
    df = _spark.sql(f"SHOW SCHEMAS IN {catalog}").toPandas()
    df = df["databaseName"].tolist()
    return df

@st.cache_data(ttl=3600)
def get_existing_tables(_spark, catalog: str, schema: str) -> list:
    df = _spark.sql(f"SHOW TABLES IN {catalog}.{schema}").toPandas()
    return df["tableName"].tolist()

def get_full_name(spark) -> Tuple[str, str, str]:
    col1, col2, col3 = st.columns(3)
    with col1:
        list_catalogs = get_catalogs(spark)
        catalog = st.selectbox("Catalog", list_catalogs, key="catalog")
    with col2:
        list_schemas = get_schemas(spark, catalog)
        schema = st.selectbox("Schema", list_schemas, key="schema")
    with col3:
        table = st.text_input("Tabela", key="table")
    return catalog, schema, table

def create_control_table(spark) -> None:
    spark.sql(
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

def get_user_from_spark(spark):
    try:
        # Consulta o usuário da sessão ativa no Databricks
        user_df = spark.sql("SELECT current_user() as user")
        return user_df.collect()[0]["user"]
    except Exception:
        return "desconhecido"

def save_table(spark, df_pandas:pd.DataFrame, full_path: str, overwrite: bool = True) -> Tuple[bool, str]:
    try:
        df_spark = spark.createDataFrame(df_pandas)
        df_spark.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite" if overwrite else "append") \
            .saveAsTable(full_path)
        return True, f"Tabela {full_path} salva com sucesso!"
    except Exception as e:
        return False, f"Erro ao salvar no Unity Catalog: {str(e)}"

def save_to_unit_catalog(spark, df_pandas:pd.DataFrame, full_path: str, overwrite: bool = True) -> Tuple[bool, str]:
    try:
        df_spark = spark.createDataFrame(df_pandas)
        df_spark.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .mode("overwrite" if overwrite else "append") \
            .save(full_path)
        return True, f"Tabela {full_path} salva com sucesso!"
    except Exception as e:
        return False, f"Erro ao salvar no Unity Catalog: {str(e)}"

    

    