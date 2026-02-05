import os
import time
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Tuple

# Streamlit
import streamlit as st

# My App
from ui.widgets import *
from ui.layout import set_custom_ccs

from core.RemoteSpark import remote_session, SparkComponent
from core.SharepointIngest import SharepointIngest
from core.ProcessDF import ProcessDF
from core.Scheduler import ApiHandler

# Configuração da Página
st.set_page_config(
    page_title="Sistema de Ingestão - Sharepoint", 
    page_icon="https://upload.wikimedia.org/wikipedia/commons/thumb/f/fc/Logo_CPFL_Energia.svg/960px-Logo_CPFL_Energia.svg.png",
    layout="wide")

# Configure Remote spark
@st.cache_resource()
def acquire_spark_component():
    spark = remote_session()
    return SparkComponent(spark)

# CSS
set_custom_ccs()

# Start spark engine
engine = acquire_spark_component()

# Create control table
engine.create_control_table()

def full_name() -> Tuple[str, str, str]:
    col1, col2, col3 = st.columns(3)
    with col1:
        list_catalogs = engine.get_catalogs()
        catalog = st.selectbox("Catalog", list_catalogs, key="catalog")
    with col2:
        list_schemas = engine.get_schemas(catalog)
        schema = st.selectbox("Schema", list_schemas, key="schema")
    with col3:
        table = st.text_input("Tabela", key="table",)
    return catalog, schema, table



class CronSyntax:

    @staticmethod
    def daily(hour: str) -> str:
        hour, minutes = hour.split(":")
        hour = int(hour)
        minutes = int(minutes)
        if not (0 <= hour <= 23 and 0 <= minutes <= 59):
            raise ValueError("Horário inválido")
        return f"0 {minutes} {hour} * * ?"
    
    @staticmethod
    def weekly(day:str, hour:str) -> str:
        week_days = dict(zip(
            ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"],
            ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        ))
        hour, minutes = hour.split(":")
        hour = int(hour)
        minutes = int(minutes)
        if not (0 <= hour <= 23 and 0 <= minutes <= 59):
            raise ValueError("Horário inválido")
        week_day = week_days.get(day)[:3]
        return f"0 {minutes} {hour} ? * {week_day}"
    
    @staticmethod
    def monthly(day:str, hour:str) -> str:
        hour, minutes = hour.split(":")
        hour = int(hour)
        minutes = int(minutes)
        if not (0 <= hour <= 23 and 0 <= minutes <= 59):
            raise ValueError("Horário inválido")
        if not (1 <= day <= 31):
            raise ValueError("Dia inválido")
        return f"0 {minutes} {hour} {day} * ?"




# Sidebar
if "page_selection" not in st.session_state:
    st.session_state.page_selection = "Upload Manual"
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/f/fc/Logo_CPFL_Energia.svg/960px-Logo_CPFL_Energia.svg.png")
    st.title("Ingestão de Dados")
    pages = ["Upload Manual", "SharePoint"] # Monitoramento
    for page in pages:
        if st.session_state.page_selection == page:
            st.button(page, key=page, use_container_width=True, type="primary")
        else:
            if st.button(page, key=page, use_container_width=True):
                st.session_state.page_selection = page
                st.rerun()
    st.markdown("<br><br><br><br><br>", unsafe_allow_html=True)
    st.markdown("---")
    st.info("Aplicação destinada a ingestão de dados no Data Lake.")
    st.caption("Desenvolvido por\nEISA BigData\n")
ingestion_type = st.session_state.page_selection

# Layout Principal
st.title("Data Ingestion App")
if "df_preview" not in st.session_state:
    st.session_state.df_preview = None
if "transient_path" not in st.session_state:
    st.session_state.transient_path = None
if "cron_syntax" not in st.session_state:
    st.session_state.cron_syntax = None
if "schedule" not in st.session_state:
    st.session_state.schedule = None
col_input, col_display = st.columns([1, 2], gap="large")
with col_input:
    if ingestion_type == "Upload Manual":
        with st.container(border=True):
            st.subheader("Upload Manual de Arquivos")
            st.write("Utilize este formulário para carregar arquivos CSV, XLSX ou Parquet manualmente.")
            st.markdown("### Enviar Arquivo")       
            file = st.file_uploader("", type=["xlsx", "csv", "parquet"], label_visibility="collapsed")
            catalog, schema, table = "", "", ""
            catalog, schema, table = full_name()
            sheet_name = get_file_params(file)
            write_mode = get_write_mode()
            st.session_state.transient_path = None
            if st.button("Carregar", type="secondary", use_container_width=True):
                if (file is not None) and ("" not in [catalog, schema, table]):
                    st.session_state.file_name = file.name.split('.')[0].lower()
                    st.session_state.file_format = file.name.split('.')[-1].lower()
                    st.session_state.sheet_name = sheet_name
                    with st.spinner("Processando arquivo..."):
                        df_preview = read_dragged_file(file, sheet_name)
                        df_preview = ProcessDF().execute(df_preview)
                        st.session_state.df_preview = df_preview
                else:
                    if file is None:
                        st.warning("Selecione um arquivo.")
                    elif "" in [catalog, schema, table]:
                        st.warning("Preencha todos os campos.")
    elif ingestion_type == "SharePoint":
        with st.container(border=True):
            st.subheader("Upload do Sahrepoint")
            st.write("Utilize este formulário para carregar tabelas hospedadas no Sharepoint.")
            st.markdown("### Enviar Arquivo")     
            sharepoint_url = st.text_input("Link do arquivo no SharePoint", placeholder="https://cpflenergia.sharepoint.com/...")
            catalog, schema, table = "", "", ""
            catalog, schema, table = full_name()
            write_mode = get_write_mode()
            st.session_state.schedule, frequency, schedule_day, schedule_hour, schedule_day_of_month = get_schedule_config()
            syntax = CronSyntax()
            if frequency == "DAILY":
                st.session_state.cron_syntax = syntax.daily(hour=schedule_hour)
            elif frequency == "WEEKLY":
                st.session_state.cron_syntax = syntax.weekly(day=schedule_day, hour=schedule_hour)
            elif frequency == "MONTHLY":
                st.session_state.cron_syntax = syntax.monthly(day=schedule_day_of_month, hour=schedule_hour)

            if st.button("Carregar", type="secondary", use_container_width=True):
                if "" not in [sharepoint_url, catalog, schema, table]:
                    sharepoint = SharepointIngest()
                    df_preview = sharepoint.execute(sharepoint_url)
                    df_preview = ProcessDF().execute(df_preview)
                    st.session_state.df_preview = df_preview
                    st.session_state.transient_path = sharepoint.filepath
                    st.session_state.file_name = sharepoint.filepath.split('.')[-2].lower()
                    st.session_state.file_format = sharepoint.filepath.split('.')[-1].lower()
                    st.session_state.sheet_name = None
                else:
                    if sharepoint_url == "":
                        st.warning("Selecione um arquivo.")
                    elif "" in [catalog, schema, table]:
                        st.warning("Preencha todos os campos.")
    elif ingestion_type == "Monitoramento":
        st.subheader("Monitoramento de Ingestões")
        st.write("Área destinada ao acompanhamento das ingestões registradas.")
        st.button("Atualizar status")
        st.info("Tabela de controle e logs serão exibidos aqui.")
with col_display:
    with st.container(border=False):
        if (st.session_state.df_preview is not None) and ("" not in [catalog, schema, table]):
            st.subheader("Preview e Validação")
            st.markdown(f"**Tabela Destino:** `{st.session_state.catalog}.{st.session_state.schema}.{st.session_state.table}`")
            st.markdown(f"**Modo:** `{write_mode.upper()}`")
            st.dataframe(st.session_state.df_preview, use_container_width=True, height=400)
            m1, m2 = st.columns(2)
            m1.metric("Linhas", st.session_state.df_preview.shape[0])
            m2.metric("Colunas", st.session_state.df_preview.shape[1])
            if st.button("Confirmar", type="secondary", use_container_width=True):
                data = {
                    "inserted_by": engine.username(),
                    "db_catalog": st.session_state.catalog,
                    "db_schema": st.session_state.schema,
                    "db_table": st.session_state.table,
                    "insert_mode": write_mode,
                    "transient": st.session_state.transient_path,
                    "file_name": st.session_state.file_name,
                    "file_format": st.session_state.file_format,
                    "sheet": None if st.session_state.sheet_name == 0 else st.session_state.sheet_name,
                    "n_rows": st.session_state.df_preview.shape[0],
                    "n_columns": st.session_state.df_preview.shape[1],
                    "dh_insertion": datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M")
                }
                dict_keys = ", ".join(data.keys())
                dict_values = ", ".join(
                    "NULL" if v is None else f"'{v}'" if isinstance(v, str) else str(v)
                    for v in data.values()
                )
                mode = True if write_mode == "overwrite" else False
                if ingestion_type  in ["Upload Manual", "SharePoint"]:
                    full_table_name = f"{st.session_state.catalog}.{st.session_state.schema}.{st.session_state.table}"
                    success, message = engine.save_table(st.session_state.df_preview, full_table_name, mode)
                if success:
                    engine.spark.sql(f"""
                        INSERT INTO hive_uc.default.controle_processamento_sharepoint_app
                        ({dict_keys})
                        VALUES ({dict_values})
                    """)
                    st.success("Carga realizada com sucesso!")
                    if st.session_state.schedule and ingestion_type=="SharePoint":
                        db_table = f"{st.session_state.catalog}.{st.session_state.schema}.{st.session_state.table}"
                        scheduler = ApiHandler(name = db_table)
                        job_success, msg = scheduler.create_job(
                            sharepoint_url=sharepoint_url, 
                            db_table=db_table, 
                            mode=write_mode, 
                            quartz_cron_expression=st.session_state.cron_syntax
                        )
                        if job_success:
                            st.warning(msg)
                        else:
                            st.error(msg)
                else:
                    st.error(f"Erro ao salvar dados: {message}")   
