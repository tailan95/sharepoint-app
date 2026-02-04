import os
import time
import json
from datetime import datetime
from zoneinfo import ZoneInfo

# Streamlit
import streamlit as st

# My App
from ui.widgets import *
from ui.layout import set_custom_ccs

from core.SharepointIngest import SharepointIngest
from core.Standardizer import Standardizer

# Configuração da Página
st.set_page_config(page_title="SharePoint Ingest", layout="wide")

from core.SparkSession import *

# CSS Avançado para Forçar o Layout
set_custom_ccs()

# Initialize spark session
spark = get_spark_session()

# Create control table
create_control_table(spark)

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
    st.caption("Desenvolvido por\nEISA BigDatal\n")
ingestion_type = st.session_state.page_selection

# Layout Principal
st.title("Data Ingestion App")
if "df_preview" not in st.session_state:
    st.session_state.df_preview = None
if "transient_path" not in st.session_state:
    st.session_state.transient_path = None
col_input, col_display = st.columns([1, 2], gap="large")
with col_input:
    if ingestion_type == "Upload Manual":
        with st.container(border=True):
            st.subheader("Upload Manual de Arquivos")
            st.write("Utilize este formulário para carregar arquivos CSV, XLSX ou Parquet manualmente.")
            st.markdown("### Enviar Arquivo")       
            file = st.file_uploader("", type=["xlsx", "csv", "parquet"], label_visibility="collapsed")
            catalog, schema, table = "", "", ""
            catalog, schema, table = get_full_name(spark)
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
                        df_preview = Standardizer().execute(df_preview)
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
            catalog, schema, table = get_full_name(spark)
            write_mode = get_write_mode()
            schedule_day, schedule_hour, schedule_day_of_month = get_schedule_config()
            if st.button("Carregar", type="secondary", use_container_width=True):
                if "" not in [sharepoint_url, catalog, schema, table]:
                    engine = SharepointIngest()
                    df_preview = engine.execute(sharepoint_url)
                    df_preview = Standardizer().execute(df_preview)
                    st.session_state.df_preview = df_preview
                    st.session_state.transient_path = engine.folder_path
                    st.session_state.file_name = engine.folder_path.split('.')[-2].lower()
                    st.session_state.file_format = engine.folder_path.split('.')[-1].lower()
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
        st.subheader("Preview e Validação")
        if (st.session_state.df_preview is not None) and (catalog is not "") and (schema is not "") and (table is not ""):
            st.markdown(f"**Tabela Destino:** `{st.session_state.catalog}.{st.session_state.schema}.{st.session_state.table}`")
            st.markdown(f"**Modo:** `{write_mode.upper()}`")
            st.dataframe(st.session_state.df_preview, use_container_width=True, height=400)
            m1, m2 = st.columns(2)
            m1.metric("Linhas", st.session_state.df_preview.shape[0])
            m2.metric("Colunas", st.session_state.df_preview.shape[1])
            if st.button("Confirmar", type="secondary", use_container_width=True):
                data = {
                    "inserted_by": get_user_from_spark(spark),
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
                    success, message = save_table(spark, st.session_state.df_preview, full_table_name, mode)
                if success:
                    spark.sql(f"""
                        INSERT INTO hive_uc.default.controle_processamento_sharepoint_app
                        ({dict_keys})
                        VALUES ({dict_values})
                    """)
                    # st.toast(f"Dados salvos no Unity Catalog!")
                    st.success("Carga realizada com sucesso!")
                else:
                    st.error(f"Erro ao salvar dados: {message}")   
