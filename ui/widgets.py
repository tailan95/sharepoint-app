import pandas as pd
from datetime import time

import streamlit as st

from typing import Optional, Union

def get_write_mode() -> str:
    return st.radio(
        "Modo de escrita",
        ["overwrite", "append"],
        horizontal=True,
        help="Selecione apenas um modo de operação:\n    -overwrite: sobrescreve o conteúdo da tabela;\n    -append: insere os dados no final da tabela."
    )

def read_dragged_file(file:st.file_uploader, sheet_name:Optional[str]=0) -> pd.DataFrame:
    try:
        if file.name.endswith('.csv'):
            return pd.read_csv(file)
        elif file.name.endswith('.xlsx'):
            return pd.read_excel(file, sheet_name=sheet_name)
        elif file.name.endswith('.parquet'):
            return pd.read_parquet(file)
        st.success("Arquivo carregado!")
        return df
    except Exception as e:
        st.error(f"Erro ao ler arquivo: {e}")
    
def get_schedule_config() -> Union[str, str, str]:
    schedule_day, schedule_hour, schedule_day_of_month = None, None, None
    with st.container(border=False):
        schedule = st.toggle("Habilitar Agendamento Automático", key="toggle_sched")
        is_disabled = not schedule
        col_freq, col_detalhes = st.columns([1, 1])
        with col_freq:
            frequency = st.selectbox("Frequência de Ingestão", ["DAILY", "WEEKLY", "MONTHLY"], disabled=is_disabled)
        with col_detalhes:
            if frequency == "WEEKLY":
                schedule_day = st.selectbox("Dia da semana", ["Segunda-feira", "Terça-feira", "Quarta-feira", "Quinta-feira", "Sexta-feira", "Sábado", "Domingo"], disabled=is_disabled)
            elif frequency == "MONTHLY":
                schedule_day_of_month = st.number_input("Dia do mês", 1, 31, 1, disabled=is_disabled)
            schedule_time_obj = st.time_input("Hora de execução", value=time(12, 0), disabled=is_disabled)
            schedule_hour = schedule_time_obj.strftime('%H:%M')
    return schedule, frequency, schedule_day, schedule_hour, schedule_day_of_month

def get_file_params(file):
    sheet_name = None
    if file is not None and file.name.endswith(('.xlsx', '.xls')):
        st.write("") # Espaçamento
        sheet_name = st.text_input(
            "Sheet", 
            placeholder="(Opcional) Sheet",
            help="Especifique o nome exato da aba no Excel. Se não preencher, a primeira será carregada."
        )
    if sheet_name == "":
        sheet_name = 0
    return sheet_name


    
