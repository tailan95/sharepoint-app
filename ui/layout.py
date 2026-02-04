import streamlit as st

def set_custom_ccs():
    st.markdown("""
        <style>
        /* Remove o scroll horizontal e ajusta o preenchimento */
        .main .block-container {
            padding-top: 2rem !important;
            padding-left: 3rem !important;
            padding-right: 3rem !important;
        }
        
        /* Título principal sem margens negativas que causam corte */
        h1 {
            padding-top: 0px;
            margin-top: 0px;
            color: #1F2937 !important;
        }

        /* background */
        .custom-header {
            background-color: #009FE3;
            color: white;
            padding: 10px 20px;
            font-size: 22px;
            font-weight: bold;
            display: flex;
            justify-content: space-between;
            align-items: center;
            /* Alinhado para não expandir além da largura da tela */
            margin-bottom: 20px;
            border-radius: 4px;
        }
        
        /* Itens de menu */
        div.stButton > button {
            width: 100%;
            border-radius: 10px;
            height: 3em;
            background-color: transparent;
            color: #1F2937;
            border: none;
            text-align: left;
            padding-left: 15px;
            font-weight: 500;
        }

        /* Remove o padding padrão da barra lateral para o botão ocupar tudo */
        [data-testid="stSidebarNav"] {padding-top: 0rem;}
        [data-testid="stSidebar"] .block-container {
            padding-top: 1rem !important;
            padding-left: 0rem !important; /* Remove espaço na esquerda */
            padding-right: 0rem !important; /* Remove espaço na direita */
        }

        /* Estilização Geral dos Botões na Sidebar */
        [data-testid="stSidebar"] div.stButton > button {
            width: 100%;
            border-radius: 0px 10px 10px 0px; /* Arredondado apenas na direita conforme o padrão */
            height: 3.5em;
            background-color: transparent;
            color: #1F2937;
            border: none;
            text-align: left;
            padding-left: 20px;
            margin: 0px !important;
            display: block;
        }

        /* Aba Ativa (Azul CPFL ocupando a largura total) */
        [data-testid="stSidebar"] div.stButton > button[kind="primary"] {
            background-color: #009FE3 !important;
            color: white !important;
            font-weight: bold;
            box-shadow: none;
        }

        /* Efeito de hover */
        [data-testid="stSidebar"] div.stButton > button:hover {
            background-color: #f0f2f6;
            color: #009FE3;
        }

        div.stButton > button {
            border: 2px solid #d3d3d3; /* Borda cinza clara */
            border-radius: 2px;
            color: #31333F;
            background-color: transparent;
            transition: all 0.2s;
        }
        div.stButton > button:hover {
            border: 2px solid #31333F; /* Borda escurece no hover */
            color: #31333F;
        }
        </style>

        """, unsafe_allow_html=True)