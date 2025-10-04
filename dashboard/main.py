import streamlit as st
import pandas as pd
import numpy as np
import datetime
import joblib
import oracledb
from time import sleep

# =========================
# CONEXÃO BANCO
# =========================
conn = oracledb.connect(
    user="rm562274",
    password="090402",
    dsn="oracle.fiap.com.br:1521/ORCL"
)
cursor = conn.cursor()


# =========================
# FUNÇÕES
# =========================
def gerar_fatos(reg: int = 1):
    """Gera dados simulados e insere na tabela fato."""
    sistema = pd.read_sql('SELECT * FROM DM_SISTEMAS', conn)
    maquinario = pd.read_sql('SELECT * FROM DM_MAQUINA', conn)

    ids_maquinas_validos = maquinario['ID_MAQUINA'].tolist()
    ids_sistema_validos = sistema['ID_SISTEMA'].tolist()

    registros = {
        'id_sistema': np.random.choice(ids_sistema_validos, reg),
        'id_maquina': np.random.choice(ids_maquinas_validos, reg),
        'timestamp_registro': datetime.datetime.now(),
        'vl_temperatura': np.random.normal(80, 15, reg).round(2),
        'vl_vibracao': np.random.normal(4, 2, reg).round(2),
        'vl_corrente': np.random.normal(150, 5, reg).round(2)
    }
    df_temp_reg = pd.DataFrame(registros)
    dados_para_inserir = [tuple(row) for row in df_temp_reg.itertuples(index=False)]
    cursor.executemany(
        '''
        INSERT INTO FT_REG_SENSORES (ID_SISTEMA, ID_MAQUINA, TIMESTAMP_REGISTRO, VL_TEMPERATURA, VL_VIBRACAO, VL_CORRENTE)
        VALUES(:1, :2, :3, :4, :5, :6)
        ''', dados_para_inserir
    )
    conn.commit()
    return df_temp_reg


def consultar_dados(conexao, num_linhas: int = 50):
    """Consulta últimos registros."""
    if conexao is None:
        return pd.DataFrame()

    query = f'''
        SELECT * FROM (
            SELECT * FROM FT_REG_SENSORES ORDER BY TIMESTAMP_REGISTRO DESC
        ) WHERE ROWNUM <= {num_linhas}
    '''
    df_consulta = pd.read_sql(query, conexao)
    return df_consulta


def nivel_risco(dataframe, coluna_risco):
    """Insere alertas conforme nível de risco detectado."""
    risco_iminente_count = len(dataframe[dataframe[coluna_risco] == 'Risco Iminente'])
    percentual_risco = (risco_iminente_count / len(dataframe)) * 100

    if percentual_risco > 25:  # entra para os três cenários
        dataframe.columns = dataframe.columns.str.upper()
        dataframe['TIMESTAMP_ALERTA'] = datetime.datetime.now()
        dataframe['NIVEL'] = 'RISCO ALTO'
        dataframe['STATUS'] = ''
        if 'VL_TEMPERATURA' in dataframe.columns:
            dataframe['TIPO_ALERTA'] = 'TEMPERATURA'
        elif 'VL_VIBRACAO' in dataframe.columns:
            dataframe['TIPO_ALERTA'] = 'VIBRACAO'
        elif 'VL_CORRENTE' in dataframe.columns:
            dataframe['TIPO_ALERTA'] = 'CORRENTE'

        dataframe = dataframe[['ID_MAQUINA', 'TIMESTAMP_ALERTA', 'TIPO_ALERTA', 'NIVEL', 'STATUS']]
        dataframe = dataframe.drop_duplicates().reset_index(drop=True)

        dados_para_inserir = [tuple(row) for row in dataframe.itertuples(index=False)]
        cursor.executemany(
            '''
            INSERT INTO FB_ALERTAS (
                ID_MAQUINA, TIMESTAMP_ALERTA, TIPO_ALERTA, NIVEL, STATUS
            )
            VALUES(:1, :2, :3, :4, :5)
            ''', dados_para_inserir
        )
        conn.commit()
        return dataframe
    return pd.DataFrame()


def gerar_alertas():
    """Aplica modelos ML e gera alertas no banco."""
    df_analise = pd.read_sql(
        f"""
        SELECT * FROM FT_REG_SENSORES
        WHERE TO_CHAR(TIMESTAMP_REGISTRO, 'DD/MM/YY HH24:MI:SS') >= 
        '{(datetime.datetime.now()-datetime.timedelta(minutes=20)).strftime('%d/%m/%y %H:%M:%S')}'
        """, conn
    )
    df_analise.columns = df_analise.columns.str.lower()

    df_temp = df_analise[['id_sistema', 'id_maquina', 'timestamp_registro', 'vl_temperatura']].copy()
    df_vibracao = df_analise[['id_sistema', 'id_maquina', 'timestamp_registro', 'vl_vibracao']].copy()
    df_corrente = df_analise[['id_sistema', 'id_maquina', 'timestamp_registro', 'vl_corrente']].copy()

    ml_corrente = joblib.load(r"C:\Users\Davi\Documents\Projetos\FIAP\FASE 6\Enterprise Challenge Fase 4\ml\Treinamento de ML\RFC_Corrt.joblib")
    ml_temperatura = joblib.load(r"C:\Users\Davi\Documents\Projetos\FIAP\FASE 6\Enterprise Challenge Fase 4\ml\Treinamento de ML\RFC_Temp.joblib")
    ml_vibracao = joblib.load(r"C:\Users\Davi\Documents\Projetos\FIAP\FASE 6\Enterprise Challenge Fase 4\ml\Treinamento de ML\RFC_Vibr.joblib")

    df_temp['RISCO'] = ml_temperatura.predict(df_temp[['vl_temperatura']])
    df_vibracao['RISCO'] = ml_vibracao.predict(df_vibracao[['vl_vibracao']])
    df_corrente['RISCO'] = ml_corrente.predict(df_corrente[['vl_corrente']])

    alertas_temp = nivel_risco(df_temp, 'RISCO')
    alertas_vibracao = nivel_risco(df_vibracao, 'RISCO')
    alertas_corrente = nivel_risco(df_corrente, 'RISCO')

    return pd.concat([alertas_temp, alertas_vibracao, alertas_corrente], ignore_index=True)


# =========================
# STREAMLIT APP
# =========================
st.set_page_config(page_title="Monitoramento IoT", layout="wide")

st.title("📊 Monitoramento de Sensores Industriais")

menu = st.sidebar.radio("Navegação", ["Gerar Fatos", "Consultar Dados", "Gerar Alertas"])

if menu == "Gerar Fatos":
    st.subheader("🔧 Gerar Dados Simulados")
    qtd = st.number_input("Quantidade de registros a gerar:", min_value=1, max_value=100, value=5)
    if st.button("Gerar"):
        df = gerar_fatos(qtd)
        st.success("Dados inseridos com sucesso!")
        st.dataframe(df)

elif menu == "Consultar Dados":
    st.subheader("📥 Últimos Registros do Banco")
    qtd = st.slider("Número de registros:", 10, 200, 50)
    df = consultar_dados(conn, qtd)
    st.dataframe(df)

elif menu == "Gerar Alertas":
    st.subheader("🚨 Verificação de Risco e Alertas")
    if st.button("Rodar Análise de Risco"):
        df_alertas = gerar_alertas()
        if df_alertas.empty:
            st.info("Nenhum alerta gerado.")
        else:
            st.warning("Foram gerados novos alertas!")
            st.dataframe(df_alertas)
