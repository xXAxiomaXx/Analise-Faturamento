import streamlit as st
import pandas as pd
import gspread
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re

# 1. Configuração da ligação ao Google Sheets
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

@st.cache_resource(show_spinner="A estabelecer ligação ao Google Sheets...")
def conectar_google_sheets():
    # Lê o texto dos Secrets e converte para dicionário
    credenciais_dict = json.loads(st.secrets["google_credentials_json"])
    
    # Usa a função nativa do gspread exclusiva para dicionários
    client = gspread.service_account_from_dict(credenciais_dict)
    
    # ATENÇÃO: Substitua pelos seus IDs reais
    sheet1 = client.open_by_key("1iqIw5mXr9oibMC8tZq7Pti4cgjIeGe-SxrVloJcsaQ4")
    sheet2 = client.open_by_key("1K_QoptXhXUY4E3_6cbhb0G1YWxaGjVIgy6bq_PD3U5w")
    
    return sheet1, sheet2

# 2. Função para extrair mês e ano do nome do separador (aba)
def parse_date_from_string(text):
    meses = {
        'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR': 4, 'MAI': 5, 'JUN': 6,
        'JUL': 7, 'AGO': 8, 'SET': 9, 'OUT': 10, 'NOV': 11, 'DEZ': 12
    }
    
    match = re.search(r'([A-Z]{3})\s+(\d{2})', text.upper())
    if match:
        mes_str = match.group(1)
        ano_str = match.group(2)
        mes = meses.get(mes_str, 0)
        
        ano = int(ano_str) + 2000 if int(ano_str) < 100 else int(ano_str)
        
        if mes != 0:
            return datetime(ano, mes, 1), mes_str, ano
            
    return None, None, None

# 3. Função Principal da Aplicação
def main():
    st.set_page_config(layout="wide", page_title="Análise FAT SUS")
    st.title("Painel de Análise - Pendências Médicas")
    
    # Calcular a data limite (1º dia do mês de 4 meses atrás)
    hoje = datetime.now()
    quatro_meses_atras = hoje - relativedelta(months=4)
    data_limite = datetime(quatro_meses_atras.year, quatro_meses_atras.month, 1)
    
    st.info(f"💡 A apresentar dados combinados a partir de: **{quatro_meses_atras.strftime('%m/%Y')}**")

    try:
        sheet1, sheet2 = conectar_google_sheets()
        # Junta os separadores de ambas as folhas numa única lista para processamento
        todas_worksheets = sheet1.worksheets() + sheet2.worksheets()
    except Exception as e:
        st.error(f"Erro ao ligar ao Google Sheets. Verifique os IDs e o ficheiro credenciais.json. Detalhe: {e}")
        return

    # Dicionários para agrupar os dados do mesmo mês
    agrupamento_dfs = {}
    nomes_abas = {}
    
    progress_text = "A ler e a combinar dados das folhas de cálculo..."
    my_bar = st.progress(0, text=progress_text)
    total_sheets = len(todas_worksheets)

    # 4. Processar cada separador
    for i, ws in enumerate(todas_worksheets):
        my_bar.progress((i + 1) / total_sheets, text=f"A analisar separador: {ws.title}")
        titulo_aba = ws.title
        
        if "FICHAS CIRURGICAS" in titulo_aba.upper():
            continue
            
        data_dt, mes_str, ano = parse_date_from_string(titulo_aba)
        
        # Apenas processa se a data for maior ou igual à data limite
        if data_dt and data_dt >= data_limite:
            
            dados = ws.get_all_values()
            if not dados or len(dados) < 2:
                continue
            
            headers_originais = dados[0]
            novos_headers = []
            contagem = {}
            
            for col in headers_originais:
                col_nome = str(col).strip().upper()
                if not col_nome:
                    col_nome = "VAZIO" 
                
                if col_nome in contagem:
                    contagem[col_nome] += 1
                    novos_headers.append(f"{col_nome}_{contagem[col_nome]}")
                else:
                    contagem[col_nome] = 0
                    novos_headers.append(col_nome)
                    
            df = pd.DataFrame(dados[1:], columns=novos_headers)
            
            # Padronizar o nome da coluna de pendências para garantir que empilham corretamente
            if 'PENDENCIAS' in df.columns:
                df.rename(columns={'PENDENCIAS': 'PENDÊNCIAS'}, inplace=True)
                
            if 'NOME' in df.columns and 'PENDÊNCIAS' in df.columns:
                # Se o mês ainda não existir no agrupamento, criamos uma lista para ele
                if data_dt not in agrupamento_dfs:
                    agrupamento_dfs[data_dt] = []
                    nomes_abas[data_dt] = f"{mes_str} {ano}"
                    
                # Adicionamos os dados deste separador à lista do respetivo mês
                agrupamento_dfs[data_dt].append(df)

    my_bar.empty() 

    if not agrupamento_dfs:
        st.warning("Nenhum dado encontrado a partir do mês limite. Verifique as datas e os cabeçalhos.")
        return
        
    # 5. Empilhar (concatenar) as folhas e ordenar cronologicamente
    dados_por_mes = {}
    for dt, dfs in agrupamento_dfs.items():
        # pd.concat junta todos os DataFrames daquele mês num só, um por baixo do outro
        df_combinado = pd.concat(dfs, ignore_index=True)
        tab_name = nomes_abas[dt]
        dados_por_mes[tab_name] = (dt, df_combinado)

    sorted_tabs = sorted(dados_por_mes.items(), key=lambda x: x[1][0])
    tab_names = [t[0] for t in sorted_tabs]
    
    tabs = st.tabs(tab_names)
    
    ignore_list = ['OK', 'FALSE', 'TRUE', 'FICHA CIRURGICA', 'DNV', 'NENHUMA', 'NAN', '']
    
    # 6. Apresentar os dados em cada separador
    for i, tab_name in enumerate(tab_names):
        with tabs[i]:
            df = sorted_tabs[i][1][1]
            
            st.subheader(f"Dados combinados referentes a: {tab_name}")
            
            df['PENDÊNCIAS'] = df['PENDÊNCIAS'].astype(str).str.strip().str.upper()
            
            df_pendencias = df[~df['PENDÊNCIAS'].isin(ignore_list)]
            
            if df_pendencias.empty:
                st.success("🎉 Nenhuma pendência médica encontrada para este mês nas folhas processadas!")
                continue
                
            resumo = df_pendencias.groupby('PENDÊNCIAS')['NOME'].apply(list).reset_index()
            resumo['QTD'] = resumo['NOME'].apply(len)
            
            resumo = resumo.sort_values(by='QTD', ascending=False)
            
            st.markdown("### 🩺 Pendências por Médico")
            
            for _, row in resumo.iterrows():
                medico = row['PENDÊNCIAS']
                qtd = row['QTD']
                pacientes = row['NOME']
                
                with st.expander(f"**{medico}** — {qtd} paciente(s) com pendência"):
                    for paciente in pacientes:
                        st.markdown(f"- {paciente}")

if __name__ == "__main__":

    main()


