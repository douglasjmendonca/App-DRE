import streamlit as st
import tempfile
import io
from PIL import Image
# Importando os pacotes para processamento dos dados
import pandas as pd
import numpy as np
import os
import pathlib
# Importando a função DataFrame do Pandas
from pandas import DataFrame
# Importanto o datetime para construção de data
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

st.write('## Processo de Despesas Contábeis')

with st.container(): # Lista de seleção dos meses e informação do ano

    st.markdown(
        """
        #### Atenção!
        **É necessário o preenchimento dos campos mês e ano.** Sem o preenchimento correto do **mês e do ano**, *o processamento dos 
        arquivos não será realizado.* Estes campos são cruciais para garantir a precisão e a contextualização dos dados.
    """
    )
    
    # Dividindo essa parte da página em duas
    coluna1, coluna2 = st.columns(2)

    with coluna1:
        st.write("Selecione o mês do relatório")
        # Criando a lista de meses e o seletor 
        meses = [" ", "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]
        mes = st.selectbox("Mês", meses)
        st.write("O mês selecionado é: ", mes)
    with coluna2:
        st.write("Digite o ano do relatório")
        ano = st.text_input("Ano")
        st.write ("O ano informado é: ", ano)

with st.container(): # Função de data
    #Função para definir os mêses e datas que deverão ser preenchidas em cada mês que for informado no ínicio do relatório
    def preencher_data_por_mes(entrada, nome_coluna_mes, nome_coluna_data):
        meses = {
            'Janeiro': '01-15',
            'Fevereiro': '02-15',
            'Março': '03-15',
            'Abril': '04-15',
            'Maio': '05-15',
            'Junho': '06-15',
            'Julho': '07-15',
            'Agosto': '08-15',
            'Setembro': '09-15',
            'Outubro': '10-15',
            'Novembro': '11-15',
            'Dezembro': '12-15'
        }

        # Certifique-se de que o nome do mês fornecido seja válido
        if nome_coluna_mes not in meses:
            raise ValueError("Nome do mês inválido")
    
        # Obter o ano atual
        ano_atual = datetime.now().year

        # Obter a data correspondente ao mês
        data_string = f"{ano}-{meses[nome_coluna_mes]}"
        data = datetime.strptime(data_string, "%Y-%m-%d")

        # Preencher a coluna do DataFrame com a data
        entrada[nome_coluna_data] = data 

with st.container(): # Conecção ao banco de dados
    load_dotenv()  # Carrega as variáveis de ambiente do arquivo 

    def get_connection():
        gcp = {
            "host": st.secrets("host"),
            "database": st.secrets("database"),
            "user": st.secrets("user"),
            "password": st.secrets("password")
    }
        conn = psycopg2.connect(**gcp)
        return conn
    
with st.container (): # Consultas ao banco de dados
    def load_data(queries):
        conn = get_connection()
        cursor = conn.cursor()

        tables = {}
        for query_name, query in queries.items():
            cursor.execute(query)
            data = cursor.fetchall()
            tables[query_name] = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
    
        conn.close()
        return tables

    queries = {
    "cidades": "Select * from protheus.dim_classe_valor;",
    "centro_custos": "Select * from protheus.dim_centro_custos",
    "plano_contas": "Select * from protheus.dim_plano_contas",
    "fornecedor": "Select * from protheus.dim_fornecedor",
    "mapeamento": "Select * from datamaster.dre_automatiza_tratamentos"
    }

    # Carregar os dados apenas na primeira execução
    if "loaded_data" not in st.session_state:
        st.session_state.loaded_data = load_data(queries)     

    # Exibir as tabelas e transformações
    cidades = st.session_state.loaded_data["cidades"]
    centro_custos = st.session_state.loaded_data["centro_custos"]
    plano_contas = st.session_state.loaded_data["plano_contas"]
    fornecedor = st.session_state.loaded_data["fornecedor"]
    mapeamento = st.session_state.loaded_data['mapeamento']

with st.container(): # Processo de Despesas Contábeis
# Processo de Movimentação de Estoque
    def process_excel_despesas_contabeis(uploaded_file):
        
        # Carregue o arquivo Excel para um DataFrame
        aba = pd.read_excel(uploaded_file, sheet_name=['SA', 'RBC'])

        # Criar um dicionário de DataFrames, onde as chaves são os nomes das abas e os valores são os DataFrames correspondentes
        dfs = {}
        for aba_nome, df in aba.items():
            dfs[aba_nome] = df
        
        # Exibir os DataFrames de cada aba
        for aba_nome, df in dfs.items():
            print(f"DataFrame da aba '{aba_nome}':")
        
        # Construindo o banco de dados de entrada com a coluna cidade sendo preenchida com o nome das abas referente a cada uma das
        # cidades nas abas no arquivo em Excel
        entrada = pd.concat([df.assign(FONTE=aba) for aba, df in dfs.items()], ignore_index=True)

        # Criando o DataFrame de tratamento aproveitando algumas colunas dos dados de entrada
        tratamento = pd.DataFrame(entrada, columns = ['Filial','Data Lcto','CtaDebito','Valor', 'Hist Lanc', 'C Custo Deb', 'Item Conta C',
                                          'Cod Cl Val D'])
        
        # Renomeando as colunas
        tratamento.rename(columns={'Filial': 'EMPRESA', 'Data Lcto': 'DATA', 'Hist Lanc': 'HISTORICO', 'Item Conta C': 
                          'COD_FORNECEDOR'}, inplace=True)
        
        # Preencher os valores em branco na coluna COD_FORNECEDOR com o valor ZZZZZZZ
        valor = 'ZZZZZZZ'
        tratamento['COD_FORNECEDOR'] = tratamento['COD_FORNECEDOR'].fillna(valor)

        #Incluindo o código da Base
        tratamento['BASE'] = np.where(entrada['FONTE'] == 'RBC', 101, entrada['FONTE'])
        tratamento['BASE'] = np.where(entrada['FONTE'] == 'SA', 102, tratamento['BASE'])

        # Criando a coluna EE com base no nome da empresa
        tratamento['EE'] = tratamento["EMPRESA"].astype(str).str[:2]

        # Filtrando a coluna EE por meio da coluna BASE para alterar os valores quando a Base for SA
        tratamento['EE'] = np.where(entrada['FONTE'] == 'SA', '00', tratamento['EE'])

        #Criando a coluna IDCONTA
        tratamento['IDCONTA'] = tratamento['BASE'].astype(str) + tratamento['EE'] + tratamento['CtaDebito'].astype(str).str.strip().str[:11]

        # Criando a coluna CONTA vazia
        tratamento['CONTA'] = ''

        # Criando um banco de dados auxiliar para unir os valores criados do IDCONTA com o banco de dados de tratamento
        auxcon = pd.DataFrame(plano_contas,columns=["idconta","conta_contabil"])
        auxcon = pd.merge(tratamento,auxcon,left_on="IDCONTA",right_on="idconta")
        auxcon = pd.DataFrame(auxcon,columns=["idconta","conta_contabil"])

        # Construindo um dicionário com os valores de idclvl e classe de valor para utilizar no preenchimento da coluna IDCONTA
        dic_con = auxcon.set_index('idconta')['conta_contabil'].to_dict()

        # Preenchimento da coluna de CONTA com os valores do dicionário criado para cada valor correspondente na coluna IDCONTA
        tratamento['CONTA'] = tratamento['IDCONTA'].map(dic_con)

        # Criando a coluna EE com base no nome da empresa
        tratamento['EEC'] = '00'

        #Criando a coluna IDCLVL
        tratamento['IDCLVL'] = tratamento['BASE'].astype(str) + tratamento['EEC'] + tratamento['Cod Cl Val D'].astype(str).str.strip().str[:7]

        #Tratando os valores em branco na coluna Cod Cl Val D e preenchenco os valores da coluna IDCLVL 
        valoresidclvl = 'YYYYYYYYYYYY'
        tratamento['IDCLVL'] = tratamento.apply(lambda row: row['IDCLVL'] if pd.notna(row['Cod Cl Val D']) else valoresidclvl, axis=1)

        # Criando a coluna CIDADE vazia
        tratamento['CIDADE'] = ''

        # Criando um banco de dados auxiliar para unir os valores criados do IDCLVL com o banco de dados de tratamento
        auxcit = pd.DataFrame(cidades,columns=["idclvl","classe_valor"])
        auxcon = pd.merge(tratamento,auxcit,left_on="IDCLVL",right_on="idclvl")
        auxcon = pd.DataFrame(auxcon,columns=["idclvl","classe_valor"])

        # Construindo um dicionário com os valores de idclvl e classe de valor para utilizar no preenchimento da coluna IDCLVL
        dic_cit = auxcit.set_index('idclvl')['classe_valor'].to_dict()

        # Preenchimento da coluna de CIDADE com os valores do dicionário criado para cada valor correspondente na coluna IDCLVL
        tratamento['CIDADE'] = tratamento['IDCLVL'].map(dic_cit)

        # Criando a coluna EE com base no nome da empresa
        tratamento['EECC'] = tratamento["EMPRESA"].astype(str).str[:2]

        # Filtrando a coluna EECC por meio da coluna BASE para alterar os valores quando a Base for RBC
        tratamento['EECC'] = np.where(entrada['FONTE'] == 'RBC', '00', tratamento['EECC'])

        #Criando a coluna IDCC
        tratamento['IDCC'] = tratamento['BASE'].astype(str) + tratamento['EECC'] + tratamento['C Custo Deb'].astype(str).str.strip().str[:7]

        #Tratando os valores em branco na coluna Cod Cl Val D e preenchenco os valores da coluna IDCLVL 
        valoresidcc = 'XXXXXXXXXXXX'
        tratamento['IDCC'] = tratamento.apply(lambda row: row['IDCC'] if pd.notna(row['C Custo Deb']) else valoresidcc, axis=1)

        # Criando a coluna CENTRO_CUSTOS vazia
        tratamento['CENTRO_CUSTOS'] = ''

        # Criando um banco de dados auxiliar para unir os valores criados do IDCC com o banco de dados de tratamento
        auxcc = pd.DataFrame(centro_custos,columns=["idcc","centro_custos"])
        auxcc = pd.merge(tratamento,auxcc,left_on="IDCC",right_on="idcc")
        auxcc = pd.DataFrame(auxcc,columns=["idcc","centro_custos"])

        # Construindo um dicionário com os valores de idclvl e classe de valor para utilizar no preenchimento da coluna IDCC
        dic_cc = auxcc.set_index('idcc')['centro_custos'].to_dict()

        # Preenchimento da coluna de CIDADE com os valores do dicionário criado para cada valor correspondente na coluna IDCLVL
        tratamento['CENTRO_CUSTOS'] = tratamento['IDCC'].map(dic_cc)

        # Criando a coluna mapeamento no banco de dados de mapeamento
        mapeamento['mapeamento'] = mapeamento['idconta'].astype(str) + mapeamento['idfornecedor'].astype(str) + mapeamento['idcc'].astype(str) + mapeamento['idclvl'].astype(str)

        # Criando a coluna MAPEAMENTO no banco de dados de tratamento
        tratamento ['MAPEAMENTO'] = tratamento['IDCONTA'].astype(str) + tratamento['COD_FORNECEDOR'].astype(str).str[:7] + tratamento['IDCC'].astype(str) + tratamento['IDCLVL'].astype(str)

        # Criando a coluna DIRETO_CSC vazia
        tratamento['DIRETO_CSC'] = ''

        # Criando um banco de dados auxiliar para unir os valores criados do mapeamento com o banco de dados de tratamento
        auxdircsc = DataFrame(mapeamento,columns=["mapeamento","direto_csc"])
        auxdircsc = pd.merge(tratamento,auxdircsc,left_on="MAPEAMENTO",right_on="mapeamento")
        auxdircsc = DataFrame(auxdircsc,columns=["mapeamento","direto_csc"])

        # Construindo um dicionário com os valores de mapeamento e direto_csc para utilizar no preenchimento da coluna MAPEAMENTO
        dic_dircsc = auxdircsc.set_index('mapeamento')['direto_csc'].to_dict()

        # Preenchimento da coluna de DIRETO_CSC com os valores do dicionário criado para cada valor correspondente na coluna MAPEAMENTO
        tratamento['DIRETO_CSC'] = tratamento['MAPEAMENTO'].map(dic_dircsc)

        # Criando a coluna TIPO_RATEIO vazia
        tratamento['TIPO_RATEIO'] = ''

        # Criando um banco de dados auxiliar para unir os valores criados do mapeamento com o banco de dados de tratamento
        auxtrat = DataFrame(mapeamento,columns=["mapeamento","tipo_rateio"])
        auxtrat = pd.merge(tratamento,auxtrat,left_on="MAPEAMENTO",right_on="mapeamento")
        auxtrat = DataFrame(auxtrat,columns=["mapeamento","tipo_rateio"])

        # Construindo um dicionário com os valores de mapeamento e direto_csc para utilizar no preenchimento da coluna MAPEAMENTO
        dic_trat = auxtrat.set_index('mapeamento')['tipo_rateio'].to_dict()

        # Preenchimento da coluna de TIPO_RATEIO com os valores do dicionário criado para cada valor correspondente na coluna MAPEAMENTO
        tratamento['TIPO_RATEIO'] = tratamento['MAPEAMENTO'].map(dic_trat)

        # Criando a coluna EEI com base no nome da empresa
        tratamento['EEI'] = tratamento["EMPRESA"].astype(str).str[1:4]
        
        #Criando a coluna IDITEM
        tratamento['IDITEM'] = tratamento['BASE'].astype(str) + tratamento['COD_FORNECEDOR'].astype(str).str.strip().str[:7]

        # Criando a coluna CENTRO_CUSTOS vazia
        tratamento['DETALHAMENTO'] = ''

        # Criando um banco de dados auxiliar para unir os valores criados do IDITEM com o banco de dados de tratamento
        auxit = DataFrame(fornecedor,columns=["iditem","a2_nome"])
        auxit = pd.merge(tratamento,auxit,left_on="IDITEM",right_on="iditem")
        auxit = DataFrame(auxit,columns=["iditem","a2_nome"])

        # Construindo um dicionário com os valores de iditem e classe de valor para utilizar no preenchimento da coluna a2_nome
        dic_it = auxit.set_index('iditem')['a2_nome'].to_dict()

        # Criando um banco de dados auxiliar para unir os valores criados do mapeamento com o banco de dados de tratamento
        auxdet = DataFrame(mapeamento,columns=["mapeamento","detalhamento"])
        auxdet = pd.merge(tratamento,auxdet,left_on="MAPEAMENTO",right_on="mapeamento")
        auxdet = DataFrame(auxdet,columns=["mapeamento","detalhamento"])

        # Construindo um dicionário com os valores de mapeamento e direto_csc para utilizar no preenchimento da coluna MAPEAMENTO
        dic_det = auxdet.set_index('mapeamento')['detalhamento'].to_dict()

        # Preenchimento da coluna de TIPO_RATEIO com os valores do dicionário criado para cada valor correspondente na coluna MAPEAMENTO
        tratamento['DETALHAMENTO'] = tratamento['MAPEAMENTO'].map(dic_det)

        # Preenchimento dos valores em branco da coluna de DETALHAMENTO com os valores do dicionário criado para cada valor correspondente na coluna IDITEM
        detalhamentobranco = tratamento['IDITEM'].map(dic_it)
        tratamento['DETALHAMENTO'] = tratamento['DETALHAMENTO'].fillna(detalhamentobranco)

        # Criando o DataFrame de saída aproveitando algumas colunas do dados de tratamento
        saida = pd.DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                        'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                         'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                        'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])
        
        # Colocandos os dados tratados na coluna VALOR
        saida['VALOR_REF'] = tratamento['Valor']

        # Colocando valor padrão na coluna FONTE
        saida['FONTE'] = 'DESPESAS CONTÁBIL'

        # Colocando valor na coluna COD_FILIAL
        saida['COD_FILIAL'] = tratamento["EMPRESA"].astype(str).str[:2]

        # Colocando valor na coluna NOME_FILIAL
        saida['NOME_FILIAL'] = tratamento["EMPRESA"].astype(str).str[5:]

        # Colocando valor padrão na coluna MULTIPLICADOR
        saida['MULTIPLICADOR'] = -1

        # Transformando o valor para preencer a coluna VALOR_REALIZADO
        saida['VALOR_REALIZADO'] = saida['VALOR_REF']*saida['MULTIPLICADOR']

        # Salve o DataFrame em um arquivo Excel em memória
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            saida.to_excel(writer, index=False)

        return output

with st.container(): #Funções e botões para o processamento do arquivo

    coluna1, coluna2 = st.columns(2)    
    with coluna1:     
        # Criar um campo de upload
        uploaded_file = st.file_uploader("Selecione um arquivo Excel", type="xlsx")
        
            
    # Nome padrão do arquivo
    nome_padraof = 'despesas_contabeis_DRE_' + mes + ano + '.xlsx'

    with coluna2:
        st.write('Quando o processo estiver finalizado, aparecerá um botão para clicar e baixar o arquivo')
        if uploaded_file:
            
            # Processar o arquivo e obter o arquivo Excel processado em memória
            processed_file = process_excel_despesas_contabeis(uploaded_file)
                
            # Exibir uma mensagem de sucesso
            st.success('Processamento concluído com sucesso!')

             # Botão para baixar o arquivo processado
            st.download_button(
                    label="Baixar Arquivo Processado",
                    data=processed_file.getvalue(),
                    file_name= nome_padraof,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
