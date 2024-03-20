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

st.write('## Processo de Movimentação de Estoque')

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
    }

    
    # Carregar os dados apenas na primeira execução
    if "loaded_data" not in st.session_state:
        st.session_state.loaded_data = load_data(queries)     

    # Exibir as tabelas e transformações
    cidades = st.session_state.loaded_data["cidades"]
    centro_custos = st.session_state.loaded_data["centro_custos"]
    plano_contas = st.session_state.loaded_data["plano_contas"]

with st.container(): # Processo de Movimentação de Estoque
# Processo de Movimentação de Estoque
    def process_excel_estoque(uploaded_file):
        
        # Carregue o arquivo Excel para um DataFrame
        entrada = pd.read_excel(uploaded_file)
        
        # Excluindo os dados onde a coluna B1_XCTB possui valor N
        entrada = entrada[entrada['B1_XCTB'] == 'S']
        
        # Criando o DataFrame de tratamento aproveitando algumas colunas dos dados de entrada
        tratamento = pd.DataFrame(entrada, columns = ['COD','PRODUTO','QTDE','CUSTO_MEDIO', 'MED_NF_ENT', 'DATA_MOV','OBS_ID_OS'])
        
        # Criando outro dataframe com os mesmo dados anteriores mas adicionando as colunas que serão calculadas
        tratamento = pd.DataFrame(tratamento, 
                columns = ['COD', 'PRODUTO', 'QTDE', 'CUSTO_MEDIO', 'MED_NF_ENT','VLR_UNIT', 'VLR_ORIGINAL', 'VLR_UNIT_CORRIG',
                          'VLR_CORRIGIDO', 'EMPRESA', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 'COD_FORNECEDOR',
                          'IDCC','CENTRO_CUSTOS', 'IDCLVL','CIDADE', 'IDCONTA','CONTA', 'DETALHAMENTO'])
        
        # Acrescentando os valores das colunas que tiveram apenas os nomes alterados
        tratamento['DATA'] = entrada['DATA_MOV']
        tratamento['DOCUMENTO'] = entrada['OBS_ID_OS']
        
        tratamento['HISTORICO'] = entrada['PRODUTO']+" "+ entrada['DESC_PRINC']
        
        # Construção do código IDCC seguindo o padrão do BD e utilizando os valores da coluna D3_CC da tabela de entrada
        tratamento["IDCC"] = "102"+entrada['FILIAL'].astype(str).str.zfill(4).str[:2]+entrada["D3_CC"].astype(str).str[:7]
        
        # Criando um banco de dados auxiliar para unir os valores criados do IDCC com o banco de dados de tratamento
        auxcc = pd.DataFrame(centro_custos,columns=["idcc","centro_custos"])
        auxcc = pd.merge(tratamento,auxcc,left_on="IDCC",right_on="idcc")
        auxcc = pd.DataFrame(auxcc,columns=["idcc","centro_custos"])
        
        # Construindo um dicionário com os valores de idcc e centro de custos para utilizar no preenchimento da coluna centro_custos
        dic_cc = auxcc.set_index('idcc')['centro_custos'].to_dict()
        
        # Preenchimento da coluna de centro_custos com os valores do dicionário criado para cada valor correspondente na coluna IDCC
        tratamento['CENTRO_CUSTOS'] = tratamento['IDCC'].map(dic_cc)
        
        # Construção do dicionário para colocar o nome dos produtos padronizados na coluna de detalhamento
        detalhamento = {
            'CABO DE REDE UTP CAT 5 BRANCO': 'CABO DE REDE',
            'CABO DROP (MONOFIBRA) FLAT LOW': 'CABO DROP',
            'CABO OPTICO CONECTORIZADO DROP COMPACTO LOW FRICITION BLI-CM-01-AR-LSZH': 'CABO OPTICO CONECTORIZADO',
            'CONECTOR PRE POLIDO CLICK RAPIDO SC/APC': 'CONECTOR PRE POLIDO',
            'CONECTOR PRE POLIDO SC/APC ROSQ TIPO B': 'CONECTOR PRE POLIDO',
            'CONECTOR PRE POLIDO SC/UPC': 'CONECTOR PRE POLIDO',
            'ONT GPON G-1425G-A NOKIA': 'ONT GPON',
            'ONT GPON NOVA PHYHOME AC1200': 'ONT GPON',
            'ONT WIFI NOVA INTELBRAS': 'ONT WIFI',
            'ONT ZTE NOVA GPON WIFI AC1200 MBPS': 'ONT GPON',
            'ONU EPON NOVA': 'ONU EPON NOVA',
            'ONU GEPON': 'ONU GEPON',
            'ONU GPON': 'ONU GPON',
            'ONU XPON HIBRIDA': 'ONU XPON',
            'ROTEADOR MERCUSYS N MW301R 2 ANTENAS – ATÉ 50 MBPS': 'ROTEADOR MERCUSYS 2 ANTENAS',
            'ROTEADOR MULT. NOVO ZTE 4 ANT. BRANCO': 'ROTEADOR MULTILASER 4 ANTENAS NOVO',
            'ROTEADOR MULTILASER NOVO 2 ANTENAS – ATÉ 50 MBPS': 'ROTEADOR MULTILASER 2 ANTENAS NOVO',
            'SIMCARD (MVNO)': 'SIMCARD (MVNO)',
            'SUPORTE CAVALETE': 'SUPORTE CAVALETE',
            'SUPORTE TELHA 3/4': 'SUPORTE TELHA',
            'TUBO 3 METROS P/ NET WIRELLES': 'TUBO 3 METROS'
            }
        
        # Preenchimento da coluna de detalhamento com os valores do dicionário criado para cada valor correspondente na coluna produto
        tratamento['DETALHAMENTO'] = tratamento['PRODUTO'].map(detalhamento)
        
        # Construção do código IDCLVL seguindo o padrão do BD e utilizando os valores da coluna D3_CLVL da tabela de entrada
        tratamento['IDCLVL'] = "102"+"00"+entrada['D3_CLVL'].astype(str).str[:7]
        
        # Criando um banco de dados auxiliar para unir os valores criados do IDCLVL com o banco de dados de tratamento
        auxcid = pd.DataFrame(cidades,columns=["idclvl","classe_valor"])
        auxcid = pd.merge(tratamento,auxcid,left_on="IDCLVL",right_on="idclvl")
        auxcid = pd.DataFrame(auxcid, columns = ["idclvl","classe_valor"])
        
        # Construindo um dicionário com os valores de idclvl e classe de valor para utilizar no preenchimento da coluna cidade
        dic_cid = auxcid.set_index('idclvl')['classe_valor'].to_dict()
        
        # Preenchimento da coluna de cidade com os valores do dicionário criado para cada valor correspondente na coluna IDCLVL
        tratamento['CIDADE'] = tratamento['IDCLVL'].map(dic_cid)
        
        # Construção do código IDCONTA seguindo o padrão do BD e utilizando os valores da coluna D3_CC da tabela de entrada
        tratamento['IDCONTA'] = "102"+"00"+entrada['CONTA_RESULTADO'].astype(str).str[:11]
        
        # Criando um banco de dados auxiliar para unir os valores criados do IDCONTA com o banco de dados de tratamento
        auxconta = pd.DataFrame(plano_contas,columns=["idconta","conta_contabil"])
        auxconta = pd.merge(tratamento,auxconta,left_on="IDCONTA",right_on="idconta")
        auxconta = pd.DataFrame(auxconta, columns = ["idconta","conta_contabil"])
            
        # Construindo um dicionário com os valores de idconta e conta contabil para utilizar no preenchimento da coluna conta
        dic_conta = auxconta.set_index('idconta')['conta_contabil'].to_dict()
        
        # Preenchimento da coluna de conta com os valores do dicionário criado para cada valor correspondente na coluna IDCONTA
        tratamento['CONTA'] = tratamento['IDCONTA'].map(dic_conta)
        
        # Criando o DataFrame de saída organizando as colunas que serão utilizadas dos dados de tratamento
        saida = DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA','EMPRESA', 'DATA', 'VALOR_REF','DOCUMENTO','HISTORICO',
                                        'COD_FORNECEDOR', 'CENTRO_CUSTOS','CIDADE','CONTA','DETALHAMENTO'])
        
        # Criando o DataFrame de saída aproveitando algumas colunas do dados de tratamento
        saida = pd.DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                            'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])
        
        # Colocando valor padrão na coluna FONTE
        saida['FONTE'] = 'MOVIMENTACAO ESTOQUE'
        
        # Colocando valor padrão na coluna DIRETO_CSC
        saida['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
        
        # Colocando valor padrão na coluna TIPO_RATEIO
        saida['TIPO_RATEIO'] = 'OK'
        
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
    nome_padraof = 'movimentacao_estoque_DRE_' + mes + ano + '.xlsx'

    with coluna2:
        st.write('Quando o processo estiver finalizado, aparecerá um botão para clicar e baixar o arquivo')
        if uploaded_file:
            
            # Processar o arquivo e obter o arquivo Excel processado em memória
            processed_file = process_excel_estoque(uploaded_file)
                
            # Exibir uma mensagem de sucesso
            st.success('Processamento concluído com sucesso!')

             # Botão para baixar o arquivo processado
            st.download_button(
                    label="Baixar Arquivo Processado",
                    data=processed_file.getvalue(),
                    file_name= nome_padraof,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        