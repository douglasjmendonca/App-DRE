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

st.write('## Processo de Faturamento e Impostos')

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

with st.container(): # Processo de Faturamento e Imposotos
# Processo de Faturamento e Impostos
    def process_excel_faturamento(uploaded_file):
        
        # Carregue o arquivo Excel para um DataFrame
        entrada = pd.read_excel(uploaded_file)

        # Incluindo a coluna DATA no Data Frame de entrada
        entrada = pd.DataFrame(entrada, columns = ['CIDADE','ESTRATIFICADO', 'VALOR', 'GRUPO', 'DATA'])

        # Preencher a coluna DATA com o mês informado no início do relatório
        preencher_data_por_mes(entrada, mes, 'DATA')

    # Tratamento do Dados de Faturamento
        # Criando o DataFrame de tratamento aproveitando algumas colunas dos dados de entrada
        tratamento = pd.DataFrame(entrada, columns = ['CIDADE','IDCLVL','DATA','VALOR', 'FILIAL', 'CONTA', 'DESCRICAO'])

        # Alterando o nome de Divinópolis para DIVINOPOLIS REGIONAL
        tratamento['CIDADE'] = np.where(tratamento['CIDADE'] == 'DIVINOPOLIS', 'DIVINOPOLIS REGIONAL', tratamento['CIDADE'])

        # Criando um banco de dados auxiliar para unir os valores de CIDADE com o banco de dados de tratamento
        auxcid = cidades[cidades['base'] == 'ProtheusSA'].copy()
        auxcid = pd.DataFrame(auxcid,columns=["idclvl","classe_valor"])
        auxcid = pd.merge(tratamento,auxcid,left_on="CIDADE",right_on="classe_valor")
        auxcid = pd.DataFrame(auxcid,columns=["idclvl","classe_valor"])

        # Construindo um dicionário com os valores de idclvl e classe de valor para utilizar no preenchimento da coluna IDCLVL
        dic_cid = auxcid.set_index('classe_valor')['idclvl'].to_dict()

        # Preenchimento da coluna de IDCLVL com os valores do dicionário criado para cada valor correspondente na coluna NCIDADE
        tratamento['IDCLVL'] = tratamento['CIDADE'].map(dic_cid)

        # Alterando os valores da coluna FILIAL para os valores da coluna GRUPO e colocando em letra maiúscula
        tratamento ['FILIAL'] = entrada ['GRUPO'].str.upper()

        # Alterando os valores da coluna DESCRICAO para os valores da coluna ESTRATIFICADO e colocando em letra maiúscula
        tratamento ['DESCRICAO'] = entrada ['ESTRATIFICADO'].str.upper()

        # Os valore da coluna CONTA são preenchidos com base nos valores de referência da coluna ESTRATIFICADO do banco de entrada.
        # Porém, os valores devem ser transformados e agrupados de acordo com os tipos de conta disponíveis.
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Disponibilidade', 'DISPONIBILIDADE', entrada['ESTRATIFICADO'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Locação', 'LOCAÇÃO DE BENS E MÓVEIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Aluguel de Equipamento', 'LOCAÇÃO DE BENS E MÓVEIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Locação de Infra', 'LOCAÇÃO DE BENS E MÓVEIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Mensalidade Pay TV', 'NF MENSALIDADE', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'SCM', 'NF SCM', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'SCM 1', 'NF SCM', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Serv.Terceiros Tel. Móvel MVNO', 'PRESTACAO DE SERVICOS DE TELEFONIA MOVEL ', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Lançamentos Financeiros MVNO', 'PRESTACAO DE SERVICOS DE TELEFONIA MOVEL ', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Assistência Avançada', 'SERVIÇOS COMPLEMENTARES', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Autenticação do Sistema', 'SERVIÇOS COMPLEMENTARES', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Master Pet', 'SERVIÇOS COMPLEMENTARES', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Serviço Complementar', 'SERVIÇOS COMPLEMENTARES', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Serviços Técnicos', 'SERVIÇOS COMPLEMENTARES', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Combo Digital', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'DEEZER', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'KASPERSKY', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Master Resolve', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Panda Antivirus', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'PLAY HUB 1 APP', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'PLAY HUB 2 APP', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'PRIME PASS I', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'PRIME PASS II', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'PRIME PASS III', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'YOU CAST 70 CANAIS', 'SERVIÇOS DIGITAIS', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'SVA sobre MVNO', 'SVA SOBRE MVNO', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Acesso - SVA', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Cloud 20', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Cloud 5', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Internet', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Lançamentos Financeiros', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Master E-mail 2', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Master E-mail 4', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Master E-mail 6', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'Master Gourmand', 'VENDAS DE INTERNET', tratamento['CONTA'])
        tratamento['CONTA'] = np.where(entrada['ESTRATIFICADO'] == 'PSCI', 'VENDAS DE INTERNET', tratamento['CONTA'])

        # Criando o DataFrame para o tratamento dos impostos aproveitando as colunas do dados de tratamento
        tratamento_impostos = DataFrame(tratamento, columns = ['CIDADE', 'IDCLVL', 'DATA', 'VALOR', 'FILIAL', 'CONTA', 'DESCRICAO', 
                                                        'ICMS', 'PIS', 'COFINS', 'FUST', 'FUNTEL', 'CSLL', 'IR'])
        
        # Classe para representar um imposto
        class Imposto:
            def __init__(self, aliquota):
                self.aliquota = aliquota

            def aplicar_imposto(self, valor):
                return valor * (self.aliquota / 100)
        
        # Construindo as regras para a aplicação da alíquota de ICMS
        tratamento_impostos['ICMS'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'NF SCM' or x == 'NF MENSALIDADE')

        # Criar instâncias da classe Imposto para cada tipo de imposto com suas condições
        icms = Imposto(aliquota=18)

        #Aplicar a função lambda a cada linha do DataFrame para calcular o valor do ICMS com base na condição e no objeto Imposto
        tratamento_impostos['VALOR_ICMS'] = tratamento_impostos.apply(lambda row: icms.aplicar_imposto(row['VALOR']) if row['ICMS'] 
                                                                else row['ICMS'], axis=1)
        
        # Construindo as regras para a aplicação da alíquota de PIS
        tratamento_impostos['PIS'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'NF SCM' or x == 'NF MENSALIDADE' or 
                                                                    x == 'DISPONIBILIDADE' or x == 'LOCAÇÃO DE BENS E MÓVEIS'
                                                                or x == 'SERVIÇOS COMPLEMENTARES' or x == 'SERVIÇOS DIGITAIS'
                                                                or x == 'SVA SOBRE MVNO' or x == 'VENDAS DE INTERNET')
        
        # Criar instâncias da classe Imposto para cada tipo de imposto com suas condições
        pis = Imposto(aliquota=0.65)

        tratamento_impostos['VALOR_PIS'] = tratamento_impostos['VALOR'] - tratamento_impostos['VALOR_ICMS']

        # Aplicar a função lambda a cada linha do DataFrame para calcular o valor do PIS com base na condição e no objeto Imposto
        tratamento_impostos['VALOR_PIS'] = tratamento_impostos.apply(lambda row: pis.aplicar_imposto(row['VALOR_PIS']) if row['PIS'] 
                                                                else row['PIS'], axis=1)
        
        # Construindo as regras para a aplicação da alíquota de COFINS
        tratamento_impostos['COFINS'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'NF SCM' or x == 'NF MENSALIDADE' or 
                                                                    x == 'DISPONIBILIDADE' or x == 'LOCAÇÃO DE BENS E MÓVEIS'
                                                                or x == 'SERVIÇOS COMPLEMENTARES' or x == 'SERVIÇOS DIGITAIS'
                                                                or x == 'SVA SOBRE MVNO' or x == 'VENDAS DE INTERNET')
        
        # Criar instâncias da classe Imposto para cada tipo de imposto com suas condições
        cofins = Imposto(aliquota=3)

        tratamento_impostos['VALOR_COFINS'] = tratamento_impostos['VALOR'] - tratamento_impostos['VALOR_ICMS']

        # Aplicar a função lambda a cada linha do DataFrame para calcular o valor do COFINS com base na condição e no objeto Imposto
        tratamento_impostos['VALOR_COFINS'] = tratamento_impostos.apply(lambda row: cofins.aplicar_imposto(row['VALOR_COFINS']) 
                                                                    if row['COFINS'] else row['COFINS'], axis=1)
        
        # Construindo as regras para a aplicação da alíquota de FUST
        tratamento_impostos['FUST'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'NF SCM' or x == 'NF MENSALIDADE' or 
                                                                    x == 'DISPONIBILIDADE'or x == 'SERVIÇOS COMPLEMENTARES' 
                                                                    or x == 'SERVIÇOS DIGITAIS' or x == 'SVA SOBRE MVNO')
        

        # Criar instâncias da classe Imposto para cada tipo de imposto com suas condições
        fust = Imposto(aliquota=1)

        tratamento_impostos['VALOR_FUST'] = tratamento_impostos['VALOR'] - tratamento_impostos['VALOR_ICMS'] - tratamento_impostos['VALOR_PIS'] - tratamento_impostos['VALOR_COFINS']

        # Aplicar a função lambda a cada linha do DataFrame para calcular o valor do FUST com base na condição e no objeto Imposto
        tratamento_impostos['VALOR_FUST'] = tratamento_impostos.apply(lambda row: fust.aplicar_imposto(row['VALOR_FUST']) 
                                                                    if row['FUST'] else row['FUST'], axis=1)
        
        # Construindo as regras para a aplicação da alíquota de FUST
        tratamento_impostos['FUNTEL'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'NF SCM' or x == 'NF MENSALIDADE' or 
                                                                    x == 'DISPONIBILIDADE'or x == 'SERVIÇOS COMPLEMENTARES' 
                                                                    or x == 'SERVIÇOS DIGITAIS' or x == 'SVA SOBRE MVNO')
        
        # Criar instâncias da classe Imposto para cada tipo de imposto com suas condições
        funtel = Imposto(aliquota=0.5)

        tratamento_impostos['VALOR_FUNTEL'] = tratamento_impostos['VALOR'] - tratamento_impostos['VALOR_ICMS'] - tratamento_impostos['VALOR_PIS'] - tratamento_impostos['VALOR_COFINS']

        # Aplicar a função lambda a cada linha do DataFrame para calcular o valor do FUNTEL com base na condição e no objeto Imposto
        tratamento_impostos['VALOR_FUNTEL'] = tratamento_impostos.apply(lambda row: funtel.aplicar_imposto(row['VALOR_FUNTEL']) 
                                                                    if row['FUNTEL'] else row['FUNTEL'], axis=1)
        
        # Construindo as regras para a aplicação da alíquota de CSLL
        tratamento_impostos['CSLL'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'VENDAS DE INTERNET')

        # Criar instâncias da classe Imposto para cada tipo de imposto com suas condições
        csll = Imposto(aliquota=(32*9)/100)

        # Aplicar a função lambda a cada linha do DataFrame para calcular o valor do CSLL com base na condição e no objeto Imposto
        tratamento_impostos['VALOR_CSLL'] = tratamento_impostos.apply(lambda row: csll.aplicar_imposto(row['VALOR']) if row['CSLL'] 
                                                                else row['CSLL'], axis=1)
        
        # Construindo as regras para a aplicação da alíquota de IR
        tratamento_impostos['IR'] = tratamento_impostos['CONTA'].apply(lambda x: x == 'VENDAS DE INTERNET')

        adicional_ir_total = tratamento_impostos.loc[tratamento_impostos['IR'] == True, 'VALOR'].sum()
        adicional_ir = (adicional_ir_total*(32/100))-20000
        adicional_ir = ((adicional_ir *10)/adicional_ir_total)/100
        adicional_ir

        tratamento_impostos['ADICIONAL_IR'] = tratamento_impostos.apply(lambda row: row['VALOR'] * adicional_ir if row['IR'] else row['IR'], axis=1)

        # Criar instâncias da classe Imposto para cada tipo de imposto com suas condições
        ir = Imposto(aliquota=(32*15)/100)

        # Aplicar a função lambda a cada linha do DataFrame para calcular o valor do IR com base na condição e no objeto Imposto
        tratamento_impostos['VALOR_IR'] = tratamento_impostos.apply(lambda row: ir.aplicar_imposto(row['VALOR']) if row['IR'] 
                                                                else row['IR'], axis=1)
        
        tratamento_impostos['VALOR_IR'] = tratamento_impostos['VALOR_IR'] + tratamento_impostos['ADICIONAL_IR']

        df_empilhado = pd.melt(tratamento_impostos, id_vars=['FILIAL','CIDADE', 'IDCLVL', 'DATA', 'CONTA', 'ICMS', 'PIS', 'COFINS', 'FUST', 
                                                            'FUNTEL', 'CSLL', 'IR'], value_vars=['VALOR_ICMS', 'VALOR_PIS', 
                                                                                                'VALOR_COFINS', 'VALOR_FUST',
                                                                                                'VALOR_FUNTEL', 'VALOR_CSLL',
                                                                                                'VALOR_IR'], 
                        var_name='NCONTA', value_name='IMPOSTO')
        
        # Excluir linhas com base nos valores de duas colunas
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_ICMS') & (df_empilhado['ICMS'] == False))]

        # Excluir linhas com base nos valores de duas colunas
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_PIS') & (df_empilhado['PIS'] == False))]

        # Excluir linhas com base nos valores de duas colunas
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_COFINS') & (df_empilhado['COFINS'] == False))]

        # Excluir linhas com base nos valores de duas colunas
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_FUST') & (df_empilhado['FUST'] == False))]

        # Excluir linhas com base nos valores de duas colunas
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_FUNTEL') & (df_empilhado['FUNTEL'] == False))]

        # Excluir linhas com base nos valores de duas colunas
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_CSLL') & (df_empilhado['CSLL'] == False))]

        # Excluir linhas com base nos valores de duas colunas
        df_empilhado = df_empilhado.loc[~((df_empilhado['NCONTA'] == 'VALOR_IR') & (df_empilhado['IR'] == 0))]

        # Criando o DataFrame de saída aproveitando algumas colunas do dados de tratamento
        saida_faturamento = DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                            'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])

        # Colocandos os dados tratados na coluna VALOR
        saida_faturamento['VALOR_REF'] = tratamento['VALOR']

        # Colocandos os dados tratados na coluna EMPRESA
        saida_faturamento['EMPRESA'] = tratamento['FILIAL']

        # Colocandos os dados tratados na coluna HISTORICO
        saida_faturamento['HISTORICO'] = tratamento['DESCRICAO']

        # Colocando valor padrão na coluna FONTE
        saida_faturamento['FONTE'] = 'FATURAMENTO E IMPOSTOS'

        # Colocando valor padrão na coluna DIRETO_CSC
        saida_faturamento['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'

        # Colocando valor padrão na coluna TIPO_RATEIO
        saida_faturamento['TIPO_RATEIO'] = 'OK'

        # Colocando valor padrão na coluna MULTIPLICADOR
        saida_faturamento['MULTIPLICADOR'] = 1

        # Transformando o valor para preencer a coluna VALOR_REALIZADO
        saida_faturamento['VALOR_REALIZADO'] = saida_faturamento['VALOR_REF']*saida_faturamento['MULTIPLICADOR']

        # Criando o DataFrame de saída aproveitando algumas colunas do dados de tratamento
        saida_impostos = DataFrame(df_empilhado, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                            'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])

        # Colocandos os dados tratados na coluna VALOR
        saida_impostos['VALOR_REF'] = df_empilhado['IMPOSTO']

        # Colocandos os dados tratados na coluna EMPRESA
        saida_impostos['EMPRESA'] = df_empilhado['FILIAL']

        # Colocandos os dados tratados na coluna CONTA
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_ICMS', 'ICMS', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_COFINS', 'COFINS', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_CSLL', 'CSLL', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_FUNTEL', 'FUNTEL', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_FUST', 'FUST', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_IR', 'IRPJ', saida_impostos['CONTA'])
        saida_impostos['CONTA'] = np.where(df_empilhado['NCONTA'] == 'VALOR_PIS', 'PIS', saida_impostos['CONTA'])

        # Colocandos os dados tratados na coluna HISTORICO
        string = ' SOBRE '
        saida_impostos['HISTORICO'] = saida_impostos['CONTA'] +  string  + df_empilhado['CONTA']

        # Colocando valor padrão na coluna FONTE
        saida_impostos['FONTE'] = 'FATURAMENTO E IMPOSTOS'

        # Colocando valor padrão na coluna DIRETO_CSC
        saida_impostos['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'

        # Colocando valor padrão na coluna TIPO_RATEIO
        saida_impostos['TIPO_RATEIO'] = 'OK'

        # Colocando valor padrão na coluna MULTIPLICADOR
        saida_impostos['MULTIPLICADOR'] = -1

        # Transformando o valor para preencer a coluna VALOR_REALIZADO
        saida_impostos['VALOR_REALIZADO'] = saida_impostos['VALOR_REF']*saida_impostos['MULTIPLICADOR']

        saida_faturamento_impostos = pd.concat([saida_faturamento, saida_impostos], axis = 0)

        # Salve o DataFrame em um arquivo Excel em memória
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            saida_faturamento_impostos.to_excel(writer, index=False)

        return output
    

with st.container(): #Funções e botões para o processamento do arquivo

    coluna1, coluna2 = st.columns(2)    
    with coluna1:     
        # Criar um campo de upload
        uploaded_file = st.file_uploader("Selecione um arquivo Excel", type="xlsx")
        
    # Nome padrão do arquivo
    nome_padraof = 'faturamento_impostos_DRE_' + mes + ano + '.xlsx'

    with coluna2:
        st.write('Quando o processo estiver finalizado, aparecerá um botão para clicar e baixar o arquivo')
        if uploaded_file:
            
            # Processar o arquivo e obter o arquivo Excel processado em memória
            processed_file = process_excel_faturamento(uploaded_file)
                
            # Exibir uma mensagem de sucesso
            st.success('Processamento concluído com sucesso!')

             # Botão para baixar o arquivo processado
            st.download_button(
                    label="Baixar Arquivo Processado",
                    data=processed_file.getvalue(),
                    file_name= nome_padraof,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
