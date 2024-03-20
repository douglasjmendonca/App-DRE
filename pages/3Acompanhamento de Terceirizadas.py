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

st.write('## Processo de Acompanhamento de Terceirizadas')

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

with st.container(): # Processo de Acompanhamento de Terceirizadas
# Processo de Acompanhamento de Terceirizadas
    def process_excel_terceirizadas(uploaded_file):
        
        # Carregue o arquivo Excel para um DataFrame
        entrada = pd.read_excel(uploaded_file)
        
        entrada['NATUREZA'] = entrada['Tipo despesa'].str[7:]
        
        # Criando o DataFrame de tratamento aproveitando algumas colunas dos dados de entrada
        tratamento = pd.DataFrame(entrada, columns = ['EMPRESA','CIDADE','Data','Valor'])
        
        # Trocando o nome das colunas Data e Valor
        tratamento = tratamento.rename (columns ={'Data': 'DATA', 'Valor': 'VALOR'})
        
        # Alterando o texto da coluna CIDADES para letra maiúscula, porque é nesse padrão que está no DataMaster
        tratamento ['CIDADE'] = tratamento ['CIDADE'].str.upper()
        
        tratamento['HISTORICO'] = 'PROVISIONAMENTO'+" "+ entrada['EMPRESA']+" "+ tratamento['CIDADE']+" "+entrada['Classificação Conta']+" "+ entrada['Tipo despesa']+" "+ entrada['Histórico']
        
        # Separando o código da descrição do centro de  custos e criandos duas novas colunas no banco de tratamento
        tratamento[['IDCC','CENTRO_CUSTOS']] = entrada['CENTRO DE CUSTO'].str.split('-', expand=True)
        
        # Construção do código IDCC seguindo o padrão do BD e utilizando os valores separados da coluna IDCC da tabela de tratamento
        tratamento["IDCC"] = "102"+"00"+tratamento["IDCC"].astype(str).str[:7]
        
        # Criando a coluna IDCONTA
        tratamento = pd.DataFrame(tratamento,columns = ['EMPRESA','CIDADE','DATA','VALOR','HISTORICO', 'IDCC','CENTRO_CUSTOS','IDCONTA'])
        
        # Preenchimento da coluna conta com as contas transformadas em EMPREEITEIRA SG&A e CALL CENTER
        tratamento.loc[tratamento['EMPRESA'] == 'CALL CENTER', 'CONTA'] = 'CALL CENTER'
        tratamento.loc[tratamento['EMPRESA'] != 'CALL CENTER', 'CONTA'] = 'EMPREITEIRAS SG&A'
        
        # Criando um banco de dados auxiliar para unir os valores criados do NCIDADE com o banco de dados de tratamento
        auxcon = plano_contas[plano_contas['base'] == 'ProtheusSA'].copy()
        auxcon = pd.DataFrame(auxcon,columns=["idconta","conta_contabil"])
        auxcon = pd.merge(tratamento,auxcon,left_on="CONTA",right_on="conta_contabil")
        auxcon = pd.DataFrame(auxcon,columns=["idconta","conta_contabil"])
        
        # Construindo um dicionário com os valores de idclvl e classe de valor para utilizar no preenchimento da coluna IDCLVL
        dic_con = auxcon.set_index('conta_contabil')['idconta'].to_dict()
        
        # Preenchimento da coluna de IDCLVL com os valores do dicionário criado para cada valor correspondente na coluna NCIDADE
        tratamento['IDCONTA'] = tratamento['CONTA'].map(dic_con)
        
        # Criando a coluna NATUREZA
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223024-PROCESSO TRABALHISTA', 'PROCESSO TRABALHISTA', entrada['Tipo despesa'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221016-RESCISAO', 'RESCISAO', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221014-ASSISTENCIA MEDICA/ODONTO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221013-COMISSÃO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221006-FERIAS', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221010-FGTS RECISORIO/GRRF', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221004-FGTS/GFIP', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221003-INSS/GPS PESSOAL', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221020-IRRF PESSOAL', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227013-IRRF SERVICO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221012-RETIRADA PRO-LABORE', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221002-SALARIO LÍQUIDO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '224006-SEGURANCA MEDICINA TRABALHO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == 'SEGURO PRESTAMISTA', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221009-VALE ALIMENTAÇÃO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221011-VALE TRANSPORTE', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221017-PENSAO ALIMENTICIA', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221015-SEGURO PESSOAL', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221007-BOLSA ESTÁGIO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '221001-ADIANTAMENTO SALARIO', 'FOLHA', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223001-ADIANTAMENTO VIAGEM', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223002-AGUA E ESGOTO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223003-ALUGUEL IMOVEL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227002-COFINS', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223006-CONDOMINIO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223007-CONSERVAÇÃO/LIMPEZA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225009-SISTEMA/SOFTWARE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223008-CURSO/TREINAMENTO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223009-ALIMENTAÇÃO/CAFE/LANCHE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225006-LOCAÇÃO MAQUINA/EQUIPAMENTO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223011-CORRESPONDENCIA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225008-MANUTENÇÃO FROTA/REPARO/MAQUINA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225007-LOCAÇÃO FROTA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '229001-IPTU', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '230001-JUROS/MULTA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '230004-IOF', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223015-ENERGIA ELETRICA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '222006-EPI', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223017-FRETE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225001-CONSULTORIA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227004-CSLL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == 'EMPRESTIMOS', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223015-ENERGIA ELETRICA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225005-HONORARIO ADVOCATICIO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '225004-HONORARIO CONTABIL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227005-IRPJ', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227014-ISSQN', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '224003-MATERIAL CONSTRUCAO/REFORMA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '224004-MATERIAL ESCRITORIO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223020-MENSALIDADE ASSOCIACAO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227016-PARCELAMENTO IMPOSTO', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227001-PIS', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227014-ISSQN', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223023-PROCESSO CIVIL/CLIENTE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '227018-SIMPLES NACIONAL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '230002-TARIFA MANUTENCAO CONTA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '228006-TAXA EXPEDIENTE', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '228001-TAXA CONSELHO PROFISSIONAL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223027-TELEFONIA FIXA', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == '223026-TELEFONIA MOVEL', 'DESPESAS', tratamento['NATUREZA'])
        tratamento['NATUREZA'] = np.where(entrada['Tipo despesa'] == 'MULTA MUNICIPAL', 'DESPESAS', tratamento['NATUREZA'])
        
        # Utilizar esse ultimo código apenas se for agrupar o CALL CENTER por despesas, caso queira que apareça as descrições da natureza
        # conforme aparece na EMPREITEIRA SG&A, caso contrário, não utilizar essa linha de código.
        tratamento['NATUREZA'] = np.where(tratamento['EMPRESA'] == 'CALL CENTER', 'DESPESAS', tratamento['NATUREZA'])
        
        #Criando a coluna NCONTA
        tratamento['NCONTA'] = tratamento['CONTA']+" "+tratamento['NATUREZA']
        
        # Criando a coluna IDCLVL
        tratamento['IDCLVL'] = pd.Series(dtype='float')
        
        # Definição da função condicional para preencher os valores na coluna NCIDADE
        tratamento['NCIDADE'] = tratamento.apply(lambda row: row['CIDADE'] if row['EMPRESA'] == 'CALL CENTER' else row['CIDADE'], axis=1 )
        
        # Substituindo o nome de cidade por CSC com base no critério de ser uma operação de Call Center
        tratamento['NCIDADE'] = np.where(tratamento['EMPRESA'] == 'CALL CENTER', 'CSC', tratamento['NCIDADE'])
        
        # Substituindo o nome de Divinópolis por Divinópolis Regional 
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'DIVINÓPOLIS', 'DIVINOPOLIS REGIONAL', tratamento['NCIDADE'])
        
        # Substituindo os caracteres especiais nos nomes das cidades
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'POÇOS DE CALDAS', 'POCOS DE CALDAS', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'SÃO SEBASTIÃO DO PARAÍSO', 'SAO SEBASTIAO DO PARAISO', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'TRÊS CORAÇÕES', 'TRES CORACOES', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'IGARAPÉ', 'IGARAPE', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'ITAÚNA', 'ITAUNA', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'PARÁ DE MINAS', 'PARA DE MINAS', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'TAUBATÉ', 'TAUBATE', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'CAMPOS DO JORDÃO', 'CAMPOS DO JORDAO', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'UNAÍ', 'UNAI', tratamento['NCIDADE'])
        tratamento['NCIDADE'] = np.where(tratamento['NCIDADE'] == 'ITAJUBÁ', 'ITAJUBA', tratamento['NCIDADE'])
        
        # Criando um banco de dados auxiliar para unir os valores criados do NCIDADE com o banco de dados de tratamento
        auxcid = cidades[cidades['base'] == 'ProtheusSA'].copy()
        auxcid = DataFrame(auxcid,columns=["idclvl","classe_valor"])
        auxcid = pd.merge(tratamento,auxcid,left_on="NCIDADE",right_on="classe_valor")
        auxcid = DataFrame(auxcid,columns=["idclvl","classe_valor"])
        
        # Construindo um dicionário com os valores de idclvl e classe de valor para utilizar no preenchimento da coluna IDCLVL
        dic_cid = auxcid.set_index('classe_valor')['idclvl'].to_dict()
        
        # Preenchimento da coluna de IDCLVL com os valores do dicionário criado para cada valor correspondente na coluna NCIDADE
        tratamento['IDCLVL'] = tratamento['NCIDADE'].map(dic_cid)
        
        # Criando o DataFrame de saída aproveitando algumas colunas do dados de tratamento
        saida = pd.DataFrame(tratamento, columns = ['IDCC','IDCLVL','IDCONTA', 'EMPRESA', 'COD_FILIAL', 'NOME_FILIAL', 'COD_PRODUTO',
                                            'DESC_PRODUTO', 'QUANTIDADE', 'DATA', 'VALOR_REF', 'DOCUMENTO', 'HISTORICO', 
                                            'COD_FORNECEDOR', 'CENTRO_CUSTOS', 'CIDADE', 'CONTA', 'DETALHAMENTO', 'FONTE', 
                                            'OBS', 'DIRETO_CSC', 'TIPO_RATEIO', 'MULTIPLICADOR', 'VALOR_REALIZADO'])
        
        # Colocandos os dados tratados na coluna DATA
        saida['DATA'] = tratamento['DATA']
        
        # Colocandos os dados tratados na coluna VALOR
        saida['VALOR_REF'] = tratamento['VALOR']
        
        # Colocandos os dados tratados na coluna HISTORICO
        saida['HISTORICO'] = tratamento['HISTORICO']
        
        # Colocandos os dados tratados na coluna CENTRO_CUSTOS
        saida['CENTRO_CUSTOS'] = tratamento['CENTRO_CUSTOS']
        
        # Colocandos os dados tratados na coluna CIDADE
        saida['CIDADE'] = tratamento['NCIDADE']
        
        # Colocandos os dados tratados na coluna CONTA
        saida['CONTA'] = tratamento['NCONTA']
        
        # Colocandos os dados tratados na coluna DETALHAMENTO
        saida['DETALHAMENTO'] = tratamento['EMPRESA']
        
        # Colocando valor padrão na coluna FONTE
        saida['FONTE'] = 'ACOMPANHAMENTO TERCEIRIZADAS'
        
        # Colocandos os dados tratados na coluna OBS
        saida['OBS'] = entrada['NATUREZA']
        
        # Colocando valor padrão na coluna DIRETO_CSC
        saida['DIRETO_CSC'] = 'OPERAÇÃO / REGIONAL'
        
        # Colocandos os dados tratados na coluna TIPO_RATEIO
        saida['TIPO_RATEIO'] = np.where(saida['CIDADE'] == 'CSC', 'TOTAL SEM MOC', saida['CIDADE'])
        saida['TIPO_RATEIO'] = np.where(saida['CIDADE'] != 'CSC', 'OK', saida['CIDADE'])
        saida['TIPO_RATEIO'] = np.where(saida['TIPO_RATEIO'] == 'CSC', 'TOTAL SEM MOC', saida['TIPO_RATEIO'])
        
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
        uploaded_file = st.file_uploader("Selcione um arquivo Excel", type="xlsx")
                    
    # Nome padrão do arquivo
    nome_padraof = 'acompanhamento_terceirizadas_DRE_' + mes + ano + '.xlsx'

    with coluna2:
        st.write('Quando o processo estiver finalizado, aparecerá um botão para clicar e baixar o arquivo')
        if uploaded_file:
            
            # Processar o arquivo e obter o arquivo Excel processado em memória
            processed_file = process_excel_terceirizadas(uploaded_file)
                
            # Exibir uma mensagem de sucesso
            st.success('Processamento concluído com sucesso!')

             # Botão para baixar o arquivo processado
            st.download_button(
                    label="Baixar Arquivo Processado",
                    data=processed_file.getvalue(),
                    file_name= nome_padraof,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )