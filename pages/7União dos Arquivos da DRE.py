import streamlit as st
import pandas as pd
from io import BytesIO
import io

st.write('## União dos Arquivos dos Processos da DRE')

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

with st.container (): # Função para unir os arquivos

    # Função para concatenar os arquivos Excel
    def concatenar_excel(arquivos_excel):
        dfs = [pd.read_excel(arquivo) for arquivo in arquivos_excel]
        df_concatenado = pd.concat(dfs, ignore_index=True)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_concatenado.to_excel(writer, index=False)

        return output
        
with st.container(): #Funções e botões para o processamento do arquivo
    
    coluna1, coluna2 = st.columns(2)    
    with coluna1:    
        # Criar um campo de upload
        arquivos_excel = st.file_uploader("Selecione os arquivos Excel", type=["xls", "xlsx"], accept_multiple_files=True, key="file_uploader")
            
        # Nome padrão do arquivo
        nome_padrao = 'Arquivo_DRE_' + mes + ano + '.xlsx'

    # Lista para armazenar os arquivos carregados
    arquivos_carregados = []

    if arquivos_excel:
        arquivos_carregados.extend(arquivos_excel)

        # Verifica se há arquivos carregados
        if arquivos_carregados:
            st.write("Arquivos Carregados:")
            for arquivo in arquivos_carregados:
                st.write(arquivo.name)
                
    with coluna2:           
       # Botão para iniciar o processo de concatenação
            st.write("Quando terminar de carregar os arquivos, clique no botão abaixo para iniciar o processamento")
            if st.button("Iniciar Processo"):
                # Concatenar os arquivos
                processed_file = concatenar_excel(arquivos_carregados)
    
                # Botão para baixar o arquivo processado
                st.download_button(
                        label="Baixar Arquivo Processado",
                        data=processed_file.getvalue(),
                        file_name=nome_padrao,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            
