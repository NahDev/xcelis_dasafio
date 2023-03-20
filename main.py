import dask.dataframe as dd
import dask.array as da
import dask.distributed as distributed
import logging
import pandas as pd   
import numpy as np
from math import isclose
import time
import logging

import os
import datetime

now = datetime.datetime.now()


logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"log/log_{now}.log"),
                logging.StreamHandler()
            ]
        )

# from scipy.spatial import distance_matrix

class ContadorPedidos:
    def __init__(self):
        self.municipios_file ='./dados/municipios.csv'
        self.hubs_file = './dados/hubs.csv'

        self.distancias_file = './dados/distancias.csv'


    def carrega_arquivo(self,arquivo_pedido):
        logging.info(f"Carregando arquivo {arquivo_pedido}...")

        hubs_df = pd.read_csv(self.hubs_file, sep=';', encoding='cp1252')
        municipios_df = pd.read_csv(self.municipios_file, sep=',', encoding='cp1252')

        pedidos_df = pd.read_csv(arquivo_pedido, sep=',', encoding='cp1252')
        
        distancias_df = pd.read_csv(self.distancias_file, sep=',', encoding='cp1252')
        logging.info(f"Arquivo {arquivo_pedido} carregado com sucesso.")
        return hubs_df, municipios_df, pedidos_df, distancias_df
    

    def nomes_hubs(self, arquivo_entrada):
        logging.info('Iniciando a função nomes_hubs')

        df = arquivo_entrada
        df['Nome HUB'] = df['Nome HUB'].apply(lambda x: ' '.join(x.split()[1:-1]))
        logging.info('Coluna Nome HUB processada com sucesso')

        df.to_csv('./dados/hubs_processados.csv', index=False,sep=',')
        logging.info('Arquivo hubs_processados.csv criado com sucesso')

        nomes_hubs = df['Nome HUB'].tolist()
        logging.info('Retornando lista com os nomes dos hubs')
        return nomes_hubs 

    def limpeza_dados_distancias(self,arquivo_distancias):
        try:
            logging.info("Iniciando limpeza de dados do arquivo de distâncias...")
            
            # Leitura do arquivo CSV ou DataFrame do Pandas
            df = arquivo_distancias

            # Selecionar as linhas com a string 'nao existe caminho entre os dois locais' ou 'Nao existe caminho entre os dois locais'
            tratativa_especial = df[df['km_rota'].isin(['nao existe caminho entre os dois locais', 'Nao existe caminho entre os dois locais',''])]

            # Salvar as linhas selecionadas em um novo arquivo CSV chamado 'tratativa_especial.csv'
            tratativa_especial.to_csv('./dados/tratativa_especial.csv', index=False)

            # Remover as linhas selecionadas do arquivo original
            df = df[~df['km_rota'].isin(['nao existe caminho entre os dois locais', 'Nao existe caminho entre os dois locais',''])]

            # Iteração sobre cada linha da coluna 'km_rota' e determinação do tipo de dado
            for i, row in df.iterrows():
                if row['km_rota'] == 'mesma cidade':
                    df.loc[i, 'km_rota'] = 10.0
                if row['km_linear'] == 'mesma cidade':
                    df.loc[i, 'km_linear'] = 10.0

            for i, row in df.iterrows():
                km_rota = str(row['km_rota'])
                if '.' not in km_rota: # se o valor não contém ponto
                    km_rota = km_rota[:-3] + '.' + km_rota[-3:] # adicionar ponto no terceiro dígito contando de trás para frente
                    df.loc[i, 'km_rota'] = km_rota # atualizar o valor no DataFrame

            # Padronizar os números nas colunas 'km_linear' e 'km_rota'
            df['km_rota'] = pd.to_numeric(df['km_rota'], errors='coerce')
            df['km_linear'] = pd.to_numeric(df['km_linear'], errors='coerce')
            df['km_linear'] = np.around(df['km_linear'], decimals=2)
            df['km_rota'] = np.around(df['km_rota'], decimals=2)

            for i, row in df.iterrows():
                if row['km_rota'] > 5000:
                    row['km_rota'] = str(row['km_rota']).replace('.','')
                    row['km_rota'] = row['km_rota'][:-4] + '.' + row['km_rota'][-4:] 
                    row['km_rota'] = float(row['km_rota'])

            for i, row in df.iterrows():
                # Verificar se km_linear é maior que km_rota
                if row['km_linear'] > row['km_rota']:
                    # Trocar os valores de km_rota e km_linear
                    df.at[i, 'km_rota'], df.at[i, 'km_linear'] = row['km_linear'], row['km_rota']

            df.to_csv(f'./dados/distancias_file_normalized.csv', index=False)

            logging.info("Limpeza de dados do arquivo de distâncias concluída com sucesso!")
            
        except Exception as e:
            logging.error(f"Ocorreu um erro ao abrir o arquivo {arquivo_distancias}")


    def procurar_cod_mun(self, arquivo, cidades_procuradas):
        # carrega o arquivo csv em um dataframe usando o pandas
        df = arquivo
        
        # cria um dicionário para armazenar as correspondências de nome da cidade para cod mun
        cod_mun_dict = {}
        
        # itera sobre as linhas do dataframe
        for index, row in df.iterrows():
            # obtém o nome da cidade e o cod mun da linha atual
            cidade_uf = row['Munic?pio - UF']
            cod_mun = row['COD mun']
            
            # adiciona a correspondência para o dicionário
            cod_mun_dict[cidade_uf] = cod_mun
            
        # cria um dicionário para armazenar os códigos mun correspondentes às cidades procuradas
        cod_mun_correspondentes = {}
        
        # itera sobre as cidades procuradas e procura os códigos correspondentes
        for cidade in cidades_procuradas:
            encontrou_cidade = False
            
            for chave in cod_mun_dict.keys():
                if str(chave).startswith(cidade):
                    cod_mun_correspondentes[cidade] = cod_mun_dict[chave]
                    encontrou_cidade = True
                    break
            
            if not encontrou_cidade:
                logging.warning(f'{cidade} não foi encontrado no arquivo {arquivo}csv.')
        # print(cod_mun_correspondentes)

        return cod_mun_correspondentes

        # exit()


    def contar_valores_e_municipios(self, arquivos_pedido,cod_muns_dict, path_mun, path_resultado):
        df = arquivos_pedido
        logging.info('Arquivo de pedidos lido com sucesso')

        municipios_df = path_mun
        logging.info('Arquivo de municípios lido com sucesso')

        counts = {}

        for key, value in cod_muns_dict.items():
            org_count = np.sum(df['COD org'].isin(([value])))
            dest_count = np.sum(df['COD dest'].isin([value]))
            total_count = org_count + dest_count

            municipio = municipios_df.loc[municipios_df['COD mun'].isin([value]), 'Munic?pio - UF'].iloc[0]

            counts[key] = {'Total de Pedidos Alocados': total_count, 'Total de Pedidos Redirecionados Enviados': org_count, 'Total de Pedidos Redirecionados Recebidos': dest_count, 'Município': municipio}

        result_df = pd.DataFrame.from_dict(counts, orient='index')

        result_df.index.name = 'Nome HUB'

        result_df.to_csv(path_resultado, sep=',', encoding='cp1252')

        return org_count, counts


    def total_pedidos_nao_entregues(self):
        logging.info("Iniciando cálculo do total de pedidos não entregues...")
        
        nao_entregues = self.pedidos_df.loc[pd.isna(self.pedidos_df['Data de Entrega'])].shape[0]
        
        logging.info(f"Total de pedidos não entregues: {nao_entregues}")
        
        nao_entregues_df = pd.DataFrame({'Total de Pedidos Não Entregues': [nao_entregues]})
        nao_entregues_df.to_csv('./dados/total_pedidos_nao_entregues.csv', index=False)
        
        logging.info("Arquivo 'total_pedidos_nao_entregues.csv' salvo com sucesso!")
        
        return nao_entregues
 

    def processar_pedidos(self, arquivo_pedidos, arquivo_municipio,pedidos_file_name):

        logging.info(f'Lendo arquivo de pedidos: {arquivo_pedidos}')
        pedidos = arquivo_pedidos
        logging.info(f'Lendo arquivo de municípios: {arquivo_municipio}')
        municipios = arquivo_municipio

        pedidos = pd.merge(pedidos, municipios, left_on='COD org', right_on='COD mun', how='left')
        pedidos = pedidos.rename(columns={'Munic?pio - UF': 'Munic?pio Org'})

        pedidos = pd.merge(pedidos, municipios, left_on='COD dest', right_on='COD mun', how='left')
        pedidos = pedidos.rename(columns={'Munic?pio - UF': 'Munic?pio Dest'})
        pedidos = pedidos.drop(['COD mun_y', 'COD mun_x'], axis=1)
        logging.info(f'Salvando resultado em arquivo CSV: resultado1_{pedidos_file_name}')
        pedidos.to_csv(f'./dados/resultado1_{pedidos_file_name}', index=False,sep=',',encoding='cp1252')

        logging.info('Processamento concluído com sucesso.')
        return pedidos



    def calcular_distancia(self, arquivo_pedido, arquivo_distancia, arquivo_saida):
        logging.info('Iniciando cálculo de distância...')
        df = pd.read_csv(arquivo_pedido, sep=',')
        df_rotas = pd.read_csv(arquivo_distancia, sep=',')
        distancia_total = []
        x = sum(1 for _ in df.iterrows())
        init_time = time.time()
        for i, row in df.iterrows():
            print(f'Faltam {x} Calculos')
            x -= 1
            num1 = row['COD org']
            num2 = row['COD dest']
            print(f'calculando a linha {num1}')
            
            df_rotas['diff1'] = abs(df_rotas['org'] - num1)
            df_rotas['diff2'] = abs(df_rotas['dest'] - num2)
            df1 = df_rotas.loc[df_rotas['diff1'] == df_rotas['diff1'].min()]
            df2 = df1.loc[df1['diff2'] == df1['diff2'].min()]
            distancia_total.append(df2['km_rota'].iloc[0])
        end_time = time.time()
        
        df['Distancia total'] = distancia_total
        df.to_csv(arquivo_saida, index=False)
        logging.info('Cálculo de distância concluído.')
        logging.info(f'Tempo de execução do Calculo {round(end_time-init_time,2)} segundos ')
        return df



    def merge_csv(self, arquivo1, arquivo2, nome_coluna, name_final_file):
        logging.basicConfig(filename='merge_csv.log', level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

        df1 = pd.read_csv(arquivo1, sep=',', encoding='cp1252')
        df2 = pd.read_csv(arquivo2, sep=',', encoding='cp1252')

        df_merged = pd.merge(df1, df2, on=nome_coluna)

        df_merged.to_csv(rf'./dados/{name_final_file}', index=False)
        
        logging.info(f"Arquivo final {name_final_file} salvo com sucesso.")
        
        return df_merged
    
    
    def calcular_resultados(self, arquivo_df1, arquivo_df2):
        df1 =arquivo_df1
        df2 = arquivo_df2

        logging.info(f"Lendo os arquivos CSV {arquivo_df1} e {arquivo_df2}")

        cod_inicial_dict = {row['COD inicial']: (row['Capacidade Entrega'], row['Nome HUB']) for _, row in df2.iterrows()}

        def get_capacidade_entrega(cod_org_x):
            cod_inicial_prox = min(cod_inicial_dict.keys(), key=lambda x: abs(x - cod_org_x))
            return cod_inicial_dict.get(cod_inicial_prox, (None, None))

        df1['Capacidade Entrega'], df1['Nome HUB'] = zip(*df1['COD org_x'].apply(get_capacidade_entrega))
        df1['Resultado'] = round(df1['Capacidade Entrega'] / df1['Distancia total'], 2) * 2

        nome_arquivo_saida = 'dados/PEDIDOS_HUBS_DISTANCIA_DIAS.csv'
        df1.to_csv(nome_arquivo_saida, sep=',', index=False, )
        logging.info(f"Resultado salvo em {nome_arquivo_saida}")


    def excluir_file(self, lista_arquivos):
        df1 = pd.read_csv('/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/total_pedidos_hubs.csv')
        df2 = pd.read_csv('/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/distancias_pedidos_final.csv')
        
        df3 = pd.read_csv('dados/PEDIDOS_HUBS_DISTANCIA_DIAS.csv',sep=',',encoding='cp1252')

        df3 = df3.drop(['COD org_y', 'COD_dest_y'], axis=1)

        df3.to_csv('dados/PEDIDOS_HUBS_DISTANCIA_DIAS.csv', index=False, encoding='cp1252', sep=';')
        writer = pd.ExcelWriter('dados/arquivos_unificados_FINAL.xlsx', engine='xlsxwriter')

        size_limit = 1048576
        n_rows = len(df1)
        for i in range(0, n_rows, size_limit):
            df1.iloc[i:i+size_limit].to_excel(writer, sheet_name=f'aba1_{i//size_limit+1}', index=False)

        n_rows = len(df2)
        for i in range(0, n_rows, size_limit):
            df2.iloc[i:i+size_limit].to_excel(writer, sheet_name=f'aba2_{i//size_limit+1}', index=False)

        writer.save()

        for arquivo in lista_arquivos:
            if os.path.isfile(arquivo):
                try:
                    df = pd.read_csv(arquivo, encoding='cp1252')
                    os.remove(arquivo)
                except Exception as e:
                    logging.exception(f"Erro ao processar o arquivo {arquivo}: {str(e)}")
            else:
                logging.warning(f"Arquivo {arquivo} não encontrado.")


    def padronizar_csv(self):
        pasta_csv = "./dados/pedidos"

        separador_padrao = ","
        separador_exist = ';'
        ini_time = time.time()
        i = len(os.listdir(pasta_csv))
        logging.info(f'Padronizando {i} arquivos')
        for arquivo_csv in os.listdir(pasta_csv):

            print(f'Faltam {i} arquivos para padronizar')
            i -= 1
            if arquivo_csv.endswith(".csv"):
                print(f'Padronizando o Arquivo {arquivo_csv}')
                with open(os.path.join(pasta_csv, arquivo_csv), "r") as f:
                    conteudo = f.read()

                conteudo = conteudo.replace(separador_exist, separador_padrao)
                conteudo = conteudo.replace("\t", separador_padrao)

                with open(os.path.join(pasta_csv, arquivo_csv), "w") as f:
                    f.write(conteudo)
        end_time = time.time()

        print(f'tempo de execução {round(end_time-ini_time,2)} segundos')

if __name__ == '__main__':
    

    

    cp = ContadorPedidos()

    cp.padronizar_csv()
    pedidos_file_name = 'pedidos000.csv'
    pedidos_file = f'./dados/pedidos/{pedidos_file_name}'
    hubs_df, municipios_df, pedidos_df, distancias_df = cp.carrega_arquivo(pedidos_file)
    path_result = rf'/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/PROCESS_{pedidos_file_name}'
    nomes_hubs = cp.nomes_hubs(hubs_df)
    logging.info(f' ####################### INICIANDO O PROCESSO {now} #######################')

    logging.info(f'Processando o arquivo {pedidos_file} {pedidos_file_name}')
    logging.info('')
    tempo_total_inicial = time.time()
    ######################## LIMPEZA DE DADOS ########################

    start_time_limpeza = time.time()
    cp.limpeza_dados_distancias(distancias_df)
    cod_muns = cp.procurar_cod_mun(municipios_df, nomes_hubs)
    end_time_limpeza = time.time()

    logging.info("Tempo de execução para Limpeza de Dados: %s segundos", round(end_time_limpeza - start_time_limpeza,2))
    logging.info('')

    ########################333m PROCESSAR VALORES MUNICIPIOS E PEDIDOS ########################33
    start_time_mun = time.time()
    # print(cod_muns)
    print(pedidos_df)
    # exit()
    total_pedidos, dict_contas = cp.contar_valores_e_municipios(pedidos_df, cod_muns, municipios_df, path_result)
    cp.processar_pedidos(pedidos_df, municipios_df, pedidos_file_name)
    path_1 = f'/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/PROCESS_{pedidos_file_name}'
    path_2 = '/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/hubs_processados.csv'
    end_time_mun = time.time()
    logging.info("Tempo de execução para Processar Valores de Municipios e Pedidos: %s segundos",round(end_time_mun- start_time_mun,2 ))

    cp.merge_csv(path_1, path_2, 'Nome HUB', 'total_pedidos_hubs.csv')
    logging.info('')

    ########################333m DISTANCIA ########################33

    arquivo_pedido = rf'/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/pedidos/{pedidos_file_name}'
    arquivo_distancia = rf'/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/distancias_file_normalized.csv'
    arquivo_said = f'dados/DISTANCIAS_{pedidos_file_name}'

    start_time_distancia = time.time()
    cp.calcular_distancia(arquivo_pedido, arquivo_distancia, arquivo_said)
    end_time_distancia = time.time()

    logging.info("Tempo de execução para processar arquivo de distância: %s segundos",round(end_time_distancia- start_time_distancia,2))

    arquivo1 = rf'/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/DISTANCIAS_{pedidos_file_name}'
    arquivo2 = rf'/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/resultado1_{pedidos_file_name}'
    nome_coluna = 'Id Pedido'
    cp.merge_csv(arquivo1, arquivo2, nome_coluna, 'distancias_pedidos_final.csv')

    df1 = pd.read_csv('/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/distancias_pedidos_final.csv', sep=',')
    df2 = pd.read_csv('/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/total_pedidos_hubs.csv', sep=',')

    logging.info("Cálculo de resultados iniciado")

    start_time_resultado = time.time()
    cp.calcular_resultados(df1, df2)
    end_time_resultado = time.time()

    logging.info("Tempo de execução para limpeza de dados: %s segundos",round(end_time_resultado- start_time_resultado,2) )
    logging.info("Excluindo arquivos desnecessários")
    # exit()
    lista_arquivos = ["dados/distancias_pedidos_final.csv","dados/hubs_processados.csv", f"dados/PROCESS_{pedidos_file_name}", f"dados/resultado1_{pedidos_file_name}", "dados/DISTANCIAS_pedidos000.csv"]
    cp.excluir_file(lista_arquivos)
    tempo_total_final = time.time()
    logging.info(f'Tempo total de execução do Código: {round(tempo_total_final - tempo_total_inicial,2)} segundos')

    logging.info("Processamento finalizado")
