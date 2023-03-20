import os
from time import time

# definir o caminho para a pasta com os arquivos CSV
pasta_csv = "dados/pedidos"

# definir o separador padrão que será usado
separador_padrao = ","
separador_exist = ";"
ini_time = time()
i = len(os.listdir(pasta_csv))
print(i)
# iterar sobre cada arquivo na pasta CSV
for arquivo_csv in os.listdir(pasta_csv):
    print(f"Faltam {i} arquivos para padronizar")
    i -= 1
    # verificar se o arquivo é realmente um arquivo CSV
    if arquivo_csv.endswith(".csv"):
        print(f"Padronizando o Arquivo {arquivo_csv}")
        # abrir o arquivo CSV e ler o conteúdo
        with open(os.path.join(pasta_csv, arquivo_csv), "r") as f:
            conteudo = f.read()

        # substituir todos os separadores existentes pelo separador padrão
        conteudo = conteudo.replace(separador_exist, separador_padrao)
        conteudo = conteudo.replace("\t", separador_padrao)
        # adicione outras linhas aqui para substituir outros separadores que você queira padronizar

        # escrever o novo conteúdo com o separador padronizado de volta para o arquivo CSV
        with open(os.path.join(pasta_csv, arquivo_csv), "w") as f:
            f.write(conteudo)
end_time = time()


print(f"tempo de execução {round(end_time-ini_time,2)} segundos")
