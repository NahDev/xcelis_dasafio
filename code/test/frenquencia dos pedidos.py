import csv


def processa_csv(arquivo):
    # Abrir o arquivo CSV
    with open(arquivo, "r") as csvfile:
        reader = csv.DictReader(csvfile)

        # Criar listas vazias para contar a frequência dos valores
        cod_org_freq = {}
        cod_dest_freq = {}
        total_freq = {}

        # Loop pelas linhas do arquivo
        for row in reader:
            # Obter o valor da coluna "COD org"
            cod_org = row["COD org"]

            # Adicionar 1 à contagem do valor na lista cod_org_freq
            if cod_org in cod_org_freq:
                cod_org_freq[cod_org] += 1
            else:
                cod_org_freq[cod_org] = 1

            # Obter o valor da coluna "COD dest"
            cod_dest = row["COD dest"]

            # Adicionar 1 à contagem do valor na lista cod_dest_freq
            if cod_dest in cod_dest_freq:
                cod_dest_freq[cod_dest] += 1
            else:
                cod_dest_freq[cod_dest] = 1

            # Adicionar 1 à contagem total do valor
            if cod_org in total_freq:
                total_freq[cod_org] += 1
            else:
                total_freq[cod_org] = 1
            if cod_dest in total_freq:
                total_freq[cod_dest] += 1
            else:
                total_freq[cod_dest] = 1

            # Verificar se o valor está na lista e remover a linha correspondente
            # if cod_org in valores_remover:
            #     reader.remove(row)
            # if cod_dest in valores_remover:
            #     reader.remove(row)

    # Imprimir as contagens
    print("Frequência dos valores na coluna 'COD org':")
    for cod_org, freq in cod_org_freq.items():
        print(f"{cod_org}: {freq}")
    print("\nFrequência dos valores na coluna 'COD dest':")
    for cod_dest, freq in cod_dest_freq.items():
        print(f"{cod_dest}: {freq}")
    print("\nFrequência total dos valores:")
    for value, freq in total_freq.items():
        print(f"{value}: {freq}")


processa_csv(
    "/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/pedidos/pedidos000.csv"
)
