import pandas as pd


class AnaliseDados:
    def __init__(self, hubs_file, municipios_file, pedidos_file):
        self.df_hubs = pd.read_csv(hubs_file, encoding="cp1252", sep=";")
        self.df_mun = pd.read_csv(municipios_file, encoding="cp1252", sep=";")
        self.df_pedidos = pd.read_csv(pedidos_file, encoding="cp1252", sep=",")
        self.cod_org = {}
        self.cod_dest = {}

    def obter_nome_hubs(self):
        list_nome_hubs = []
        for i, row in self.df_hubs.iterrows():
            nome_hub = row["Nome HUB"]
            palavras = nome_hub.split()
            palavras = palavras[1:-1]
            novo_nome_hub = " ".join(palavras)
            list_nome_hubs.append(novo_nome_hub)
        return list_nome_hubs

    def obter_cod_mun(self, list_nome_hubs):
        cod_muns = []
        for name_hub in list_nome_hubs:
            for indice, linha in self.df_mun.iterrows():
                if name_hub in linha["Município - UF"]:
                    cod_muns.append(linha["COD mun"])
        return cod_muns

    def contar_ocorrencias(self, cod_muns):
        for cod_mun in cod_muns:
            self.cod_org[cod_mun] = (self.df_pedidos["COD org"] == cod_mun).sum()
            self.cod_dest[cod_mun] = (self.df_pedidos["COD dest"] == cod_mun).sum()

        # Seleciona apenas as chaves com valores positivos nos dicionários self.cod_org e self.cod_dest
        self.chaves_positivas_org = [
            chave for chave, valor in self.cod_org.items() if valor > 0
        ]
        self.chaves_positivas_dest = [
            chave for chave, valor in self.cod_dest.items() if valor > 0
        ]

        return self.chaves_positivas_org, self.chaves_positivas_dest


if __name__ == "__main__":
    analise = AnaliseDados(
        "./dados/hubs.csv", "./dados/municipios.csv", "./dados/pedidos/pedidos000.csv"
    )
    list_nome_hubs = analise.obter_nome_hubs()
    cod_muns = analise.obter_cod_mun(list_nome_hubs)
    count_org, count_dest = analise.contar_ocorrencias(cod_muns)
    print("O número de ocorrências em cada coluna é:")
    print("Coluna COD org:", len(count_org), count_org)
    print("Coluna COD dest:", len(count_dest), count_dest)
