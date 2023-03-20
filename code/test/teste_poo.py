import pandas as pd


def calcular_distancia(arquivo_pedido, arquivo_distancia, arquivo_saida):
    df = pd.read_csv(arquivo_pedido)
    distancia_total = []
    for i, row in df.iterrows():
        num1 = row["COD org"]
        num2 = row["COD dest"]
        df_rotas = pd.read_csv(arquivo_distancia, delimiter=",")
        df_rotas["diff1"] = abs(df_rotas["org"] - num1)
        df_rotas["diff2"] = abs(df_rotas["dest"] - num2)
        df1 = df_rotas.loc[df_rotas["diff1"] == df_rotas["diff1"].min()]
        df2 = df1.loc[df1["diff2"] == df1["diff2"].min()]
        distancia_total.append(df2["km_rota"].iloc[0])
    df["Distancia total"] = distancia_total
    df.to_csv(arquivo_saida, index=False)
    return df


arquivo_entrada = (
    "/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/resultado1.csv"
)
arquivo_distancia = r"/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/distancias1.csv"
arquivo_saida = "resultado2.csv"
df = calcular_distancia(arquivo_entrada, arquivo_distancia, arquivo_saida)
print(df)
