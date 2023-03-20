import pandas as pd


def calcular_distancia(arquivo_pedido, arquivo_distancia, arquivo_saida):
    df = pd.read_csv(arquivo_pedido, sep=",")
    distancia_total = []
    # print(df)
    for i, row in df.iterrows():
        num1 = row["COD org"]
        num2 = row["COD dest"]

        print(num1, num2)
        df_rotas = pd.read_csv(arquivo_distancia, sep=",")
        df_rotas["diff1"] = abs(df_rotas["org"] - num1)
        df_rotas["diff2"] = abs(df_rotas["dest"] - num2)
        df1 = df_rotas.loc[df_rotas["diff1"] == df_rotas["diff1"].min()]
        df2 = df1.loc[df1["diff2"] == df1["diff2"].min()]
        distancia_total.append(df2["km_rota"].iloc[0])
    df["Distancia total"] = distancia_total
    df.to_csv(arquivo_saida, index=False)
    return df


arquivo_pedido = r"/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/pedidos/pedidos000.csv"
arquivo_distancia = r"/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/distancias1_pedidos001.csv"
arquivo_said = r"/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/dados/distancias_pedidos.csv"
df = calcular_distancia(arquivo_pedido, arquivo_distancia, arquivo_said)
print(df)
