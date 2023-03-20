import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.distance import great_circle

# Criar um objeto geolocalizador
geolocalizador = Nominatim(user_agent="my_app")

# Ler o arquivo CSV
pedidos = pd.read_csv(
    "/home/nahuan/Python/Jupiter/Excellis/PS_PYTHON-DEV_2301-main/resultado1.csv",
    delimiter=",",
)

# Criar um dicionário para armazenar as coordenadas de cada cidade
coordenadas = {}


# Definir uma função para obter as coordenadas de uma cidade
def obter_coordenadas(cidade):
    if cidade in coordenadas:
        return coordenadas[cidade]

    location = geolocalizador.geocode(cidade)

    if location is not None:
        coordenadas[cidade] = (location.latitude, location.longitude)
        return coordenadas[cidade]
    else:
        return None


# Definir uma função para calcular a distância total
def calcular_distancia(origem, destino):
    if origem is not None and destino is not None:
        distancia = great_circle(origem, destino).km
        return distancia
    else:
        return None


# Usar a função vectorize para aplicar a função calcular_distancia em cada linha do dataframe
pedidos["Distancia Ida e Volta"] = np.vectorize(calcular_distancia)(
    pedidos.apply(
        lambda row: (
            obter_coordenadas(row["Munic?pio Org"]),
            obter_coordenadas(row["Munic?pio Dest"]),
        ),
        axis=1,
    )
)

# Imprimir o dataframe com a nova coluna "Distancia Ida e Volta"
print(pedidos)
# pedidos.to_csv('pedidos000.csv', index=False)
