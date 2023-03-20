import pandas as pd
import os

# Define o caminho para a pasta com os arquivos csv
path = "/caminho/para/a/pasta"

# Loop pelos arquivos na pasta
for filename in os.listdir(path):
    if filename.endswith(".csv"):
        # Abre o arquivo csv usando pandas
        df = pd.read_csv(os.path.join(path, filename), sep=",")
        # Escreve o arquivo csv de volta com o separador ','
        df.to_csv(os.path.join(path, filename), index=False, sep=",")
