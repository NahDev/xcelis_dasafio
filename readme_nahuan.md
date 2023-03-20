# Documentação - Desafio de Análise de Dados

Este projeto é uma solução para um desafio de análise de dados que envolve a manipulação de arquivos CSV. O objetivo do projeto é criar funções que possam extrair informações relevantes de diferentes arquivos CSV e salvar essas informações em novos arquivos.

O projeto utiliza as bibliotecas pandas e numpy para a manipulação dos dados. As funções foram estruturadas no estilo de programação orientada a objeto para tornar o código mais organizado e modular.

## Requisitos
instale os requisitos do codigo com pip install requirements.txt

## Como usar
O código pode ser executado a partir do arquivo "main.py". Para utilizar as funções, basta instanciar a classe "ContadorPedidos" e chamar os métodos correspondentes às informações desejadas.

## Modificações 

Antes de começar a trabalhar no Visual Studio, é importante que você faça a linha de montagem passar pelo arquivo "main.py" e ajuste a variável "pedidos_file_name" com o nome do arquivo que você deseja utilizar. No entanto, é importante prestar atenção em um detalhe: é necessário ajustar o separador do arquivo .csv na variável "pedidos_df" dentro da função 'carrega_arquivo', caso contrário, a máquina ficará tão confusa quanto o Homer Simpson tentando entender física quântica.

Para melhorar o desempenho do código, na primeira vez que rodar, deixe a função "cp = ContadorPedidos()" (que está na linha 388) ser executada. Depois, é possível comentar essa linha, pois os arquivos CSV na pasta "pedidos" estarão padronizados.

Ao final do código, temos a função "excluir_file()", que tem como objetivo eliminar arquivos desnecessários e juntar as informações pertinentes em um arquivo xlsx, com as informações sendo separadas por abas.

É necessário refatorar o arquivo "tratativas especiais", onde a string "não existe caminho entre os dois locais" será utilizada para fazer uma condição e pegar as distâncias. Esse também é um ponto de melhoria.



## Funções Disponíveis
As seguintes funções estão disponíveis:

total_pedidos_alocados(): Retorna o total de pedidos alocados em cada HUB, de acordo com as informações dos arquivos "hubs.csv" e "pedidos000.csv".

total_pedidos_redirecionados_recebidos(): Retorna o total de pedidos redirecionados recebidos em cada município, de acordo com as informações dos arquivos "hubs.csv", "municipios.csv" e "pedidos000.csv".

total_pedidos_redirecionados_enviados(): Retorna o total de pedidos redirecionados enviados em cada município, de acordo com as informações dos arquivos "hubs.csv", "municipios.csv" e "pedidos000.csv".

total_pedidos_nao_entregues(): Retorna o total de pedidos que não foram entregues, de acordo com as informações do arquivo "pedidos.csv".

total_dias_entrega(): Retorna o total de dias para entrega de cada HUB, de acordo com as informações dos arquivos "hubs.csv" e "pedidos000.csv".

## Limitações


Não foi possivel criar uma função para tirar o zip dos arquivos, portanto esse é um ponto de melhoria desse codigo

As funções foram desenvolvidas com base nos arquivos fornecidos para o desafio e podem não funcionar corretamente caso haja alguma alteração na estrutura ou conteúdo desses arquivos. Além disso, as funções foram testadas apenas com os dados fornecidos, e podem não funcionar corretamente com outros conjuntos de dados.
