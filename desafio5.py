from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Desafio5")
sc = SparkContext(conf = conf)

# Função que retorna o total de bytes da requisição convertendo o valor para o tipo 'int'.
def addToBytesReturned(line):
    # Ao ser identificado no dataset que possui um registro que apenas contém o nome do host, 
    # é executado essa condição para que seja evitado algum erro no mapeamento.
    if len(line.split()) < 2:
        return 0
    # As próximas duas condições retornam os bytes baseado em como o nome do Host está definido.
    elif line.split("\" ")[1].split()[0].isdigit():
        return int(line.split("\" ")[1].split()[1]) if line.split("\" ")[1].split()[1] != "-" else 0
    else:
        return int(line.split("\" ")[2].split()[1]) if line.split("\" ")[2].split()[1] != "-" else 0 

lines = sc.textFile("file://access_log_Jul95") + sc.textFile("file://access_log_Aug95")
# Pega todas as linhas e mapeia para os bytes das requisições.
# Em seguida reduz o resultado somando todos os valores.
result = lines.map(lambda x: addToBytesReturned(x)).reduce(lambda x,y: x+y)

# Imprime o resultado.
print(result)
