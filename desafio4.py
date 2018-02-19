from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Desafio4")
sc = SparkContext(conf = conf)

# Função que retorna array com a data e o status da requisição.
def addToDataResponses(line):
    # Ao ser identificado no dataset que possui um registro que apenas contém o nome do host, 
    # é executado essa condição para que seja evitado algum erro no mapeamento.
    if len(line.split()) < 2:
        return ["0","0"]
    # As próximas duas condições retornam a data e o status baseado em como o nome do Host está definido.
    elif line.split("\" ")[1].split()[0].isdigit():
        return [line.split("[")[1].split(":")[0], line.split("\" ")[1].split()[0]]
    else:
        return [line.split("[")[1].split(":")[0], line.split("\" ")[2].split()[0]]

lines = sc.textFile("file://access_log_Jul95") + sc.textFile("file://access_log_Aug95")
# Mapeia a data e status das requisições e em seguida filtra para retornar apenas as con status '404'.
dateResponse = lines.map(lambda x: addToDataResponses(x)).filter(lambda x: x[1] == "404" )
# Agrupa por valor apenas as datas, , contando a quantidade de aparições de cada.
result = dateResponse.map(lambda x: x[0]).countByValue()
# Ordena as datas pelo dia.
sortedResults = collections.OrderedDict(sorted(result.items()))

# Imprime o resultado.
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
