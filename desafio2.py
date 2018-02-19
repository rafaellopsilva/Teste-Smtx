from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Desafio2")
sc = SparkContext(conf = conf)

# Função que retorna o status da requisição.
def addToResponses(line):
    # Ao ser identificado no dataset que possui um registro que apenas contém o nome do host, 
    # é executado essa condição para que seja evitado algum erro no mapeamento do status da requisição.
    if len(line.split()) < 2:
        return "0"
    # As próximas duas condições retornam o status baseado em como o nome do Host está definido.
    elif line.split("\" ")[1].split()[0].isdigit():
        return line.split("\" ")[1].split()[0]
    else:
        return line.split("\" ")[2].split()[0]

lines = sc.textFile("file:///SparkCourse/access_log_Jul95") + sc.textFile("file:///SparkCourse/access_log_Aug95")
# Mapeia os status das requisições e em seguida filtra para retornar apenas as con status '404'
result = lines.map(lambda x: addToResponses(x)).filter(lambda x: x == '404')

# Imprime o resultado, contando quantas ocorrências existem.
print(result.count())