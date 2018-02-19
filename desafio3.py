from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Desafio3")
sc = SparkContext(conf = conf)

# Função que retorna array com a url e o status da requisição.
def addToUrlResponses(line):
    # Ao ser identificado no dataset que possui um registro que apenas contém o nome do host, 
    # é executado essa condição para que seja evitado algum erro no mapeamento.
    if len(line.split()) < 2:
        return ["0","0"]
    # As próximas duas condições retornam a url e o status baseado em como o nome do Host está definido.
    elif line.split("\" ")[1].split()[0].isdigit():
        return [line.split("\" HTTP")[0].split()[len(line.split("\" HTTP")[0].split())-1], line.split("\" ")[1].split()[0]]
    else:
        return [line.split("\" HTTP")[0].split()[len(line.split("\" HTTP")[0].split())-1], line.split("\" ")[2].split()[0]]
        
lines = sc.textFile("file:///SparkCourse/access_log_Jul95") + sc.textFile("file:///SparkCourse/access_log_Aug95")
# Mapeia a url e status das requisições e em seguida filtra para retornar apenas as con status '404'.
hostResponse = lines.map(lambda x: addToUrlResponses(x)).filter(lambda x: x[1] == "404" )
# Agrupa por valor apenas as url's, contando a quantidade de aparições de cada.
result = hostResponse.map(lambda x: x[0]).countByValue()
# Ordena as url's para ordem decrescente.
resultAgroupped = sorted(result.items(), key=lambda x: x[1], reverse=True)

# Imprime o resultado, mostrando os 5 primeiros.
for i in range(5):
    print(resultAgroupped[i][0])