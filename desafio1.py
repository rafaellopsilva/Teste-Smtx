from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Desafio1")
sc = SparkContext(conf = conf)

lines = sc.textFile("file://access_log_Jul95") + sc.textFile("file://access_log_Aug95")
# Pega todas as linhas e mapeia para o resultado o primeiro indíce da String 
# separado pelo caractere de espaço, o que corresponde ao Hostname ou endereço IP.
# Em seguida agrupa o resultado pelos seus valores.
result = lines.map(lambda x: x.split()[0]).countByValue()

# Imprime o resultado, exibindo o tamanho de itens que possui após agrupar os valores.
print(len(result))