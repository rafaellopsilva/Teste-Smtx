# Teste-Smtx
Teste de proficiência


# Introdução
O projeto consiste de 5 desafios práticos relacionados a análise de dados e algumas questões teóricas.

# Questões teóricas

# Qual o objetivo do comando cache em Spark? 
O comando ‘cache’ no Spark é uma operação realizada para armazenar na memória um RDD, essa operação só é concretizada sob demanda, o que significa que apenas numa segunda operação a informação cacheada será utilizada.

# O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê? 
Porque o Spark utiliza o Resilient Distributed Dataset para realizar operações na memória e no cache dos nós de um cluster, enquanto o MapReduce utiliza o disco rígido para realizar suas operações, por conta disso, o Spark ganha uma performance maior em relação ao concorrente.

# Qual é a função do SparkContext? 
O SparkContext é o responsável por fazer a conexão entre o código que está sendo desenvolvido e o cluster do Spark, gerando um “contexto” no qual o código irá atuar. Geralmente ele é definido através de uma variável que será utilizada para utilizar as funções do Spark.

# Explique com suas palavras o que é Resilient Distributed Datasets (RDD). 
O RDD é uma coleção de objetos e elementos que são distribuídas através de um cluster do Spark, tendo a possibilidade de ser trabalhado em paralelo. Além disso, ele é tolerante a falhas e pode ser operado tanto em sistemas de arquivos tradicionais, como também em banco de dados NoSQL. Outro ponto importante é que ele sempre é executado no nó principal do cluster, que é encarregado de distribuir as operações entre seus demais nós.

# GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
O GroupByKey se torna menos eficiente que o ReduceByKey porque enquanto o ReduceByKey primeiro combina a saída por chave antes de misturar com as demais partições, o GroupByKey apenas mistura e agrupa por chave após os dados serem misturados com os demais nós, com isso, muita informação desnecessária é passada para frente e acaba causando uma utilização de memória muito grande.

# Explique o que o código Scala abaixo faz.
1.	O código começa abrindo um arquivo que está armazenado no HDFS.
2.	Adiciona a variável ‘counts’ a quantidade de vezes que cada palavra aparece no arquivo, agrupando-as por chave, que no caso será cada palavra, e o valor, que é a quantidade de vezes que a palavra se repete.
3.	Salva o arquivo novamente no HDFS.

# Desafios

Para cada desafio, um arquivo diferente está adicionado ao projeto, que conterá uma saída com o comando "print" para cada desafio.