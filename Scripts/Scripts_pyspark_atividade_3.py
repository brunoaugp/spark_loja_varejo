#Aplicativo que salva tabela em banco de dados PostgreSQL com arquivo nos diferentes formatos (com header), com os inputs:

# -t = formato do arquivo
# -i = diretório que está o arquivo
# -n = nome da tabela a ser salva
# -b = nome do banco de dados

import sys,getopt
from pyspark.sql import SparkSession

if __name__=="__main__":
    #iniciando sessao
    spark = SparkSession.builder.appName("Aplicativo4").getOrCreate()

    #lendo argumentos de entrada
    opts, args = getopt.getopt(sys.argv[1:],"t:i:n:b:")
    formato,infile,name,bd = "","","",""

    for opt, arg in opts:
        if opt == "-t":
            formato = arg
        elif opt == "-i":
            infile = arg
        elif opt == "-n":
            name = arg
        elif opt == "-b":
            bd = arg

#lendo dados com dataframe (se não houver header, trocar "True" para "False")
dados = spark.read.format(formato).load(infile, header = True, inferSchema = True)

#salvando dataframe como tabela no BD bd no PostGres (colocar sua senha do BD)
dados.write.format("jdbc")\
.option("url",f"jdbc:postgresql://localhost:5432/{bd}")\
.option("dbtable",name)\
.option("user","postgres")\
.option("password","******")\
.option("driver","org.postgresql.Driver").save()

#ecerrando Spark
spark.stop()

# EXEMPLO para executar:ir no terminal no local do app e digitar (necessario ter driver JDBC e informar caminho como abaixo):

# spark-submit --jars /home/brunoaugp/Downloads/postgresql-42.5.0.jar Scripts_pyspark_atividade_3.py -t parquet -i /home/brunoaugp/download/Atividades/Produtos.parquet -n produtos -b vendas
