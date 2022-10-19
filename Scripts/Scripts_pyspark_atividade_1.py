from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.functions import *
from pyspark.sql.functions import expr
from pyspark.sql.types import *


# 1. importar dos dados das tabelas para o pyspark (arquivos parquet)

clientes = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/Clientes.parquet")
clientes.show(5)

vendas = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/Vendas.parquet")
vendas.show(5)

itensvendas = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/ItensVendas.parquet")
itensvendas.show(5)

produtos = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/Produtos.parquet")
produtos.show(5)

vendedores = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/Vendedores.parquet")
vendedores.show(5)

# 2. Criar consulta que mostre Nome, Estado e Status de cada cliente

cs1 = clientes.select("Cliente","Estado","Status")
cs1.show()
cs1.write.format("parquet").save("/home/brunoaugp/cs1")

# 3. Criar consulta que mostre apenas os clientes do status "Platinum" e "Gold"

cs2 = clientes.select("*").where((Func.col("Status")=="Platinum")|(Func.col("Status")=="Gold"))
cs2.show()

cs2.write.format("parquet").save("/home/brunoaugp/cs2")

# 4. Demonstrar o total de vendas de cada status

cs3 = vendas.join(clientes,vendas.ClienteID == clientes.ClienteID,"inner") \
.groupBy(clientes.Status).agg(sum("Total")) \
.orderBy(Func.col("sum(Total)").desc())
cs3.show()

cs3.write.format("parquet").save("/home/brunoaugp/cs3")










