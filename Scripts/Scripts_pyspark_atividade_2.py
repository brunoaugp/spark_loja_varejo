from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.functions import *
from pyspark.sql.functions import expr
from pyspark.sql.types import *

# 1. Criar um banco de dados no DW do Spark nomeado VendasVarejo e persistir todas tabelas no BD;

spark.sql("create database VendasVarejo")
spark.sql("show databases").show()
spark.sql("use VendasVarejo").show()


clientes = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/Clientes.parquet")
clientes.write.saveAsTable("clientes")
spark.sql("select * from clientes").show(10)

vendas = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/Vendas.parquet")
vendas.write.saveAsTable("vendas")
spark.sql("select * from vendas").show(10)

itensvendas = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/ItensVendas.parquet")
itensvendas.write.saveAsTable("itensvendas")
spark.sql("select * from itensvendas").show(10)

produtos = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/Produtos.parquet")
produtos.write.saveAsTable("produtos")
spark.sql("select * from produtos").show(10)

vendedores = spark.read.format("parquet").load("/home/brunoaugp/download/Atividades/Vendedores.parquet")
vendedores.write.saveAsTable("vendedores")
spark.sql("select * from vendedores").show(10)

spark.sql("show tables").show()

# 2. Criar consulta que mostre cada item vendido: Nome do cliente, Data da Venda, Produto, Vendedor e Valor total do item 

resp2 = itensvendas.join(produtos, itensvendas.ProdutoID == produtos.ProdutoID,"inner")\
    .join(vendas, itensvendas.VendasID == vendas.VendasID,"inner")\
    .join(vendedores, vendas.VendedorID == vendedores.VendedorID,"inner")\
    .join(clientes, vendas.ClienteID == clientes.ClienteID,"inner")\
    .select("Cliente","Data","Produto","Vendedor","VAlorTotal")

resp2.show(20)

resp2.write.format("parquet").save("/home/brunoaugp/resp2")
