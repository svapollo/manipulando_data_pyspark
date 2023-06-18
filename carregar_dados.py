from pyspark.sql import SparkSession
from pathlib import os
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder\
    .appName('Carregar_dados_disney') \
    .config("spark.jars", r"driver\postgresql-42.5.1.jar") \
    .config("spark.executor.extraClassPath", r"driver\postgresql-42.5.1.jar") \
    .getOrCreate()

# .config("spark.driver.extraClassPath", r"driver\postgresql-42.5.1.jar") \

#################################################
# lista de arquivos origem

lista_df = [
    "disney_movies"
]

lista_caminho_dados = [
    "./dados_origem/disney_movies_qtd.csv"
]

lista_formato_origem = [
    "csv"
]


# ler arquivos da lista_caminho_dados

for i in range(len(lista_caminho_dados)):
    lista_df[i] = spark.read\
                .format(lista_formato_origem[i])\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .option("multiline", "true")\
                .load(lista_caminho_dados[i])


#################################################


#################################################
# listas para gravar no BD

schema = [
    "disney"
]

lista_tables = [
    "disney_movies"
]

# Gravar no BD
for ap in range(len(lista_df)):
    lista_df[ap].write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/manipulando_datas") \
        .option("dbtable", f"{schema[ap]}.{lista_tables[ap]}") \
        .option("user", str(os.getenv('POSTGRES_USER'))) \
        .option("password", str(os.getenv('POSTGRES_PASSWORD'))) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
