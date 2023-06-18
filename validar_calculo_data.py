# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date, col, months_between, current_date
from pyspark.sql.types import IntegerType
from pathlib import os
from dotenv import load_dotenv

load_dotenv()


#####################################################################
# Ler campo calculado (num_launch_days) do banco de dados
#####################################################################
# Create SparkSession
spark = SparkSession.builder \
           .appName('valida_calculo_data') \
           .config("spark.jars", r"driver\postgresql-42.5.1.jar") \
           .config("spark.executor.extraClassPath", r"driver\postgresql-42.5.1.jar") \
           .getOrCreate()

# Read table using jdbc()
df_calculado_bd = spark.read \
    .jdbc("jdbc:postgresql://localhost:5432/manipulando_datas",
          "disney.disney_movies",
          properties={"user": str(os.getenv('POSTGRES_USER')),
                      "password": str(os.getenv('POSTGRES_PASSWORD')),
                      "driver": "org.postgresql.Driver"})

# show DataFrame
df_calculado_bd.show()

#####################################################################
# ler arquivo csv e analisa transformações para o campo data
#####################################################################

df_arquivo_data = spark.read\
                .format("csv")\
                .option("sep", ";")\
                .option("header", "true")\
                .option("inferSchema", "true")\
                .load("dados_origem/disney_movies_excel.csv")
df_arquivo_data.show()
df_arquivo_data.printSchema()
df_arquivo_data_cast_date = df_arquivo_data\
                            .withColumn('cast_release_date',
                                        date_format(to_date(df_arquivo_data.release_date,
                                                            'dd/MM/yyyy'), 'yyyyMMdd'))
df_arquivo_data_cast_date.show()
df_arquivo_data_cast_date.printSchema()

df_arquivo_data_cast_date_int = df_arquivo_data_cast_date\
                                .withColumn('cast_int_cast_release_date',
                                            col('cast_release_date')
                                            .cast(IntegerType()))
df_arquivo_data_cast_date_int.show()
df_arquivo_data_cast_date_int.printSchema()

df_arquivo_data_final = df_arquivo_data_cast_date_int\
                        .withColumn('release_date',
                                    col('cast_int_cast_release_date'))

df_arquivo_data_final.show()
df_arquivo_data_final.printSchema()
