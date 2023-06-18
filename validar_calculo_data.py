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

