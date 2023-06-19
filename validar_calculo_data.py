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
#####################################################################
# aplicar calculo no arquivo de origem
#####################################################################


def calcula_mes_entre_int_e_today(df_data_inicio_int):
    try:
        data_atual = current_date()
        print(data_atual)

        df_data_inicio_int.show()
        df_data_inicio_int.printSchema()

        # não é necessário formatar, convertendo para data já sai yyyy-MM-dd
        # df_data_inicio_int_date = df_data_inicio_int.withColumn('release_date_cast',
        #                                                        date_format(to_date(df_data_inicio_int.release_date,
        #                                                                'yyyyMMdd'), 'yyyy-MM-dd'))

        df_data_inicio_int_date = df_data_inicio_int.withColumn('release_date_cast',
                                                                to_date(df_data_inicio_int.release_date, 'yyyyMMdd'))

        print('-----depois do cast para datetype')
        df_data_inicio_int_date.show()
        df_data_inicio_int_date.printSchema()
        '''
        df_data_inicio_string = df_data_inicio_int_date.withColumn('release_date_string',
                                                                   col('release_date_cast')
                                                                   .cast(StringType()))

        print('-----depois do cast para stringtype separado por -')
        df_data_inicio_string.show()
        df_data_inicio_string.printSchema()
        '''
        print('-----calculando diferença entre meses')
        # codigo abaixo retorna um double
        # df_arquivo_calculado_double = df_data_inicio_int_date.withColumn('dif_meses_double', months_between(data_atual, col('release_date_cast')))
        # df_arquivo_calculado_int = df_arquivo_calculado_double.withColumn('dif_meses',
        #                                                                  col('dif_meses_double')
        #                                                                  .cast(IntegerType()))

        df_arquivo_calculado_double = df_data_inicio_int_date.withColumn('dif_meses_double_cast_int', months_between(data_atual, col('release_date_cast')).cast(IntegerType()))
        df_arquivo_calculado_int = df_arquivo_calculado_double.withColumn('dif_meses',
                                                                          col('dif_meses_double_cast_int')
                                                                          .cast(IntegerType()))

        return df_arquivo_calculado_int
    except Exception as e:
        print(e)


df_arquivo_calculado_int = calcula_mes_entre_int_e_today(df_arquivo_data_final)

df_arquivo_calculado_int.show()
df_arquivo_calculado_int.printSchema()

# df_arquivo_calculado_int.write.csv('dados_calculado/final_calculado.csv')
#####################################################################
# validar se valores inseridos na tabela foram calculados corretamente
#####################################################################

condicao = [((df_arquivo_calculado_int.id == df_calculado_bd.id) & (df_arquivo_calculado_int.dif_meses_double_cast_int != df_calculado_bd.num_launch_days))]

df_inconsistencia = df_arquivo_calculado_int.join(df_calculado_bd,
                                                  on=condicao, how='left_semi')


df_inconsistencia.show()
df_parquet_inconsistencia = df_inconsistencia.select('id', 'movie_title', col('dif_meses').alias('num_launch_days'), 'genre', 'mpaa_rating', 'total_gross', 'inflation_adjusted_gross')
df_parquet_inconsistencia.show()
df_parquet_inconsistencia.write.parquet('dados_calculado/incosistencia.parquet')
