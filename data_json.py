# Databricks notebook source
#Silver VRA

# COMMAND ----------

dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20216-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20217-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20218-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20219-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_202110-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_202111-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20211-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20212-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20214-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20213-3.json
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20215-3.json

# COMMAND ----------

#LEITURA DE DADOS da VRA da camada Bronze

# COMMAND ----------

from pyspark.sql.functions import lower, col


df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/VRA_20217-1.json")


# COMMAND ----------

display(df1)

# COMMAND ----------

# Tratamentos de dados do VRA

# COMMAND ----------

def snake_case(df):
    for column in df.columns:
        new_column = column.lower().replace(' ','_')
        df = df.withColumnRenamed(column, new_column)
    return df

df_csv = snake_case(df1)

# COMMAND ----------

display(df_csv)

# COMMAND ----------

df_csv.write.format("delta").save("/storage/Silver/VRA")

# COMMAND ----------

#Criar nova tabela aerodromos

# COMMAND ----------

import json
import requests
import time
from pyspark.sql.types import *
from pyspark.sql import functions as F
from delta.tables import *



df_aerodromo = spark.read.format("delta").load("/storage/Silver/VRA")

# COMMAND ----------

display(df_aerodromo)

# COMMAND ----------

df_aero = df_aerodromo.select("icaoaeródromodestino").distinct().collect()

# COMMAND ----------

display(df_aero)

# COMMAND ----------

df_abend = []

for cod in df_aero:
    url = "https://airport-info.p.rapidapi.com/airport"
    querystring = {"icao":cod}
    headers = {
	"X-RapidAPI-Key": "b441e54837msh2fdb3679a41e476p1b062ajsn2ec76f7ce557",
	"X-RapidAPI-Host": "airport-info.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    df_abend.append(cod)
    display(response.json())


# COMMAND ----------

print(df_abend)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

schema = StructType([ \
    StructField("id",IntegerType(),True), 
    StructField("iata",StringType(),True),\
    StructField("icao",StringType(),True),\
    StructField("name", StringType(), True),\
    StructField("location", StringType(), True),\
    StructField("street_number", StringType(), True),\
    StructField("city", StringType(), True),\
    StructField("country_iso", StringType(), True),\
    StructField("country", StringType(), True),\
    StructField("postal_code", StringType(), True),\
    StructField("phone", StringType(), True),\
    StructField("latitude", StringType(), True), \
    StructField("longitude", StringType(), True),\
    StructField("uct", StringType(), True),\
    StructField("website", StringType(), True)                 
  ])

df_test =spark.createDataFrame(df_abend, schema=schema)

df_test.printSchema()

display(df_test)



# COMMAND ----------

df_aero.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save("/storage/Silver/VRA")

# COMMAND ----------

# #Para cada companhia aérea trazer a rota(origem-destino) mais utilizada com as seguintes informações:
# 	Razão social da companhia aérea
# 	Nome Aeroporto de Origem
# 	ICAO do aeroporto de origem
# 	Estado/UF do aeroporto de origem
# 	Nome do Aeroporto de Destino
# 	ICAO do Aeroporto de destino
# 	Estado/UF do aeroporto de destino

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from(
# MAGIC select
# MAGIC 	'razão_social','icaoaeródromoorigem','icaoaeródromodestino','endereço_sede',icao,
# MAGIC 	
# MAGIC 	dense_rank () OVER (PARTITION BY 'icaoaeródromoorigem', 'icaoaeródromodestino' ORDER BY 'razão_social' DESC) as rn
# MAGIC from
# MAGIC 	Bronze.VRA INNER JOIN Bronze.AIR_CIA on  VRA.icao  = AIR_CIA.icao
# MAGIC ) 
# MAGIC order by rn desc

# COMMAND ----------
