# Databricks notebook source
#SILVER AIR_CIA

# COMMAND ----------

dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/ANAC_20211220_203643-4.csv
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/ANAC_20211220_203627-4.csv
dbfs:/FileStore/shared_uploads/murilogouvearibeiro@gmail.com/ANAC_20211220_203733-4.csv

# COMMAND ----------

#LEITURA DE DADOS da AIR_CIA da camada Bronze

# COMMAND ----------

from pyspark.sql.functions import lower, col , split


df1 = spark.read.format("csv").option("header", "true").option("delimiter", ";").load("dbfs:/FileStore/storage/Bronze/AIR_CIA/*.csv")


# COMMAND ----------

display(df1)

# COMMAND ----------

# Tratamentos de dados do AIR_CIA

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

df1 = df_csv.withColumn('icao', split(df_csv['icao_iata'], ' ').getItem(0))\
        .withColumn('iata', split(df_csv['icao_iata'], ' ').getItem(1))
df1.display(truncate=False)

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save("/storage/Silver/AIR_CIA")

# COMMAND ----------

df_silver_aircia = spark.read.format("delta").load("/storage/Silver/AIR_CIA")

# COMMAND ----------

display(df_silver_aircia)
