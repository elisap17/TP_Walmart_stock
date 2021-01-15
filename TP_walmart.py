# Importation des bibliotheques
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql import types 

# 1) Initialisation du client Spark
spark = SparkSession.builder\
                    .master("local")\
                    .appName("walmart_stock")\
                    .getOrCreate()

# 2) Importation du fichier Walmart stock
df = spark.read \
            .option("header",True) \
            .csv("walmart_stock.csv") 
# montre les 5 premieres lignes
df.show(5)

# 3) Noms des colonnes
print(df.columns)

# 4) Schema des donnees
df.printSchema()
#toutes les donnees etaient de type String, nous les transformons donc 
df = df.withColumn("Date", df["Date"].cast(types.DateType())) \
        .withColumn("Open", df["Open"].cast(types.DoubleType())) \
        .withColumn("High", df["High"].cast(types.DoubleType())) \
        .withColumn("Low", df["Low"].cast(types.DoubleType())) \
        .withColumn("Close", df["Close"].cast(types.DoubleType())) \
        .withColumn("Volume", df["Volume"].cast(types.IntegerType())) \
        .withColumn("Adj Close", df["Adj Close"].cast(types.DoubleType()))

df.printSchema()

# 5) Creation d'un nouveau dataframe avec une nouvelle colonne HV_Ratio
#python
df2 = df.withColumn("HV_Ratio", col("High")/col("Volume"))
df2.show(5)

#sql
#configuration pour utiliser SQL
df.createOrReplaceTempView("df_sql") 
spark.sql("SELECT High/Volume as HV_Ratio FROM df_sql").show()

# 6) Jour avec le prix le plus haut
#python
df.orderBy(col("High").desc()).select(col("Date")).show(1)

#sql
spark.sql("SELECT Date FROM df_sql WHERE High = (SELECT MAX(High) from df_sql)").show()

# 7) Moyenne de la colonne Close
#python
df.agg(avg("Close").alias("Moyenne_Close")).show()

#sql
spark.sql("SELECT AVG(Close) as Moyenne_Close FROM df_sql").show()

# 8) Maximum et minimum de la colonne Volume
#python
df.select(min("Volume").alias("Minimum_Volume"),max("Volume").alias("Maximum_Volume")).show()

#sql
spark.sql("SELECT MIN(Volume) as Minimum_Volume, MAX(Volume) as Maximum_Volume FROM df_sql").show()

# 9) Close < 60
#python
print(df.filter("Close < 60").count())

#sql
spark.sql("SELECT count(*) as Nb_jours FROM df_sql WHERE Close < 60").show()

# 10) Pourcentage avec High > 80
#python
print((df.filter("High > 80").count()/df.count())*100)

#sql
spark.sql("SELECT (SELECT COUNT(High) FROM df_sql WHERE High > 80) / COUNT(High) * 100 as Pourcentage FROM df_sql").show()

