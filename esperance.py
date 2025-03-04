from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *


def get_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("TxtToJsonFixedWidth") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .getOrCreate()

if __name__ == "__main__":
    spark = get_spark()
    donnee = spark.read.parquet("output_parquet")
    donnee = donnee.filter(col("date_naissance").isNotNull() & col("date_deces").isNotNull())
    donnee = donnee.withColumn("age_deces", year(col("date_deces")) - year(col("date_naissance"))) 
    donnee_invalide = donnee.filter(col("date_naissance").isNull() | col("date_deces").isNull())
    nombre_individus_invalide = donnee_invalide.count()
    esperance = donnee.groupBy("code_lieu_naissance").agg(avg("age_deces").alias("esperance_vie"),count("*").alias("nombre_individus"))
    print("Nombre d'individus avec une date de naissance ou de décès manquante :"+str(nombre_individus_invalide))

    # Pour faire des tops, on applique un filtre sql afin de ne pas avoir les communes avec moins de 4 individus décédés dans les données, souvent du à un ajout tardif de la commune dans les données
    esperance_filtre = esperance.filter(col("nombre_individus") > 10)

    # Top 10 départements avec la plus haute espérance de vie
    top_10_highest = esperance_filtre.orderBy(desc("esperance_vie")).limit(10)
    print("Top 10 départements avec la plus haute espérance de vie :")
    top_10_highest.show(truncate=False)

    # Top 10 départements avec la plus faible espérance de vie
    top_10_lowest = esperance_filtre.orderBy(asc("esperance_vie")).limit(10)
    print("Top 10 départements avec la plus faible espérance de vie :")
    top_10_lowest.show(truncate=False)
