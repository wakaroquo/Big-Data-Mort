from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from pathlib import Path
import os
import death

# Schéma attendu pour les données de décès comme précisé sur https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/ (nécessité de le définir car fichier txt en non json)
DEATH_RECORD_SCHEMA = StructType([
    StructField("nom", StringType(), True),
    StructField("prenoms", StringType(), True),
    StructField("sexe", IntegerType(), True),
    StructField("date_naissance", DateType(), True),
    StructField("code_lieu_naissance", StringType(), True),
    StructField("commune_naissance", StringType(), True),
    StructField("pays_ou_lieu_naissance", StringType(), True),
    StructField("date_deces", DateType(), True),
    StructField("code_lieu_deces", StringType(), True),
    StructField("num_acte_deces", StringType(), True)
])

def get_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("TxtToParquetFixedWidth") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation, G1 Concurrent GC") \
        .getOrCreate()
#j'ai essayé ce que les warning disaient pour le garbage collector, pas sur que ça change grand chose
#la config datatime est nécessaire vu qu'il y a des dates avant 1900, lesquels ont une écriture différente selon le mode legacy ou hybride, cf doc sparks

def parse_line(line : str) -> [death]:
    # This should really return an option type but whatever, python…
    try:
        return death.death(line)
    except e:
        return []


def first_read(data_file_paths: list[str], schema_path: str) -> DataFrame:
    #comme spécifié, les expressions sont de taille fixe dans les fichiers txt
    data_file_paths=os.path.join(DATA_FOLDER,'*')
    df = spark.read.text(data_file_paths)
    return df.rdd.flatMap(parse_line)



def process_death_data(df: DataFrame) -> DataFrame:
    """Nettoyage et transformation complémentaires des données"""
    return df.filter(col("date_deces") > col("date_naissance")) \
             .withColumn("age_deces", floor(datediff(col("date_deces"), col("date_naissance")) / 365.25)) \
             .withColumn("annee_deces", year(col("date_deces")))

#fonction main proche du tp3
if __name__ == "__main__":
    spark = get_spark()
    print("Version de Spark :", spark.version)
    DATA_FOLDER = "download" 
    SCHEMA_PATH = "/tmp/death_record_schema.json"
    
    df = first_read(DATA_FOLDER, SCHEMA_PATH)
    
    
    processed_df = process_death_data(df)
    
    processed_df.write.partitionBy("annee_deces", "code_lieu_deces") \
        .parquet("./data", mode="overwrite")
    
    spark.stop()
