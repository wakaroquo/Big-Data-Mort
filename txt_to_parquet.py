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
        return death.death(line).__dict__.__str__()
    except ValueError:
        # Case where the line is not parsable, give up
        return []


def first_read(data_file_paths: list[str]) -> DataFrame:
    # Read all the files and parse everything that is parsable
    data_file_paths=os.path.join(DATA_FOLDER,'*')
    df = spark.read.text(data_file_paths)
    return df.rdd.flatMap(parse_line)

#fonction main proche du tp3
if __name__ == "__main__":
    spark = get_spark()
    print("Version de Spark :", spark.version)
    DATA_FOLDER = "download"
    
    parsed = first_read(DATA_FOLDER)

    parsed.saveAsTextFile("data/parsed.json")

    # This first read should also write a schema, for further
    # parsing, but it is merely the `death` class schema.

    # Todo: remove fileds of `death` that we do not plan on using
    # Todo: load the json, and write it to parquet
    
