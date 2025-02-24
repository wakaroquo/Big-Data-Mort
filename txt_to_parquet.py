from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from pathlib import Path
import os

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

def first_read(data_file_paths: list[str], schema_path: str) -> DataFrame:
    #comme spécifié, les expressions sont de taille fixe dans les fichiers txt
    data_file_paths=os.path.join(DATA_FOLDER,'*')
    df = spark.read.text(data_file_paths)
    df = df.select(
        expr("substring(value, 1, 80) as nom_prenom"),
        expr("substring(value, 81, 1) as sexe_str"),
        expr("substring(value, 82, 8) as date_naissance_str"),
        expr("substring(value, 90, 5) as code_lieu_naissance"),
        expr("substring(value, 95, 30) as commune_naissance"),
        expr("substring(value, 125, 30) as pays_ou_lieu_naissance"),
        expr("substring(value, 155, 8) as date_deces_str"),
        expr("substring(value, 163, 5) as code_lieu_deces"),
        expr("substring(value, 168, 9) as num_acte_deces")
    )
    
    # Séparation en nom et prénom, meme si probablement inutile tant qu'on ne fait pas des statistiques sur les prénoms, puis tous les typages
    df = df.withColumn("nom", split(col("nom_prenom"), "\\*").getItem(0)) \
           .withColumn("prenoms", split(col("nom_prenom"), "\\*").getItem(1)) \
           .drop("nom_prenom")
    df = df.withColumn("sexe", col("sexe_str").cast(IntegerType())).drop("sexe_str")
    df = df.withColumn("date_naissance", 
                       to_date(when(col("date_naissance_str") == "00000000", None)
                               .otherwise(col("date_naissance_str")), "yyyyMMdd")) \
           .withColumn("date_deces", 
                       to_date(when(col("date_deces_str") == "00000000", None)
                               .otherwise(col("date_deces_str")), "yyyyMMdd")) \
           .drop("date_naissance_str", "date_deces_str")
    expected_fields = set(field.name for field in DEATH_RECORD_SCHEMA.fields)
    actual_fields = set(df.columns)
    if expected_fields != actual_fields:
        raise ValueError("Schema mismatch detected!")
    Path(schema_path).write_text(DEATH_RECORD_SCHEMA.json(), encoding="utf-8")
    return df

def next_read(data_file_paths: list[str], schema_path: str) -> DataFrame:
    """
    Lecture optimisée en se basant sur le schéma prédéfini.
    Ici, on relit le fichier texte et on applique les mêmes transformations.
    """
    with open(schema_path, "r") as f:
        schema = StructType.fromJson(json.load(f))
    
    df = spark.read.text(data_file_paths)
    
    df = df.select(
        expr("substring(value, 1, 80) as nom_prenom"),
        expr("substring(value, 81, 1) as sexe_str"),
        expr("substring(value, 82, 8) as date_naissance_str"),
        expr("substring(value, 90, 5) as code_lieu_naissance"),
        expr("substring(value, 95, 30) as commune_naissance"),
        expr("substring(value, 125, 30) as pays_ou_lieu_naissance"),
        expr("substring(value, 155, 8) as date_deces_str"),
        expr("substring(value, 163, 5) as code_lieu_deces"),
        expr("substring(value, 168, 9) as num_acte_deces")
    )
    
    df = df.withColumn("nom", split(col("nom_prenom"), "\\*").getItem(0)) \
           .withColumn("prenoms", split(col("nom_prenom"), "\\*").getItem(1)) \
           .drop("nom_prenom") \
           .withColumn("sexe", col("sexe_str").cast(IntegerType())).drop("sexe_str") \
           .withColumn("date_naissance", 
                       to_date(when(col("date_naissance_str") == "00000000", None)
                               .otherwise(col("date_naissance_str")), "yyyyMMdd")) \
           .withColumn("date_deces", 
                       to_date(when(col("date_deces_str") == "00000000", None)
                               .otherwise(col("date_deces_str")), "yyyyMMdd")) \
           .drop("date_naissance_str", "date_deces_str")
    
    #on retire les lignes problématiques
    df = df.filter(col("date_deces").isNotNull() & col("date_naissance").isNotNull())
    return df

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
    
    raw_df = first_read(DATA_FOLDER, SCHEMA_PATH)
    raw_df.printSchema()
    
    df = next_read(DATA_FOLDER, SCHEMA_PATH)
    
    processed_df = process_death_data(df)
    
    processed_df.write.partitionBy("annee_deces", "code_lieu_deces") \
        .parquet("./data", mode="overwrite")
    
    spark.stop()
