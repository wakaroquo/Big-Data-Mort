from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

COLUMN_POSITIONS = [
    ("nom_prenom", (0, 80)),
    ("sexe", (80, 81)),
    ("date_naissance", (81, 89)),
    ("code_lieu_naissance", (89, 94)),
    ("commune_naissance", (94, 124)),
    ("pays_ou_lieu_naissance", (124, 154)),
    ("date_deces", (154, 162)),
    ("code_lieu_deces", (162, 167)),
    ("num_acte_deces", (167, 176))
]
# Schéma attendu pour les données de décès comme précisé sur https://www.data.gouv.fr/fr/datasets/fichier-des-personnes-decedees/ (nécessité de le définir car fichier txt en non json)
DEATH_SCHEMA = StructType([
    StructField("nom_prenom", StringType()),
    StructField("sexe", IntegerType()),
    StructField("date_naissance", DateType()),
    StructField("code_lieu_naissance", StringType()),
    StructField("commune_naissance", StringType()),
    StructField("pays_ou_lieu_naissance", StringType()),
    StructField("date_deces", DateType()),
    StructField("code_lieu_deces", StringType()),
    StructField("num_acte_deces", StringType())
])

def get_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("TxtToJsonFixedWidth") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .getOrCreate()

def parse_fixed_width(df: DataFrame) -> DataFrame:
    exprs = [
        trim(substring(col("value"), pos[0] + 1, pos[1] - pos[0])).alias(name) 
        for name, pos in COLUMN_POSITIONS
    ]
    
    df_parsed = df.select(*exprs)

    # Conversion du sexe en entier
    df_parsed = df_parsed.withColumn("sexe", col("sexe").cast(IntegerType()))

    # Conversion des dates avec gestion des valeurs inconnues
    def clean_date(date_col):
        return when(col(date_col).rlike("0000.*"), lit(None)) \
               .otherwise(to_date(col(date_col), "yyyyMMdd"))

    df_parsed = df_parsed.withColumn("date_naissance", clean_date("date_naissance")) \
                         .withColumn("date_deces", clean_date("date_deces"))

    return df_parsed

#fonction main proche du tp3
if __name__ == "__main__":
    spark = get_spark()
    DATA_FOLDER = "download"
    
    df = spark.read.text(os.path.join(DATA_FOLDER, "*.txt"))
    parsed_df = parse_fixed_width(df)
    # ecriture en json pour débugage
    if len(sys.argv) >= 2 and sys.argv[1] == "--json":
        parsed_df.write \
            .mode("overwrite") \
            .option("dateFormat", "yyyy-MM-dd") \
            .json("output_json")
    #ecriture en parquet
    parsed_df.write \
        .mode("overwrite") \
        .parquet("output_parquet")

    # This first read should also write a schema, for further
    # parsing, but it is merely the `death` class schema.

    # Todo: remove fileds of `death` that we do not plan on using