from pyspark.sql import SparkSession, DataFrame


def get_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("TxtToJsonFixedWidth") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .getOrCreate()


DATA_FOLDER = "data"
DOWNLOAD_FOLDER = "download"

DOWNLOAD_DECES = DOWNLOAD_FOLDER + "/deces"
DOWNLOAD_AGE = DOWNLOAD_FOLDER + "/ages.xls"

DATA_DECES = DATA_FOLDER + "/deces"
DATA_AGE = DATA_FOLDER + "/ages"

DECES_DB_URL = "https://www.data.gouv.fr/api/2/datasets/5de8f397634f4164071119c5/resources/"
AGE_DB_URL = "https://www.insee.fr/fr/statistiques/fichier/1893198/estim-pop-dep-sexe-aq-1975-2023.xls"