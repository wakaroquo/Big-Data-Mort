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
    df_test = spark.read.parquet("output_parquet")
    df_test.show(10, truncate=False)
    df_test.printSchema()

#Si tous ce passe bien, affiche le schéma parquet ainsi que les 10 premières entrées
