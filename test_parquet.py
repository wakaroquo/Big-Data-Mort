from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import common

if __name__ == "__main__":
    spark = common.get_spark()

    if len(sys.argv) == 2:
        file = sys.argv[1]
    else:
        file = common.DATA_DECES

    df_test = spark.read.parquet(file)
    df_test.show(10, truncate=False)
    df_test.printSchema()

#Si tous ce passe bien, affiche le schéma parquet ainsi que les 10 premières entrées
