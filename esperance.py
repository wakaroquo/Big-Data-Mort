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
    total_deces = donnee.count()
    print("Nombre total de décès enregistrés : "+str(total_deces))
    donnee = donnee.withColumn("age_deces", year(col("date_deces")) - year(col("date_naissance"))) 
    donnee_invalide = donnee.filter(col("date_naissance").isNull() | col("date_deces").isNull())
    nombre_individus_invalide = donnee_invalide.count()

    #On supprimer les données les colones avec un code_lieu décès manquant
    donnee = donnee.filter(col("code_lieu_deces").isNotNull() & (col("code_lieu_deces") != ""))

    #calcul de l'age moyen de décès
    age_moyen_deces = donnee.groupBy("code_lieu_deces").agg(avg("age_deces").alias("age_moyen_deces"),count("*").alias("nombre_individus"))
    print("Nombre d'individus avec une date de naissance ou de décès manquante :"+str(nombre_individus_invalide))

    # Pour faire des tops, on applique un filtre sql afin de ne pas avoir les communes avec moins de 4 individus décédés dans les données, souvent du à un ajout tardif de la commune dans les données
    age_moyen_deces_filtre = age_moyen_deces.filter(col("nombre_individus") > 10000)

    # Top 10 communes avec la plus haute espérance de vie
    top_10_highest = age_moyen_deces_filtre.orderBy(desc("age_moyen_deces")).limit(10)
    print("Top 10 communes avec le plus haut age moyen de décès :")
    top_10_highest.show(truncate=False)

    # Top 10 communes avec la plus faible espérance de vie
    top_10_lowest = age_moyen_deces_filtre.orderBy(asc("age_moyen_deces")).limit(10)
    print("Top 10 communes avec le plus faible age moyen de décès :")
    top_10_lowest.show(truncate=False)

    #Maintenant on fait par département, même chose que précédemment mais la ligne suivant permet d'agréger en fonction de la substring des 2 premiers caractères du code département
    donnee = donnee.withColumn("code_departement", substring(col("code_lieu_deces"), 1, 2))

    #On supprimer les données les colones avec un code de département qu'on vient d'obtenir manquant
    donnee = donnee.filter(col("code_departement").isNotNull() & (col("code_departement") != ""))
    age_moyen_deces_departement = donnee.groupBy("code_departement") \
        .agg(avg("age_deces").alias("age_moyen_deces"), count("*").alias("nombre_individus"))

    age_moyen_deces_departement_filtre = age_moyen_deces_departement.filter(col("nombre_individus") > 10000)

    top_10_highest_dep = age_moyen_deces_departement_filtre.orderBy(desc("age_moyen_deces")).limit(10)
    print("Top 10 DÉPARTEMENTS avec la plus haute espérance de vie :")
    top_10_highest_dep.show(truncate=False)

    top_10_lowest_dep = age_moyen_deces_departement_filtre.orderBy(asc("age_moyen_deces")).limit(10)
    print("Top 10 DÉPARTEMENTS avec la plus faible espérance de vie :")
    top_10_lowest_dep.show(truncate=False)