from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from carte import carte
import common


def filter_group_deces(deces: DataFrame) -> DataFrame:
    """Filtre les éléments des décès cohérents et les regroupe par année de décès, département, et âge de décès

    Args:
        deces (DataFrame): Dataframe contenant la liste des décès tels que émis dans `parse_deces`

    Returns:
        DataFrame: avec quatre colones : code_departement, annee_deces, age_deces, nombre_deces
    """

    return deces.withColumn(# On infère le département de décès
        "code_departement", substring(col("code_lieu_deces"), 1, 2)
        )\
            .filter(
                col("code_departement").isNotNull() & (col("code_departement") != "")
            )\
            .withColumn( # On ajoute les colonnes qui ont du sens
                "annee_deces", year(col("date_deces"))
            )\
            .withColumn(
                "age_deces", round(months_between(col("date_deces"), col("date_naissance")) / 12).cast(IntegerType())
            )\
            .groupBy( # On regroupe les décès par département et par an
                ["code_departement", "annee_deces", "age_deces"]
            )\
            .agg(count("*").cast(IntegerType()).alias("nombre_deces"))


def mapping_extrapole(row: Row) -> list[Row]:
    """Prend une ligne de la répartion de la population et en extrapole la répartition par âge et non par tranche d'âge

    Args:
        row (Row): ligne avec les champs annee, departeemtn, population, age_de et age_jusqua

    Returns:
        List(Row): tableau de lignes avec les champs annee, departement, population, age
    """

    # Si c'est un intervalle, on en calcul la longeur, sinon sa longeur est 1
    nb_years = 1 if row.age_jusqua is None else (row.age_jusqua - row.age_de) + 1
    return [
        Row(
            annee=row.annee,
            code_departement=row.departement,
            age=i,
            # On suppose que la population est équirépartie dans la tranche d'âge
            population=int(row.population/nb_years)
        )
        for i in range(row.age_de, row.age_de + nb_years)
    ]

def extrapolate_age_partitionning(population: DataFrame) -> DataFrame:
    """Extrapole l'ensemble de la population par âge grâce à la population par tranche d'âge

    Args:
        population (DataFrame): jeu de données avec les champs annee, departeemtn, population, age_de et age_jusqua

    Returns:
        DataFrame: jeu de données correspondant avec les champs annee, departement, population, age
    """
    # On applique le mapping de la fonction mapping_extrapole, et on s'assure que les bons types sont respéctés
    return population.rdd.flatMap(mapping_extrapole).toDF(
        StructType([
            StructField('annee', IntegerType(), True),
            StructField('code_departement', StringType(), True),
            StructField('age', IntegerType(), True),
            StructField('population', IntegerType(), True)
        ])
    )

def pi_list(age_pis: list[Row]) -> list[float]:
    """Fonction auxiliaire qui transforme des lignes en un tableau complet

    Args:
        age_pis (list[Row]): liste de lignes de la forme age, pi, avec pi la probabilité de rester en vie à l'age

    Returns:
        list[float]: liste qui dans la cellule i contient la probabilité de survivre à l'année i+1
    """

    # On calcul l'âge maximal
    max = 0
    for row in age_pis:
        if row.age > max:
            max = row.age

    # Par défaut, s'il n'y a pas de décès, la probabilité de mourir est nulle, donc de survivre vaut 1
    pis = [1] * (max)
    for row in age_pis:
        pis[row.age-1] = row.pi
    return pis



@udf(returnType=FloatType())
def life_expecancy(age_pis: list[Row]) -> float:
    """Mapper qui transforme une liste de probabilité de survie à chaque âge en espérance de vie

    Args:
        age_pis (list[Row]): liste de lignes de la forme age, pi, avec pi la probabilité de rester en vie à l'age

    Returns:
        int: Espérance de vie
    """
    # On range les pi dans un tableau
    pis = pi_list(age_pis)

    # On applique la formule :
    # E = sum_{i=0}^\infty \prod_{j=0}^n p_j
    res = 0
    prod = 1
    for pi in pis:
        prod *= pi
        res += prod
    return res


def compute_life_expectancy(deces: DataFrame, population: DataFrame) -> DataFrame:
    """Compute life expectancy from data

    Args:
        deces (DataFrame): Dataframe contenant le fichier des décès
        population (DataFrame): Dataframe contenant la répartition de la population

    Returns:
        DataFrame: Représentation de l'espérance de vie dans chaque département, et chaque année
    """
    # Prétraitement des données
    deces = filter_group_deces(deces)
    population = extrapolate_age_partitionning(population)

    return deces\
        .join( # On fusionne les deux données
        population,
        [
            deces.code_departement == population.code_departement,
            deces.age_deces == population.age,
            deces.annee_deces == population.annee
        ]
    )\
        .drop( # On supprime les lignes redondantes pour y voir plus clair
            population.code_departement, "age_deces", "annee_deces"
        )\
        .withColumn( # On calcul la probabilité de rester en vie 
            "pi", when(col("population") != 0, (col("population") - col("nombre_deces")) / col("population")).otherwise(0)
        )\
        .withColumn(  # On fusionne age et pi pour pouvoir les avoir au moment de la fusion
            "age_pi", struct(col("age"), col("pi")))\
        .groupBy(["code_departement", "annee"])\
        .agg( # On fusionne et on garde la colone age et pi
            collect_list("age_pi").alias("list_age_pi"))\
        .withColumn( # On calcule l'espérance de vie
            "esperance", life_expecancy(col("list_age_pi")))\
        .drop( # On supprime cette colones car inutile et longue, c'est un tableau
            "list_age_pi")

def compute_age_moyen_deces_commune(deces: DataFrame):
    return deces.withColumn("age_deces", datediff(col("date_deces"), col("date_naissance")) / 365)\
    .filter( # On supprime les données les colonnes avec un code_lieu décès manquant (normalement 0, cf info_deces)
        col("code_lieu_deces").isNotNull() & (col("code_lieu_deces") != ""))\
    .groupBy( #calcul de l'age moyen de décès
    "code_lieu_deces").agg(avg("age_deces").alias("age_moyen_deces"),count("*").alias("nombre_individus"))\
    .filter(col("nombre_individus") > 10000)

def compute_age_moyen_deces_departement(deces: DataFrame):
    return deces.withColumn("age_deces", datediff(col("date_deces"), col("date_naissance")) / 365)\
    .filter( # On supprime les données les colonnes avec un code_lieu décès manquant (normalement 0, cf info_deces)
        col("code_lieu_deces").isNotNull() & (col("code_lieu_deces") != ""))\
    .withColumn( #on regroupe les communes par departement (la ligne suivant permet d'agréger en fonction de la substring des 2 premiers caractères du code département)
        "code_departement", substring(col("code_lieu_deces"), 1, 2))\
    .groupBy( #calcul de l'age moyen de décès par departement
    "code_departement").agg(avg("age_deces").alias("age_moyen_deces"),count("*").alias("nombre_individus"))\
    .filter(col("nombre_individus") > 10000)

def info_deces(raw_deces: DataFrame):
    total_deces = raw_deces.count()
    print("Nombre total de décès enregistrés : "+str(total_deces))
    donnee_invalide = raw_deces.filter(col("date_naissance").isNull() | col("date_deces").isNull())
    nombre_individus_invalide = donnee_invalide.count()
    print("Nombre d'individus avec une date de naissance ou de décès manquante :"+str(nombre_individus_invalide))
    return

def fun_facts_commune(deces: DataFrame):

    # Top 10 communes avec la plus haute espérance de vie
    top_10_highest = deces.orderBy(desc("age_moyen_deces")).limit(10)
    print("Top 10 communes avec le plus haut age moyen de décès :")
    top_10_highest.show(truncate=False)

    # Top 10 communes avec la plus faible espérance de vie
    top_10_lowest = deces.orderBy(asc("age_moyen_deces")).limit(10)
    print("Top 10 communes avec le plus faible age moyen de décès :")
    top_10_lowest.show(truncate=False)

    return

def fun_facts_departement(deces: DataFrame):
    # Top 10 departement avec la plus haute espérance de vie
    top_10_highest_dep = deces.orderBy(desc("age_moyen_deces")).limit(10)
    print("Top 10 DÉPARTEMENTS avec le plus haut age moyen de décès :")
    top_10_highest_dep.show(truncate=False)

    # Top 10 departement avec la plus faible espérance de vie
    top_10_lowest_dep = deces.orderBy(asc("age_moyen_deces")).limit(10)
    print("Top 10 DÉPARTEMENTS avec le plus faible age moyen de décès :")
    top_10_lowest_dep.show(truncate=False)

    return

if __name__ == "__main__":
    spark = common.get_spark()
    repartition = spark.read.parquet(common.DATA_AGE)
    raw_deces = spark.read.parquet(common.DATA_DECES)
    info_deces(raw_deces)
    deces_commune=compute_age_moyen_deces_commune(raw_deces)
    deces_departement=compute_age_moyen_deces_departement(raw_deces)
    fun_facts_commune(deces_commune)
    fun_facts_departement(deces_departement)

    carte_esperance=carte(compute_life_expectancy(raw_deces, repartition),"esperance")
    carte_esperance.dessine()

    carte_deces=carte(deces_departement,"age_moyen_deces")
    carte_deces.dessine()

    carte_deces.afficher()
    carte_esperance.afficher()