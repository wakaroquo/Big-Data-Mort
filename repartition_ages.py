import os
import pickle
import pandas as pd
import requests
import pyspark
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import common
import re


def parse_tranche(tranche: str) -> (int, int | None):
    """Parse the header of a column

    Args:
        tranche (str): header of the column under the form `X à Y ans` ou `X ans et plus`

    Raises:
        ValueError: if the input string does not follow format

    Returns:
        (int, int | None): range of age described, to 200 if the age is not bounded
    """
    res = re.findall(r"(\d+) à (\d+) ans", tranche)
    if len(res) == 1:
        return int(res[0][0]), int(res[0][1])
    
    res = re.findall(r"(\d+) ans et plus", tranche)
    if len(res) == 1:
        return int(res[0]), None
    
    raise ValueError


def parse_excel_file(excel_file: str) -> list[(int, str, int, int, int)]:
    """parse the excel file or fail trying

    Args:
        excel_file (str): path of the file

    Returns:
        list[(int, int, int, int, int)]: list of lines in the order: annee, departement, age_de, age_jusqua, population
    """
    # lecture du fichier excel autrement
    print(f"Lecture du fichier Excel {excel_file}...")
    xls = pd.ExcelFile(excel_file)


    data = []

    for sheet_name in xls.sheet_names:
        # on passe la première page inutile
        if sheet_name=='À savoir':
            continue
        
        #On traite chaque page ensuite
        print("Traitement de la feuille : année= "+ str(sheet_name))

        # Les données de header sont a la 4e ligne
        donnees = xls.parse(sheet_name, header=4)
        columns = donnees.columns

        # On ignore les 2 premières colonnes où il y a les codes et noms de département
        colonne_age = columns[2:]

        for _, ligne in donnees.iterrows():
            code_dept = str(ligne[columns[0]]).strip()

            # on filtre les lignes inutiles (type france métropolitaine total)
            if not code_dept.isdigit():
                continue

            # Construction de la liste des pairs correspondant au département actuellement étudié
            pairs = []
            for colonne in colonne_age:
                tranche_age = str(colonne).strip()

                # Trying to parse column, if not possible, it means that it is not an age slice
                try:
                    (age_de, age_jusqua) = parse_tranche(tranche_age)
                except ValueError:
                    break

                try:
                    val = int(ligne[colonne])
                except ValueError:
                    val = 0


                data.append((int(sheet_name), code_dept, age_de, age_jusqua, val))
    return data

def load_data_parquet(excel_file: str, parquet_file: str) -> pyspark.sql.DataFrame:
    """Loads the age repartition in each departement from excel file or parquet if it exists, and generate the parquet file.

    Args:
        excel_file (str): relative path of excel file
        parquet_file (str): relative path of parquet file

    Returns:
        pyspark.sql.DataFrame: dataframe describing the age repartition
    """
    spark = common.get_spark()


    # chargement du fichier pickle si la liste de dictionnaires est déjà faite
    if os.path.exists(parquet_file):
        print(f"Chargement des données depuis {parquet_file}...")
        return spark.read.parquet(parquet_file)


    schema = StructType([
        StructField('annee', IntegerType(), True),
        StructField('departement', StringType(), True),
        StructField('age_de', IntegerType(), True),
        StructField('age_jusqua', IntegerType(), True),
        StructField('population', IntegerType(), True)
    ])

    df = spark.createDataFrame(parse_excel_file(excel_file), schema=schema)

    df.write.parquet(parquet_file)

    # For debugging pruposes
    if len(sys.argv) >= 2 and sys.argv[1] == "--json":
        df.write.json(parquet_file+".json")

    return df


if __name__ == "__main__":
    data = load_data_parquet(
        excel_file=common.DOWNLOAD_AGE, 
        parquet_file=common.DATA_AGE
    )


