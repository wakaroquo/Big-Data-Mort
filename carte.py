from pyspark.sql import SparkSession, DataFrame
import geopandas as gpd
import matplotlib.pyplot as plt

class carte:
    # 2 attributs, les données spark a étudier, et le nom de la colonne à étudier (le nom de la colonne des departements est supposé etre "code_departement")
    def __init__(self, spark_data, column_name):
        self.spark_data = spark_data
        self.column_name = column_name

    def dessine(self):
        # on récupère le peu de données spark qu'il reste, on peut traiter le résultat final avec pandas étant donné que nous n'avons plus les problématiques de big data ici, on converti d'abord les données
        donnees_pandas = self.spark_data.select("code_departement", self.column_name).toPandas()

        # on récupère la carte des départements français
        url_geojson = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements.geojson"
        carte = gpd.read_file(url_geojson)
        carte_remplie = carte.merge(donnees_pandas, left_on="code", right_on="code_departement", how="left")

        # configuration de matplotlib a partir de maintenant
        fig, ax = plt.subplots(1, 1, figsize=(10, 8))

        carte_remplie.plot(
            column=self.column_name,
            cmap="magma",        
            legend=True,     
            linewidth=0.5, edgecolor="black",
            ax=ax
        )

        ax.set_title(self.column_name+" par département", fontsize=14)
        ax.axis("off")

        #sauvegarde de la carte correspondant à l'instance
        plt.savefig('carte_'+self.column_name+'.png')

    #fonction à appeler pour afficher la carte correspondant a l'instance
    def afficher(self):
        plt.clf()
        img = plt.imread('carte_'+self.column_name+'.png')
        plt.imshow(img)
        plt.axis("off")
        plt.show()