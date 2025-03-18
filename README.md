# Big-Data-Mort

## Configuration générale

Le fichier `common.py` contient les configurations de _spark_,
des chemins d'accès des fichiers et les URL pour télécharger
les fichiers. 

## Source des données

Le script `download.py` permet de télécharger deux fichiers :
  - le fichier des personnes décédées (en réalité un fichier par année) ;
  - le fichier des populations par tranche d'âge.

## Parsing
 
### `parse_deces.py`

Ce script permet de lire le fichier des personnes décédées et d'en extraire
les champs tels que décrits dans la documentation du jeu de données. Ces champs
sont donc extraits puis enregistrés dans un fichier _parquet_ qu'il est plus
facile de lire.

### `repartition_ages.py`

Ce script est similaire à `parse_deces.py` : il lit le fichier de répartitions
des âges et l'exporte en format _parquet_.
