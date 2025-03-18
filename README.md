# Big-Data-Mort

## Installation

```bash
python3 -m venv .venv --copies
source .venv/bin/activate
pip install -e .
```

## Lancement
Pour lancer le deroulement complet des scripts, il faut simplement lancer runAll.sh, qui s'occupe de lancer les scripts dans le bon ordre. La description des différents scripts est décrite ci-dessous.

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
facile et rapide de lire.

### `repartition_ages.py`

Ce script est similaire à `parse_deces.py` : il lit le fichier de répartitions
des âges et l'exporte en format _parquet_.

## Traitement

### `esperance.py`

Ce script est le script de traitement principal, qui permet à partir des fichiers parquets d'obtenir les données recherchées par commune et departement (age moyen de décès et esperance de vie)

### `carte.py`

Ce script permet, à partir de données traitées spark (ayant une colonne code_departement et un attribut a étudier), d'afficher une carte de la france représentant l'attribut selectionné selon la position géographique. Toute carte dessinée est sauvegardée au format png dans le dossier courrant.

### `test_parquet.py`

Ce script n'est pas à appeler pour le fonctionnement classique, il permet seulement d'afficher les 10 premières entrées des fichiers spark afin de voir si les fichiers en questions ont bien été écrits.
