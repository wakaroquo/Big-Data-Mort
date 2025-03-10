import os
import pickle
import pandas as pd
import requests
# Bien lire le commantaire suivant !!!
# A retenir: ce fichier python permet de créer une liste derrière sauvegardée en fichier pickle (pop_data.pkl)
# Afin d'avoir les données sur l'année i , il faut chercher dans liste_entière[2023-i]. On a alors un dictionnaire qui, a chaque code de département, associe une liste de paires ('j,j+4',nombre de personnes dans la tranche d'âge [j,j+4])
# A noter: parmi la liste de pair, une fois qu'on dépasse le total, on obtient des tranche d'âge du type '60,64.2', ce qui correspond au données pondérées par le sexe (ici 2 correspond aux femmes), ces données sont inutiles dans le traitement actuel prévu.
def load_data(excel_file, pickle_file):

    # chargement du fichier pickle si la liste de dictionnaires est déjà faite
    if os.path.exists(pickle_file):
        print(f"Chargement des données depuis {pickle_file}...")
        with open(pickle_file, "rb") as f:
            liste_entiere = pickle.load(f)
        return liste_entiere

    # lecture du fichier excel autrement
    print(f"Lecture du fichier Excel {excel_file}...")
    xls = pd.ExcelFile(excel_file)
    liste_entiere = []

    for sheet_name in xls.sheet_names:
        # on passe la première page inutile
        if sheet_name=='À savoir':
            continue
        
        #On traite chaque page ensuite
        print("Traitement de la feuille : année= "+ str(sheet_name))

        # Les données de header sont a la 4e ligne
        donnees = xls.parse(sheet_name, header=4)

        # Dictionnaire de l'année étudiée (code_dept -> liste de paires (tranche d'âge, nombre de personne dans cette tranche d'âge) )
        year_data = {}
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
                tranche_age = tranche_age.replace(" ans", "").replace(" à ", ",")
                population = ligne[colonne]
                pairs.append((tranche_age, population))

            # On stocke dans le dictionnaire
            year_data[code_dept] = pairs

        # On insère dans la liste des dictionnaires de chaque année
        liste_entiere.append(year_data)

    # On sauvegarde la liste en pickle
    print(f"Sauvegarde des données dans {pickle_file}...")
    with open(pickle_file, "wb") as f:
        pickle.dump(liste_entiere, f)

    return liste_entiere


if __name__ == "__main__":
    file_response = requests.get("https://www.insee.fr/fr/statistiques/fichier/1893198/estim-pop-dep-sexe-aq-1975-2023.xls")
    file_response.raise_for_status()
    with open("ages.xls", 'wb') as file:
        file.write(file_response.content)
    data = load_data(
        excel_file="ages.xls", 
        pickle_file="pop_data.pkl"
    )


    # Affichage d'une partie de la liste:
    print("Structure de data[0] pour la feuille la plus récente :")
    for dept_code, pairs in list(data[0].items())[:3]: 
        print(f"Département {dept_code}: {pairs}")
