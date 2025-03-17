import os
import requests
BASE_API_URL = "https://www.data.gouv.fr/api/2/datasets/5de8f397634f4164071119c5/resources/"
DOWNLOAD = "download"
DOWNLOAD_FOLDER = DOWNLOAD+"/deces"

def fetch_resources(page):
    params = {
        'page': page,
        'page_size': 6, 
        'type': 'main',         
    }
    response = requests.get(BASE_API_URL, params=params)
    response.raise_for_status()
    return response.json()['data']

def get_all_resources():
    all_resources = []
    page = 1
    while True:
        print(f"Récupération de la page {page}...")
        resources = fetch_resources(page)
        if not resources:
            break
        all_resources.extend(resources)
        page += 1
    return all_resources


def download():
    all_resources = get_all_resources()
    if not os.path.exists(DOWNLOAD_FOLDER):
        os.makedirs(DOWNLOAD_FOLDER)

    for resource in all_resources:
        if resource['format'] == 'txt' and 'deces-' in resource['title'] and 'm' not in resource['title'].lower() and 't4' not in resource['title'].lower():
            file_url = resource['url']
            file_name = resource['title']
            file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
            if not os.path.exists(file_path):
                print(f"Téléchargement de {file_name}...")
                file_response = requests.get(file_url)
                file_response.raise_for_status()
                with open(file_path, 'wb') as file:
                    file.write(file_response.content)


if __name__ == "__main__":
    download()
    age=requests.get("https://www.insee.fr/fr/statistiques/fichier/1893198/estim-pop-dep-sexe-aq-1975-2023.xls")
    with open(DOWNLOAD+"ages.xls", 'wb') as file:
        file.write(age.content)
