import os
import requests
import common


def fetch_resources(page):
    params = {
        'page': page,
        'page_size': 6, 
        'type': 'main',         
    }
    response = requests.get(common.DECES_DB_URL, params=params)
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


def download_deces():
    all_resources = get_all_resources()
    if not os.path.exists(common.DOWNLOAD_DECES):
        os.makedirs(common.DOWNLOAD_DECES)

    for resource in all_resources:
        if resource['format'] == 'txt' and 'deces-' in resource['title'] and 'm' not in resource['title'].lower() and 't4' not in resource['title'].lower():
            file_url = resource['url']
            file_name = resource['title']
            file_path = os.path.join(common.DOWNLOAD_DECES, file_name)
            if not os.path.exists(file_path):
                print(f"Téléchargement de {file_name}...")
                file_response = requests.get(file_url)
                file_response.raise_for_status()
                with open(file_path, 'wb') as file:
                    file.write(file_response.content)


def download_ages():
    age=requests.get(common.AGE_DB_URL)
    with open(common.DOWNLOAD_AGE, 'wb') as file:
        file.write(age.content)


if __name__ == "__main__":
    download_deces()
    download_ages()
