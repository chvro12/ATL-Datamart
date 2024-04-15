from minio import Minio
from minio.error import S3Error
import urllib.request
import os
import sys
import ssl

# Désactivation de la vérification SSL pour le téléchargement
ssl._create_default_https_context = ssl._create_unverified_context

def main():
    grab_data()
    write_data_minio()

def grab_data() -> None:
    urls = [
        'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-12.parquet',
    ]

    save_dir = "../../data/raw"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f"Dossier {save_dir} créé.")

    for url in urls:
        file_name = url.split('/')[-1]
        save_path = os.path.join(save_dir, file_name)
        print(f"Tentative de téléchargement de {file_name}...")

        try:
            urllib.request.urlretrieve(url, save_path)
            print(f"{file_name} téléchargé avec succès et enregistré dans {save_path}.")
        except Exception as e:
            print(f"Échec du téléchargement de {file_name}. Erreur : {e}")

def write_data_minio():
    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket_name = "atl-datamart-project"
    try:
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print(f"Bucket {bucket_name} créé.")
        else:
            print(f"Bucket {bucket_name} existe déjà.")

        data_dir = "../../data/raw"
        for file_name in os.listdir(data_dir):
            if file_name.endswith(".parquet"):
                file_path = os.path.join(data_dir, file_name)
                print(f"Tentative de téléversement de {file_name} dans {bucket_name}...")

                with open(file_path, "rb") as file_data:
                    file_stat = os.stat(file_path)
                    client.put_object(
                        bucket_name,
                        file_name,
                        file_data,
                        file_stat.st_size
                    )
                print(f"{file_name} téléversé avec succès dans {bucket_name}.")

    except S3Error as e:
        print(f"Erreur MinIO lors de l'accès au bucket {bucket_name} : {e}")
    except Exception as e:
        print(f"Erreur inattendue : {e}")

if __name__ == '__main__':
    sys.exit(main())
