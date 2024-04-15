import os
import sys
import gc
import tempfile
import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
from minio.error import S3Error
from tqdm import tqdm

def init_minio_client():
    print("Initialisation du client MinIO...")
    try:
        client = Minio(
            "localhost:9000",
            access_key="NUxZbk2YKyGvC12QHyv8",
            secret_key="bGGAxWXxJ2dCwpxRJjybuGWmwi6eTXS75QkdgKat",
            secure=False
        )
        print("Client MinIO initialisé avec succès.")
        return client
    except S3Error as e:
        print(f"Erreur lors de l'initialisation du client MinIO: {e}")
        sys.exit(1)

def download_parquet_files_from_minio(client, bucket_name):
    print(f"Téléchargement des fichiers Parquet depuis le bucket MinIO: {bucket_name}")
    try:
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            if obj.object_name.endswith('.parquet'):
                print(f"Téléchargement du fichier {obj.object_name}...")
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.parquet')
                client.fget_object(bucket_name, obj.object_name, temp_file.name)
                print(f"Fichier {obj.object_name} téléchargé avec succès.")
                yield temp_file.name
                temp_file.close()
    except S3Error as e:
        print(f"Erreur lors du téléchargement des fichiers depuis MinIO: {e}")
        sys.exit(1)

def write_data_postgres(dataframe: pd.DataFrame) -> bool:
    print("Tentative d'écriture des données dans PostgreSQL...")
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    database_url = f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@" \
                   f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    try:
        engine = create_engine(database_url)
        dataframe.to_sql(db_config["dbms_table"], engine, index=False, if_exists='append')
        print(f"Données écrites avec succès dans la table {db_config['dbms_table']} de PostgreSQL.")
        return True
    except Exception as e:
        print(f"Erreur lors de l'écriture dans la base de données: {e}")
        return False

def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    print("Nettoyage des noms de colonnes du DataFrame...")
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe

def main() -> None:
    minio_client = init_minio_client()
    bucket_name = "atl-datamart-project"

    files_to_process = list(download_parquet_files_from_minio(minio_client, bucket_name))
    print(f"{len(files_to_process)} fichiers à traiter.")

    for file_path in tqdm(files_to_process, desc="Traitement des fichiers", unit="file"):
        try:
            print(f"Lecture du fichier Parquet : {file_path}")
            parquet_df = pd.read_parquet(file_path, engine='pyarrow')
            clean_df = clean_column_name(parquet_df)
            if not write_data_postgres(clean_df):
                print(f"Échec de l'écriture des données du fichier {file_path} dans PostgreSQL.")
        finally:
            os.remove(file_path)
        gc.collect()

    print("Processus terminé.")

if __name__ == '__main__':
    sys.exit(main())