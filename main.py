from pyspark.sql import SparkSession
#import gui
import sys
import os
import shutil
from queries import top_users, top_hashtags
from preprocessing import preprocessing
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pyspark.sql.types import *
from pyspark.sql.functions import split
from pyspark import StorageLevel

###FILE ANCORA DA TESTARE ###
def main():

    scelta_utente = input(
        "Vuoi utilizzare un gruppo di dataset predefinito già scaricati (loco) "
        "o vuoi scegliere i CSV da Google Drive (drive)? [loco/drive]: "
    ).lower()

    if scelta_utente == "loco":
        cartella_locale = "C:\\Users\\Gianluigi\\Desktop\\esame di big data\\Formatted dataset\\2020-10"
        csv_files = [f for f in os.listdir(cartella_locale) if f.endswith(".csv")]
        if not csv_files:
            print("Nessun file CSV trovato nella cartella locale.")
        else:
            print("\nCSV disponibili nella cartella locale:\n")
            for i, f in enumerate(csv_files):
                print(f"{i}: {f}")

            scelta = input("\nInserisci gli indici dei CSV da utilizzare (es. 0,2,3): ")
            indici = [int(x.strip()) for x in scelta.split(",")]

            indici = indici[:10]

            file_selezionati = [csv_files[idx] for idx in indici]

            for f in file_selezionati:
                src_path = os.path.join(cartella_locale, f)
                dest_path = os.path.join("download_csv_drive", f)
                shutil.copy2(src_path, dest_path)
            print("File csv correttamente selezionati.")

    elif scelta_utente == "drive":
        gauth = GoogleAuth()
        gauth.LocalWebserverAuth()
        drive = GoogleDrive(gauth)

        folder_id = "1tdl0PYZvCqwImlEDcmVKwh-zMVRtAnSp"

        file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()

        csv_files = [f for f in file_list]

        print("\nCSV disponibili:\n")
        for i, f in enumerate(csv_files):
            print(f"{i}: {f['title']}  ---> link: {f['alternateLink']}")


        scelta = input("\nInserisci gli indici dei CSV da scaricare (es. 0,2): ")
        indici = [int(x.strip()) for x in scelta.split(",")]

        indici = indici[:10]

        dimensione_totale = sum(int(csv_files[idx]['fileSize']) for idx in indici if 'fileSize' in csv_files[idx])
        dimensione_gb = dimensione_totale / (1024**3)

        print(f"\nDimensione totale dei file selezionati: {dimensione_gb:.2f} GB")
        conferma = input("Vuoi procedere con il download? (s/n): ").lower()

        if conferma == 'n':
            print("Download annullato.")
        else:
            os.makedirs("download_csv_drive", exist_ok=True)

            for idx in indici:
                file = csv_files[idx]
                percorso_locale = os.path.join("download_csv_drive", file['title'])
                file.GetContentFile(percorso_locale)
            print("File scaricati")

    else:
        print("Scelta non valida.")
        sys.exit("Programma terminato a causa di scelta non valida.")


    spark = SparkSession.builder \
        .appName("Twitter Dataset Profiling") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.local.dir", "./spark_tmp") \
        .getOrCreate()

    cartella_locale="download_csv_drive"

    file_per_spark = [os.path.join(cartella_locale, f) for f in os.listdir(cartella_locale) if f.endswith(".csv")]

    df = None
    for f in file_per_spark:
        df_temp = carica_csv_in_spark(f, spark)
        if df is None:
            df = df_temp
        else:
            df = df.unionByName(df_temp)

    df.persist(StorageLevel.MEMORY_AND_DISK)

    print("File caricati in Spark DataFrame. Numero di righe totali:", df.count())

#Ricordarsi di cancellare i file che sono nella cartella download_csv_drive



    df.show()
    '''
    df = preprocessing(df, "nome")

    engagement, tweet_count = top_users(df)
    print("\nTop utenti per numero di tweet:")
    tweet_count.show(10, truncate=False)

    print("\nTop utenti per like totali:")
    engagement.show(10, truncate=False)

    df = preprocessing(df, "hashtags")

    hashtag_count, hashtag_engagement = top_hashtags(df)
    print("\nTop hashtag per numero di tweet:")
    hashtag_count.show(10, truncate=False)

    print("\nTop hashtag per like medi:")
    hashtag_engagement.show(10, truncate=False)
    '''

# Funzione per caricare CSV in DataFrame Spark con schema completo
def carica_csv_in_spark(percorso_file, spark):
    schema = StructType([
            StructField("tweet_id", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("user_id_str", StringType(), True),
            StructField("text", StringType(), True),
            StructField("hashtags", StringType(), True),  # inizialmente stringa
            StructField("retweet_count", IntegerType(), True),
            StructField("favorite_count", IntegerType(), True),
            StructField("in_reply_to_screen_name", StringType(), True),
            StructField("source", StringType(), True),
            StructField("retweeted", BooleanType(), True),
            StructField("lang", StringType(), True),
            StructField("location", StringType(), True),
            StructField("place_name", StringType(), True),
            StructField("place_lat", FloatType(), True),
            StructField("place_lon", FloatType(), True)
        ])
    df = spark.read.csv(percorso_file, header=True, schema=schema)

    # Converti hashtags da stringa separata da virgola a array
    df = df.withColumn("hashtags", split(df["hashtags"], ","))
    return df

if __name__ == "__main__":
    main()



