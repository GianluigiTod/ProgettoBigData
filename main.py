from pyspark.sql import SparkSession
#import gui
from queries import top_users, top_hashtags
from preprocessing import preprocessing

def main():
    spark = SparkSession.builder.appName("TwitterAnalysis").config("spark.driver.memory", "2g").getOrCreate()

    dataset_path = "C:\\Users\\Gianluigi\\Desktop\\esame di big data\\Formatted dataset\\2020-10\\tweet_USA_2_october.csv\\tweet_USA_2_october.csv"
    df = spark.read.option("header", True).csv(dataset_path)

    df = preprocessing(df, "nome")
    """
    engagement, tweet_count = top_users(df)
    print("\nTop utenti per numero di tweet:")
    tweet_count.show(10, truncate=False)

    print("\nTop utenti per like totali:")
    engagement.show(10, truncate=False)


    hashtag_count, hashtag_engagement = top_hashtags(df)
    print("\nTop hashtag per numero di tweet:")
    hashtag_count.show(10, truncate=False)

    print("\nTop hashtag per like medi:")
    hashtag_engagement.show(10, truncate=False)
    """
    df.show()


if __name__ == "__main__":
    main()

