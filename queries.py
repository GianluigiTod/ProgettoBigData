from pyspark.sql.functions import col, expr, avg, desc, sum as spark_sum, round as spark_round

def top_users(df, n=10):
    top_tweet_count = df.groupBy("screen_name").count().orderBy(desc("count")).limit(n)

    # Converte in interi i campi numerici
    df = df.withColumn("favorite_count", expr("try_cast(favorite_count as int)")) \
        .withColumn("retweet_count", expr("try_cast(retweet_count as int)"))


    df = df.fillna({"favorite_count": 0, "retweet_count": 0})

    top_engagement = df.groupBy("screen_name").agg(
        spark_sum(col("favorite_count")).alias("total_likes"),
        spark_sum(col("retweet_count")).alias("total_retweets")).orderBy(desc("total_likes")) \
        .limit(n)

    return top_engagement, top_tweet_count


def top_hashtags(df, n=10):


    hashtag_count = df.groupBy("hashtags").count().orderBy(desc("count")).limit(n)

    df = df.withColumn("favorite_count", expr("try_cast(favorite_count as int)")) \
        .withColumn("retweet_count", expr("try_cast(retweet_count as int)"))

    # Engagement medio per hashtag
    hashtag_engagement = df.groupBy("hashtags") \
        .agg(
            spark_round(avg(col("favorite_count")), 2).alias("avg_likes"),
            spark_round(avg(col("retweet_count")), 2).alias("avg_retweets")).orderBy(desc("avg_likes")).limit(n)

    return hashtag_count, hashtag_engagement