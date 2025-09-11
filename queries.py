from pyspark.sql.functions import col, count, avg, desc, sum as spark_sum

def top_users(df, n=10):
    top_tweet_count = df.groupBy("screen_name").count().orderBy(desc("count")).limit(n)

    top_engagement = df.groupBy("screen_name").agg(
        spark_sum(col("favorite_count")).alias("total_likes"),
        spark_sum(col("retweet_count")).alias("total_retweets")).orderBy(desc("total_likes")) \
        .limit(n)

    return top_engagement, top_tweet_count


def top_hashtags(df, n=10):

    hashtag_count = df.groupBy("hashtag").count().orderBy(desc("count")).limit(n)

    # Engagement medio per hashtag
    hashtag_engagement = df.groupBy("hashtag") \
        .agg(avg(col("favorite_count")).alias("avg_likes"),
            avg(col("retweet_count")).alias("avg_retweets")).orderBy(desc("avg_likes")).limit(n)

    return hashtag_count, hashtag_engagement