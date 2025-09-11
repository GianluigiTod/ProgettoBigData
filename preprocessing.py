from pyspark.sql.functions import col, split, explode
from pyspark.sql.types import IntegerType, TimestampType

def preprocessing(df, attributo):

    if attributo == "nome":
        #Per cancellare coloro che non hanno un nome
        df = df.filter(col("screen_name").isNotNull()).filter(col("screen_name") != "NA")
    return df
