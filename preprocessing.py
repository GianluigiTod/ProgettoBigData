from pyspark.sql.functions import col, split, explode
from pyspark.sql.types import IntegerType, TimestampType

#secondo me ha senso fare questa cosa a parte, perché se la mettessi in ogni funzione in cui ce n'è bisogno allora potrebbe ripetersi più volte del previsto
def preprocessing(df, attributo):

    if attributo == "nome":
        #Per cancellare coloro che non hanno un nome
        df = df.filter(col("screen_name").isNotNull()).filter(col("screen_name") != "NA")


    if attributo == "hashtags":
        df = df.filter((col("hashtags").isNotNull()) & (col("hashtags") != "NA"))

    return df