from sparknlp.base import DocumentAssembler
from sparknlp.annotator import Tokenizer, BertEmbeddings, ClassifierDLModel
from sparknlp.pretrained import PretrainedPipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import BooleanType, StringType
from pyspark.ml import Pipeline
import sparknlp

# ----------------------------
# 1. Crea SparkSession
# ----------------------------
'''
spark = SparkSession.builder \
    .appName("TwitterClassifier") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.0.0") \
    .config("spark.driver.memory","4G") \
    .config("spark.executor.memory","4G") \
    .getOrCreate()
    '''

# Poi avvia Spark NLP senza argomenti
sparknlp.start()

# ----------------------------
# 2. Carica il modello preaddestrato DistilBERT per bias politico
# ----------------------------
# Nota: 'sameer35/distilbert_political_bias' è il modello dal Models Hub
classifier = ClassifierDLModel.pretrained("distilbert_political_bias", "en") \
    .setInputCols(["document", "token"]) \
    .setOutputCol("political_prediction")

# ----------------------------
# 3. Costruisci la pipeline Spark NLP
# ----------------------------
document_assembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

pipeline = Pipeline(stages=[document_assembler, tokenizer, classifier])

# ----------------------------
# 4. Funzione di utilità per predire se il tweet è politico
# ----------------------------
def is_political(text: str, threshold: float = 0.5) -> bool:
    """
    Restituisce True se il testo è politico, False altrimenti.
    
    Parameters:
    - text: stringa del tweet
    - threshold: probabilità minima per considerare il tweet politico
    
    Il modello restituisce label: 'left', 'right', 'neutral'.
    Consideriamo politico se label è 'left' o 'right' e probabilità > threshold.
    """
    # Crea DataFrame temporaneo con una sola riga
    df = spark.createDataFrame([(text,)], ["text"])
    
    # Applica la pipeline
    result_df = pipeline.fit(df).transform(df)
    
    # Estrai label e probabilità
    row = result_df.select("political_prediction.result", "political_prediction.metadata").collect()[0]
    label = row["result"][0]  # 'left', 'right', 'neutral'
    metadata = row["metadata"][0]  # contiene probabilità come stringa
    prob = float(metadata.split(",")[0].split(":")[1])  # estrae probabilità
    
    return label in ["left", "right"] and prob >= threshold

# ----------------------------
# 5. Esempio d'uso
# ----------------------------
if __name__ == "__main__":
    test_texts = [
        "The new healthcare bill is a disaster for our country!",
        "Just had a great pizza in NYC",
        "I support the new environmental policy passed by Congress"
    ]
    
    for text in test_texts:
        print(f"Text: {text}\nIs political? {is_political(text)}\n")
