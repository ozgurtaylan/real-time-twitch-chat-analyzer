from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql.functions import expr, from_json, concat , explode, split, col, udf, when , count , countDistinct
from pyspark.sql import Window
import time
import json
import nltk
import string
import re
nltk.download('stopwords')
nltk.download('punkt')

# Data preprocessing

# remove whitespace from text
def remove_whitespace(text):
    return  " ".join(text.split())

# lowercase
def text_lowercase(text):
    return text.lower()

# remove numbers
def remove_numbers(text):
    result = re.sub(r'\d+', '', text)
    return result

# remove punctuation
def remove_punctuation(text):
    translator = str.maketrans('', '', string.punctuation)
    return text.translate(translator)

# remove stopwords function(returns list as an output)
def remove_stopwords(text):
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    stop_words = set(stopwords.words("english"))
    word_tokens = word_tokenize(text)
    filtered_text_list = [word for word in word_tokens if word not in stop_words]
    return filtered_text_list

def preprocess_text(text):
    rmv_whtspace = remove_whitespace(text)
    lowercased_txt = text_lowercase(rmv_whtspace)
    rmv_punct = remove_punctuation(lowercased_txt)
    rmv_nmbr = remove_numbers(rmv_punct)
    rmv_stopword_list = remove_stopwords(rmv_nmbr)
    final_txt = ' '.join(rmv_stopword_list)
    return final_txt

preprocessUDF = udf(lambda z:preprocess_text(z),StringType())

def sentiment_udf(sentence):
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    sid_obj = SentimentIntensityAnalyzer()
    sentiment_dict = sid_obj.polarity_scores(sentence)
    if sentiment_dict['compound'] >= 0.05 :
        return "Positive"
    elif sentiment_dict['compound'] <= - 0.05 :
        return "Negative"
    else :
        return "Neutral"
    
sentimentUDF = udf(lambda z:sentiment_udf(z),StringType())  

spark = SparkSession.builder \
        .appName("MyApp") \
        .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:29092") \
  .option("subscribe", "twitch-message") \
  .option("startingOffsets", "earliest") \
  .load()

spark.sparkContext.setLogLevel("WARN")
string_df = df.selectExpr("CAST(value AS STRING)")

schema = StructType([
        StructField("streamer", StringType(), True),
        StructField("order", IntegerType(), True),
        StructField("time" , StringType(), True),
        StructField("username" , StringType(), True),
        StructField("message" , StringType(), True),
        ])
person_df = string_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

df = person_df.na.drop()
preprocessed_df = df.select(col("streamer"),col("order"),col("time"),col("username"),preprocessUDF(col("message")).alias("message"))
blank_removed_df = preprocessed_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in preprocessed_df.columns]).na.drop()
sentiment_df = blank_removed_df.withColumn("sentiment", sentimentUDF(col("message")))

query = sentiment_df \
    .writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", "twitch") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()

query.awaitTermination()
