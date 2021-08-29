#!/usr/bin/env python
from kafka import KafkaConsumer
import nltk
import json

nltk.download("stopwords")
nltk.download("punkt")

# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils


# Kafka settings
topic = 'twitter-stream'

def returnText(x):
    try:
        return x['text']
    except:
        return ""

# this will be used to de-serialise the messages
def myJsonReader(m):
    return returnText(json.loads(m))


from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import string
stop_words = set(stopwords.words('english'))

def get_tokens(line):
    tokens = word_tokenize(line)
    # convert to lower case
    tokens = [w.lower() for w in tokens]
    # remove punctuation from each word
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in tokens]
    # remove remaining tokens that are not alphabetic
    words = [word for word in stripped if word.isalpha()]
    # filter out stop words
    words = [w for w in words if not w in stop_words]
    return words

if __name__ == '__main__':

    print("consumer started")
    consumer = KafkaConsumer(topic,value_deserializer=myJsonReader)

    for msg in consumer:
        # for printing tweets:
        # print(msg.value)
        print(get_tokens(msg.value))
