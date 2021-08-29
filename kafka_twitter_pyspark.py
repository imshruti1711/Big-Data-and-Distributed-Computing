#!/usr/bin/env python

# based on examples/src/main/python/streaming/kafka_wordcount.py

from __future__ import print_function

import sys
import nltk
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import string

stop_words = set(stopwords.words('english'))
custom_stop_words= set(['https','rt','corona'])

def returnText(x):
    try:
        return x['text']
    except:
        return ""


def get_tokens(line):
    tokens = word_tokenize(line)
    # convert to lower case
    tokens = [w.lower() for w in tokens]
    # remove punctuation from each word
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in tokens]
    # stripped = tokens
    # remove remaining tokens that are not alphabetic
    words = [word for word in stripped if word.isalpha()]
    # filter out stop words
    words = [w for w in words if not w in stop_words]
    #filter custom stopwords 
    words= [w for w in words if not w in custom_stop_words]
    return words


# for debugging:
def testmap(line):
    return word_tokenize(line)

if __name__ == "__main__":

    topic = 'twitter-stream'

    sc = SparkContext()
    ssc = StreamingContext(sc, 1) # 1 second intervals!

    zkQuorum = "localhost:2181"
    kafka_stream = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kafka_stream.map(lambda x: json.loads(x[1])).map(returnText)

    # count the occurance of words in batches of tweets
    counts = lines.flatMap(get_tokens) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts = counts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    counts.pprint()


    ssc.start()
    ssc.awaitTermination()
