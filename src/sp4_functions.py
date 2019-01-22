from pyspark import SparkContext
import math


def get_word_dictionary(wordcountlist):
    """
    Returns a list containing all the words across the 8 documents which is
    further used for computing the Inverse Document Frequency (IDF)
    :param wordcountlist:
    :return word_dictionary:
    """
    word_dictionary = []
    for i in range(len(wordcountlist)):
        word_dictionary.extend(wordcountlist[i])
    return word_dictionary


def get_idf(wordcountlist):
    """
    Computes Inverse Document Frequency (IDF) from the second parameter in the
    tuple associated with a particular key, both of which are returned by
    using the combineByKey() method on the RDD

    :param wordcountlist:
    :return idf:
    """
    sc1 = SparkContext.getOrCreate()
    data = sc1.parallelize(get_word_dictionary(wordcountlist)) \
        .combineByKey(lambda value: (value, 1),
                      lambda x, value: (x[0] + value, x[1] + 1),
                      lambda x, y: (x[0] + y[0], x[1] + y[1]))
    idf = data.map(lambda x: (x[0], math.log(8 / x[1][1]))).collectAsMap()
    return idf