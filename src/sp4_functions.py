from pyspark import SparkContext
import math


def get_word_dictionary(wordcountlist):
    """
    Returns a list containing (word, word_count) tuples of all the words across the 8 documents, which is further
    used for computing the Inverse Document Frequency (IDF).
    :param wordcountlist:
    :return word_dictionary:
    """
    word_dictionary = []
    for i in range(len(wordcountlist)):
        word_dictionary.extend(wordcountlist[i])
    return word_dictionary


def get_idf(wordcountlist):
    """
    Computes Inverse Document Frequency (IDF) using combineByKey() method, which returns RDD containing
    (word, (a, b)), where 'a' is the total word count across all documents and 'b' is the number of documents
    the word appears in. By using the formula log(N / n_t), we compute IDF for all the words in word_dictionary.
    :param wordcountlist:
    :return idf:
    """
    # Creating existing SparkContext, or creating one if none is present
    sc1 = SparkContext.getOrCreate()
    data = sc1.parallelize(get_word_dictionary(wordcountlist)) \
        .combineByKey(lambda value: (value, 1),
                      lambda x, value: (x[0] + value, x[1] + 1),
                      lambda x, y: (x[0] + y[0], x[1] + y[1]))
    # Computed IDF is stored as dictionary
    idf = data.map(lambda x: (x[0], math.log(8 / x[1][1]))).collectAsMap()
    return idf
