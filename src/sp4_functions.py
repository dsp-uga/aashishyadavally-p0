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

