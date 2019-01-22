import os
import json
from pyspark import SparkContext
from src.sp4_functions import *


def get_word_countlist(subproject, fileslist, path):
    """
    Returns respective lists containing word counts for each of the subprojects sp*, filtered with their
    corresponding pre-processing steps
    :param subproject:
    :param fileslist:
    :param path:
    :return wordcountlist:
    """
    wordcountlist = []
    stopwords = sc.textFile(os.path.join(path, 'stopwords.txt')).collect()
    # List of punctuations that need to be stripped from the words in the documents
    punctuations = (".", ",", ":", ";", "!", "?", "'")

    if subproject == "sp1":
        for file in fileslist:
            text_file = sc.textFile(os.path.join(path, 'data', file))
            # Splits the word, converts each word into lowercase, computes word-count by grouping all tuples with
            # same key, i.e, tuples with same words together
            wordcount = text_file.flatMap(lambda x: x.lower().split()) \
                .map(lambda x: (x, 1))\
                .reduceByKey(lambda a, b: a + b)
            wordcountlist.extend(wordcount.collect())

    elif subproject == "sp2":
        for file in fileslist:
            text_file = sc.textFile(os.path.join(path, 'data', file))
            # Splits the word, converts each word into lowercase, computes word-count by grouping all tuples with
            # same key, i.e, tuples with same words together. Furthermore, all words which do not appear in the
            # stopwords list are filtered out of the RDD
            wordcount = text_file.flatMap(lambda x: x.lower().split()) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .filter(lambda x: x[0] not in stopwords)
            wordcountlist.extend(wordcount.collect())

    elif subproject == "sp3":
        for file in fileslist:
            text_file = sc.textFile(os.path.join(path, 'data', file))
            # Splits the word, converts each word into lower case. Then, words with one character are filtered
            # out, words which start with punctuations are filtered out, words which end with punctuations
            # are filtered out, words which are not in the list of stopwords are filtered out. Finally, the word
            # count is then calculated on this RDD
            wordcount = text_file.flatMap(lambda x: x.lower().split()) \
                .filter(lambda x: len(x) > 1) \
                .filter(lambda x: not (x.startswith(punctuations))) \
                .filter(lambda x: not (x.endswith(punctuations))) \
                .filter(lambda x: x not in stopwords) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b)
            wordcountlist.extend(wordcount.collect())

    elif subproject == "sp4":
        for file in fileslist:
            text_file = sc.textFile(os.path.join(path, 'data', file))
            # Splits the word, converts each word into lower case. Then, words with one character are filtered
            # out. Furthermore, one punctuation is stripped off the words from the front and the end, for
            # whichever have them, and word count is calculated
            wordcount = text_file.flatMap(lambda x: x.lower().split()) \
                .filter(lambda x: len(x) > 1) \
                .map(lambda x: x[1:] if x.startswith(punctuations) else x) \
                .map(lambda x: x[:-1] if x.endswith(punctuations) else x) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b)
            wordcountlist.append(wordcount.collect())
    return wordcountlist


def get_top_40(subproject, wordcountlist):
    """
    Returns a dictionary containing the Top-40 words, and their corresponding word-counts/
    TF-IDF weights after the text pre-processing, for each of the subprojects
    :param subproject:
    :param wordcountlist:
    :return output:
    """
    if subproject != "sp4":
        # Words which have a word count <2 are filtered out, and the top 40 words, retrieved by sorting the
        # dictionary in descending order based on word count, is returned as a dictionary
        data = sc.parallelize(wordcountlist) \
            .filter(lambda x: x[1] >= 2) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda x: x[1], ascending=False)
        output = dict(data.take(40))
        return output
    else:
        output = []
        idf = get_idf(wordcountlist)
        # Rhe top 40 words, retrieved by sorting the dictionary in descending order based on TF-IDF values
        # is returned as a dictionary
        for i in range(len(wordcountlist)):
            top5_tf_idf = sc.parallelize(wordcountlist[i]) \
                .map(lambda x: (x[0], x[1] * idf[x[0]])) \
                .sortBy(lambda x: x[1], ascending=False) \
                .take(5)
            output.extend(top5_tf_idf)
        return dict(output)


def write_to_json(subproject, path, output):
    """Writes the output dictionaries to corresponding sp*.json files in the outputs directory"""
    if os.path.isdir(os.path.join(path, 'outputs')):
        with open(os.path.join(path, 'outputs', subproject + '.json'), 'w') as f:
            json.dump(output, f)
    else:
        os.mkdir(os.path.join(path, 'outputs'))
        with open(os.path.join(path, 'outputs', subproject + '.json'), 'w') as f:
            json.dump(output, f)

    f.close()
    print(subproject + ".json file successfully created.")
    return 1


sc = SparkContext()
