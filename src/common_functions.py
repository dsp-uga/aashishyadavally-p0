import os
import json
from pyspark import SparkContext
from src.sp4_functions import *


def get_word_countlist(subproject, fileslist, path):
    """
    Returns respective lists containing word counts for each of the subprojects sp1,
    sp2, sp3 and sp4; filtered with their corresponding pre-processing steps
    :param subproject:
    :param fileslist:
    :param path:
    :return wordcountlist:
    """
    wordcountlist = []
    stopwords = sc.textFile(os.path.join(path, 'stopwords.txt')).collect()
    punctuations = (".", ",", ":", ";", "!", "?", "'")
    if subproject == "sp1":
        for file in fileslist:
            text_file = sc.textFile(os.path.join(path, 'data', file))
            wordcount = text_file.flatMap(lambda x: x.lower().split())\
                .map(lambda x: (x, 1))\
                .reduceByKey(lambda a, b: a + b)
            wordcountlist.extend(wordcount.collect())

    elif subproject == "sp2":
        for file in fileslist:
            text_file = sc.textFile(os.path.join(path, 'data', file))
            wordcount = text_file.flatMap(lambda x: x.lower().split()) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .filter(lambda x: x[0] not in stopwords)
            wordcountlist.extend(wordcount.collect())

    elif subproject == "sp3":
        for file in fileslist:
            text_file = sc.textFile(os.path.join(path, 'data', file))
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
        data = sc.parallelize(wordcountlist) \
            .filter(lambda x: x[1] >= 2) \
            .reduceByKey(lambda a, b: a + b) \
            .sortBy(lambda x: x[1], ascending=False)
        output = dict(data.take(40))
        return output
    else:
        output = []
        idf = get_idf(wordcountlist)
        for i in range(len(wordcountlist)):
            top5_tf_idf = sc.parallelize(wordcountlist[i]) \
                .map(lambda x: (x[0], x[1] * idf[x[0]])) \
                .sortBy(lambda x: x[1], ascending=False) \
                .take(5)
            output.extend(top5_tf_idf)
        return dict(output)


def write_to_json(subproject, path, output):
    """Writes the output dictionaries to corresponding sp*.json files"""
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
