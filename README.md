# Project 0: Natural Language Processing using Apache Spark

This project is a starter tutorial to **Spark**, implementing Word Count, TF_IDF on a set of 8 documents taken from [Project Guterberg](https://www.gutenberg.org/) using Spark's Python API.

The files include:
  * _Pride and Prejudice_, by Jane Austen 
  * _Alice’s Adventures in Wonderland_, by Lewis Carroll
  * _Ulysses_, by James Joyce
  * _Leviathan_, by Thomas Hobbes
  * _The Iliad_, by Homer
  * _The War of the Worlds_, by H.G. Wells 
  * _The Republic_, by Plato
  * _Little Women_, by Louisa May Alcott

The tasks implemented in this project include:
  1. Word Count
  2. Removing stopwords as given in 'stopwords.txt'
  3. Removing the punctuations (“.”, “,”, “:”, “;”, “;”, “!”, “?”) from the front and back of each of the words
  4. Applying TF-IDF
  
  ## Getting Started
  These instructions describe the prerequisites and steps to get the project up and running.
  
  ### Prerequisites
  This project uses [Apache Spark 2.3.2](https://spark.apache.org/releases/spark-release-2-3-2.html). You will need to have it installed in your machine to be able to run the code. Setting the environment variables right is one of the bigger challenges one could face while installing Spark. Here is a list of user environment variables one needs to set in order to run Spark smoothly.
  * `HADOOP_HOME = /path/to/unzipped/spark/directory`
  * `SPARK_HOME = /path/to/unzipped/spark/directory`
  
  One should also add the following path to user path variables:
  * `/path/to/unzipped/spark/directory/bin`
  
  Furthermore, few of the commands used in the code are specific to Python 3 (I use [Python 3.6.7](https://www.python.org/downloads/release/python-367/)), and thus, you need to have a newer (3.0+) version of Python installed.
  
  Java 8 or 8+ needs to be pre-installed in the machine.
    
  ### Usage
  To run the code and generate output sp*.json files in the `/outputs` directory, run the command:
  
  `spark-submit /path/to/project/submission.py`
  
  ### Output
  Upon running the command in the ‘Usage’ section, the code will run for about 4 minutes and the sp*.json files will be generated in the `/outputs` directory.
  
  The Autolab submission works for me, and gives scores of 98.5, 100, 96.5, 100 for each of sp1.json, sp2.json, sp3.json, sp4.json

## Contributors
* In the current project, [Aashish Yadavally](https://github.com/aashishyadavally) was the only contributor. See [Contributors](https://github.com/dsp-uga/aashishyadavally-p0/blob/master/CONTRIBUTORS.md) file for more details.
## Authors
* [Aashish Yadavally](https://github.com/aashishyadavally)

## License
This project is licensed under the **MIT License**. See [LICENSE](https://github.com/dsp-uga/aashishyadavally-p0/blob/master/LICENSE) for more details.
