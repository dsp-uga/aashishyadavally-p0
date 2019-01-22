# aashishyadavally-p0
## Project 0: Natural Language Processing using Apache Spark

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
  
  ## Usage
  To run the code and generate output sp*.json files in the `/outputs` directory, run the command:
  `spark-submit /path-to-project/submission.py`
  
  ## Output
  Upon running the command in the ‘Usage’ section, the code will run for about 4 minutes and the sp*.json files will be generated in the `/outputs` directory.
  
  The Autolab submission works for me, and gives scores of 98.5, 100, 96.5, 100 for each of sp1.json, sp2.json, sp3.json, sp4.json
