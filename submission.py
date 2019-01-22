from src.common_functions import *
import sys
import os


def main():
    """
    Lorem Ipsum
    """
    path = sys.argv[0][:-13]
    subprojects = ["sp1", "sp2", "sp3", "sp4"]
    files_list = os.listdir(os.path.join(path, 'data'))

    for subproject in subprojects:
        wordcount_list = get_word_countlist(subproject, files_list, path)
        output_dictionary = get_top_40(subproject, wordcount_list)
        write_to_json(subproject, path, output_dictionary)


if __name__ == "__main__":
    main()