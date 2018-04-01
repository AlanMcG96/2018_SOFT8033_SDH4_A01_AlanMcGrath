





# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs
from heapq import nlargest
from operator import itemgetter



# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    sortWords = []
    i = 0
    print("Sorting",end=" ")
    for line in input_stream:
        word_list = line.split(" ")
        if((word_list[0][:2] == languages[0]) or (word_list[0][:2] == languages[1]) or (word_list[0][:2] == languages[2])):
            sortWords.append( [word_list[0], word_list[1], int(word_list[2])] )
            sortWords.sort
    print(" Done!")
    
    dict = {}
    for words in sortWords:
        if words[0] in dict:
            dict[words[0]].append([words[1],words[2]])
        else:
            dict[words[0]] = [[words[1],words[2]]]
            
    for i in dict:
#        dict[i].sort(key=lambda x: x[1], reverse=True)
#        dict[i] = dict[i][0:5]
#        output_stream.write(i +"\t" + dict[i][0][0] + "," + str(dict[i][0][1]) + "\n")
        for index in range(0,5):
            if len(dict[i]) > index:
                dict[i].sort(key=lambda x: x[1], reverse=True)
                dict[i] = dict[i][0:5]
                output_stream.write(i + "\t" + dict[i][index][0] + "," + str(dict[i][index][1]) + "\n")
    
    
    pass  
    
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    i_file_name = "pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
