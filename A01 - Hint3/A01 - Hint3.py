# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs

#-------------------------------------------
#FUNCTION data_split
#-------------------------------------------
def data_split(n)
  splitData = n.split(" ")
  dataValues = splitData[0]+"\t("+splitData[1]+", "+splitData[2]+")"
  
  return dataValues

#-------------------------------------------
#FUNCTION sort_language
#-------------------------------------------

def sort_language(n)
  split = n.split(" ")
  splitLang = split[0]
  
  return splitLang

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)
    
    readFile = sc.textFile(dataset_dir)
    readData = readFile(data_split)
    lang = readData.groupBy(sort_language)
    

	# Complete the Spark Job

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
