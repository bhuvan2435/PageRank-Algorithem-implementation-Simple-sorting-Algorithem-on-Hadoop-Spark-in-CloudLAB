from hdfs import InsecureClient           // will load our data set
from pyspark.sql import SparkSession     //An entry point to   programming spark. Used to create dataframe, registers the dataas tables, SQL
import sys                              //This provides access to variables used-maintained by the interpreter.

class attempt:                         // Input-ouput-paths-for-file
        def __init__(flag, ipath, opath):
                flag.pathfile_for_i = ipath
                flag.pathfile_for_o = opath

        def sort_file(flag):             //function that sorts the dataset
                spark = SparkSession.builder.appName("Group-2_CS-494_HW1").getOrCreate()
                df_load = spark.read.format("com.databricks.spark.csv").option("header", "true").load(flag.pathfile_for_i)
                df_load = df_load.orderBy(['cca2', 'timestamp'], ascending=[1, 1])
                df_load.write.csv(flag.pathfile_for_o)
                df_load.show()

if len(sys.argv) != 3:  //conditon to check the attributes
        print("Can't print should obtain only 2 attributes: <filepath_for_i> <filepath_for_o>")
flag2 = attempt(sys.argv[1], sys.argv[2])
flag2.sort_file()
