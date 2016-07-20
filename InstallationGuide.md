#Installation Guide

###Installing PySpark
1. Download Spark at http://spark.apache.org/downloads.html
2. Unzip package and place into directory of choice. I choose ~/Library
3. Add the following to your .bash_profile:
```
export SPARK_HOME=<path to directory you created>
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH
```
Make sure the version numbers for py4j are correct for your particular install.
(Above code thanks to Prasbeesh K. at http://blog.prabeeshk.com)

###Setting up FlightOpt
1. Put FlightGUI.py, the three parquet files, and Coords.json into a single directory
2. Run <python FlightGUI.py>
