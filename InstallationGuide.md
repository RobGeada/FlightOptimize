#Installation Guide

###Installing PySpark
1. Download Spark at http://spark.apache.org/downloads.html
2. Unzip package and place into directory of choice. I choose ~/Library
3. Add the following to your .bash_profile:
'''
export SPARK_HOME=~/Library/spark
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.8-src.zip:$PYTHONPATH
'''
(Above code thanks to Prasbeesh K. at http://blog.prabeeshk.com)
