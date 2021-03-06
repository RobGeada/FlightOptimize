#Installation Guide

###Installing PySpark
1. [Download Spark](http://spark.apache.org/downloads.html)
2. Unzip package and place into directory of choice. I choose ~/Library
3. Add the following to your .bash_profile:
```
export SPARK_HOME=<path to directory you created>
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH
```
Make sure the version numbers for py4j are correct for your particular install.
(Above code thanks to [Prasbeesh K](http://blog.prabeeshk.com/blog/2015/04/07/self-contained-pyspark-application/)!)

###Check Other Dependencies
If you're a regular Python user, the other dependencies are very common, but in case you don't have them, the neccesary packages are listed [here](https://github.com/RobGeada/FlightOptimize/blob/master/Dependencies.md).

###Setting up FlightOpt
1. Put FlightGUI.py and the FlightData folder into a single directory
2. Run `python FlightGUI.py`. Make sure your current working directory is the same one that FlightGUI.py is located in! 
