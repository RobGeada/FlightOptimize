import pyspark
import numpy as np
import math
import time
import random
from aptNode import aptNode

spark = pyspark.SparkContext("local[*]")
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
sqlc = SQLContext(spark)

#===========Debugging and I/O Tools==============
def wait(label):
    raw_input("Waiting at label: {}".format(label))

def spacer():
    print "======================"

def reg(rdd,name):
    rdd.registerTempTable(name)

def rPrev(rdd,n):
    for i in rdd.take(n):
        print i
    wait("rPreview")

def iPrev(item):
    print item
    wait("iPreview")

def ps(rdd):
    rdd.printSchema()
    wait("printSchema")

def printNames(list):
    names =[]
    for sc in list:
        name = (getName(sc.ID),(sc.g,sc.h,sc.f),len(getApt(sc.ID)[2]))
        names.append(name)
    iPrev(names)

#==========Import and format data=========== 
routes = sqlc.read.json("routes.json")
airports=sqlc.read.json("airports2.json")

def toIntUDF(string):
    return int(string)

toInt = udf(toIntUDF,IntegerType())
airports=airports.withColumn("ID",toInt("ID")).orderBy("ID",ascending=True).cache()
validIDs=airports.select("ID").collect()


#===========Haversine Distance Calc==========
#find great-circle distance between two airports
def distance(aInfo,bInfo):
    if (aInfo == bInfo):
        return 0
    else:
        #define trig variables
        R = 6371000
        phi1,phi2 = math.radians(float(aInfo[0])),math.radians(float(bInfo[0]))
        lam1,lam2=math.radians(float(aInfo[1])),math.radians(float(bInfo[1]))
        delPhi=phi2-phi1
        delLam=lam2-lam1
    
        #calculate haversine
        a = math.sin(delPhi/2)**2 + (math.cos(phi1) * math.cos(phi2) * math.sin(delLam/2)**2)
        c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))
        return (R*c)/1000

#==========Creating Directional Graph===========
#initialize nodes
dictAir ={}
for i in airports.select("Name","ID","Lat","Long").orderBy("ID").collect():
    if (i[1]!=None):
        dictAir["Airport{}".format(i[1])]= (i[0],(i[2],i[3]),[])

#define dictionary helper functions 
def getApt(ID):
    return dictAir["Airport{}".format(ID)]
def getName(ID):
    return dictAir["Airport{}".format(ID)][0]
def getCoords(ID):
    return dictAir["Airport{}".format(ID)][1]

#make mapping
for i in routes.select("Source ID","Dest ID").collect():
    if (i[0]!='\\N' and i[1]!='\\N'):
        sourceCNX = getApt(i[0])[2]
        sourceCNX.append(int(i[1]))

#========A* Graph Search Implementation=======
def closestRoute(A,B):
    #initializations
    aCoords = getCoords(A)
    bCoords = getCoords(B)
    
    #closed is list of explored nodes
    closedID=[]
    
    #unexpl is list of 'frontier' nodes
    unexpl=[aptNode(A,aCoords,0,0,0)]
    unexplID=[A]
    
    result = None
    while (unexplID and result == None):

        #find unexplored with minimum f, call it q
        q = unexpl[0]
        for i in unexpl:
            if i.f < q.f:
                q = i

        #see if q is destinaton
        if (q.ID == B):
            result = q
            break

        #add q to closed list, remove it from unexpl list
        unexpl.remove(q)
        unexplID.remove(q.ID)
        closedID.append(q.ID)

        #establish successors of q
        for i in getApt(q.ID)[2]:
            
            #if in closed list, ignore
            if (i in closedID):
                break
            
            #not in open, add it
            elif (i not in unexplID):
                sc = aptNode(i,getCoords(i),0,0,0)
                sc.path = q.path + [q.ID]
                sc.g = q.g + distance(q.pos,sc.pos)
                sc.h = distance(sc.pos,bCoords)
                sc.setf()
                unexplID.append(i)
                unexpl.append(sc)
            
            #if in open, see if this path is better, else ignore
            else:
                g = q.g + distance(q.pos, getCoords(i))
                better = [val for val in unexpl if i == val.ID and g < val.g]
                if better:
                    #iPrev("Successor {} has a better path: {} vs {}".format(getName(i),g,better[0].g))
                    better[0].path = q.path + [q.ID]
                    better[0].g = g
                    better[0].setf()

    #process results
    if (result == None):
        return None
    else:
        return result.path + [result.ID]

#=========Define network analysis helper functions=======

#select random, valid airport
def randPlaces():
    A = 2585
    length = len(validIDs)-1
    while (getApt(A)[2]==[]):
        i = random.randint(0,length)
        A = validIDs[i].ID
    return A

#from trip data, extract all airport trios
def extractTrios(list):
    last = len(list) - 1
    trios=[]
    n = 0
    while (n+2 <= last):
        trios.append((list[n],list[n+1],list[n+2]))
        n+=1
    return trios


#=========Begin network testing and analysis==========

#initializations
n = 0
totals = []
trips = []
start1 = time.time()

#analyze n trips
while n<1000:
    A = randPlaces()
    B = randPlaces()
    
    #this is where the magic happens
    start = time.time()    
    outPath = closestRoute(A,B)
    finish= time.time()
    
    #if there is no connection, skip
    if (outPath != None):
        print("Test {}".format(n))
        totals.append(finish-start)
        trips.append(outPath)
        n+=1

finish1 = time.time()

#===========Process cumulative trip data==============

#filter out directly connected airports
indirect= [x for x in trips if len(x) > 2]

#extract trios from trip data
trios = map(extractTrios,indirect)
trios = [i for sublist in trios for i in sublist] 
triosRDD = spark.parallelize(trios).map(lambda x: Row(Trio=x))
triosDF = sqlc.createDataFrame(triosRDD,["Trio"])
triosDF = triosDF.groupBy("Trio").count().orderBy('count',ascending=False)
mostCommon = triosDF.first()

#=========Trio analysis function====================
def tripOpt(trio):
    A,B,C = trio[0][0],trio[0][1],trio[0][2]
    ACoords,BCoords,CCoords = getCoords(A),getCoords(B),getCoords(C)
    ABC = distance(ACoords,BCoords) + distance(BCoords,CCoords)
    AC  = distance(ACoords,CCoords)
    saved = (ABC - AC)*trio[1]
    results = 'Most common trio: {} -> {} -> {} (Occurs {} times).\nBy connecting {} to {}, {} km of total travel is eliminated, or {} km per trip\nThis calculation took {} seconds and considers {} trips.'.format(getName(A),getName(B),getName(C),trio[1],getName(A),getName(C),saved,saved/trio[1],finish1-start1,n)
    iPrev(results)

#==========Print final results=============
tripOpt(mostCommon)
    

