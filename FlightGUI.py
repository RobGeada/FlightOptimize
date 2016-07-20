import pyspark
import numpy as np
import math
import time
import random
import sys,os,os.path
import matplotlib.pyplot as plt

spark = pyspark.SparkContext("local[*]")
spark.setLogLevel("OFF")
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import udf,sum,min,mean
from pyspark.sql.types import IntegerType,FloatType
sqlc = SQLContext(spark)

#===========Debugging and I/O Tools==============
def t():
    return time.time()

def wait(label):
    raw_input("Waiting at label: {}".format(label))

def spacer():
    print "======================"

def reg(rdd,name):
    rdd.registerTempTable(name)

def rPrev(rdd,n,timing):
    for i in rdd.take(n):
        print i
    if not timing:
        wait("rPreview")

def iPrev(item):
    print item
    wait("iPreview")

def ps(rdd):
    rdd.printSchema()
    wait("printSchema")

def printNames(list):
    for sc in list:
        print "{}. F: {} G:{} H: {}".format(sc['geoInfo'],sc["f"],sc['g'],sc["h"])
    wait("names")

#==========Import and format data=========== 
#import data

def beginSetup():
    global routes
    global coords
    global itens
    cwd = os.getcwd()
    rtPath = cwd+"/FlightOptResults.parquet"
    if os.path.isdir(cwd+"/FlightOptRoutes.parquet"):
        firstRoutes = False
        routes = sqlc.read.parquet("FlightOptRoutes.parquet")
    else:
        firstRoutes = True
        routes = sqlc.read.json("USNetwork2.json")
 
    if os.path.isdir(cwd+"/FlightOptItens.parquet"):
        firstItens = False
        itens = sqlc.read.parquet("FlightOptRoutes.parquet")
    else:
        firstItens = True
        itens  = sqlc.read.json("Itenaries.json").select("ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID","PASSENGERS","MARKET_MILES_FLOWN")

    coords = sqlc.read.json("Coords.json").select("AIRPORT_ID","AIRPORT","LATITUDE","LONGITUDE")
    return firstRoutes and firstItens 
    

#define formatting functions
def toIntUDF(string):
    return int(float(string))
toInt = udf(toIntUDF,IntegerType())

def toKmUDF(n):
    return float(n)*1.60934
toKm = udf(toKmUDF,FloatType())

def timeNormalizeUDF(dept,time,passengers):
    d = float(dept)
    t = float(time)
    if (d == 0 or t == 0 or passengers == 0):
        return 9876543.21
    else:
        return t/(d*60)
timeNormalize = udf(timeNormalizeUDF,FloatType())

#format routes data
def formatRoutes():
    global routes
    routes=routes.withColumn("PASSENGERS",toInt("PASSENGERS"))
    routes=routes.withColumn("ORIGIN_AIRPORT_ID",toInt("ORIGIN_AIRPORT_ID"))
    routes=routes.withColumn("DEST_AIRPORT_ID",toInt("DEST_AIRPORT_ID"))
    routes=routes.withColumn("DEPARTURES_PERFORMED",toInt("DEPARTURES_PERFORMED"))
    routes=routes.withColumn("RAMP_TO_RAMP",timeNormalize("DEPARTURES_PERFORMED","RAMP_TO_RAMP","PASSENGERS")).cache()


def formatItens():
    #format itenary data
    global itens
    itens = itens.withColumn("ORIGIN_AIRPORT_ID",toInt("ORIGIN_AIRPORT_ID"))
    itens = itens.withColumn("DEST_AIRPORT_ID",toInt("DEST_AIRPORT_ID"))
    itens = itens.withColumn("MARKET_MILES_FLOWN",toKm("MARKET_MILES_FLOWN"))
    aggArg = sum("PASSENGERS").alias("PASSENGERS"),mean("MARKET_MILES_FLOWN").alias("MARKET_KMS_FLOWN")
    itens = itens.groupBy("ORIGIN_AIRPORT_ID","DEST_AIRPORT_ID").agg(*aggArg).cache()
    
#==========Creating Directional Graph===========

#initialize dictionaries
dictCoords = {}
dictAir ={}
dictCity={}
revAir  ={}
def createDicts():
    for i in coords.collect():
        ID  = i[0]
        name= i[1]
        lati = i[2]
        longi= i[3]
        revAir["AirID{}".format(name)]=ID
        dictCoords["Coords{}".format(ID)]=(lati,longi)

def initNode(ID,geoInfo,depts):
    coords = dictCoords.get("Coords{}".format(ID))
    airFmt = {'ID':ID,'coords':coords,'geoInfo':geoInfo,'cnx':[],'f':0,'g':0,'h':0,'open':0,'closed':0,'path':[],'depts':depts}
    dictAir["Airport{}".format(ID)] = airFmt
   
def initCity(ID,geoInfo):
    dictCity["City{}".format(ID)] = {'geoInfo':geoInfo,'flux':0,'net':0}

def modNode(ID,g,path):
    currNode = dictAir.get("Airport{}".format(ID))
    currNode['g']=g
    currNode['f']=g+currNode['h']
    currNode['path']=path

def resetNode(node):
    node['f']=0
    node['g']=0
    node['h']=0
    node['path']=[]
    node['open']=0
    node['closed']=0

#define dictionary helper functions 
def getApt(ID):
    return dictAir["Airport{}".format(ID)]

def getCity(ID):
    return dictCity["City{}".format(ID)]

def getName(ID):
    return dictAir["Airport{}".format(ID)]['geoInfo']

def getID(name):
    return revAir["AirID{}".format(name)]


#==========Make Weighted Mapping====================
#i[0]=Orig ID
#i[1]=Orig City
#i[2]=Orig Code
#i[3]=Dest ID
#i[4]=Dest City
#i[5]=Dest Code
#i[6]=Airline
#i[7]=Passengers
#i[8]=Departures
#i[9]=Ramp to Ramp Time (minutes)

def makeMapping():
    global routes
    grpString = "ORIGIN_AIRPORT_ID","ORIGIN_CITY_NAME","ORIGIN","DEST_AIRPORT_ID","DEST_CITY_NAME","DEST","UNIQUE_CARRIER_NAME"
    routes = routes.groupBy(*grpString).agg(sum("PASSENGERS").alias("PASSENGERS"),sum("DEPARTURES_PERFORMED").alias("DEPARTURES_PERFORMED"),mean("RAMP_TO_RAMP").alias("RAMP_TO_RAMP"))
    for i in routes.collect():
        if not dictAir.get("Airport{}".format(i[0])):
            initNode(i[0],(i[1],i[2]),i[8])
        if not dictAir.get("Airport{}".format(i[3])):
            initNode(i[3],(i[4],i[5]),0)
        if (i[9]!=9876543.21):
            tripTime =i[9]
            getApt(i[0])['depts'] += i[8]
            sourceCNX = getApt(i[0])['cnx']
            sourceCNX.append((int(i[3]),tripTime,i[6]))

#===========Population Flow=================
def createCityFlux():
    cityList = []
    for key,value in dictCity.items():
        cityList.append(Row(geoInfo=value['geoInfo'],flux=value['flux'],net=value['net']))
    cityListRDD = spark.parallelize(cityList)
    cityListDF  = sqlc.createDataFrame(cityListRDD,['flux','geoInfo','net']).orderBy('flux',ascending=False)
    rPrev(cityListDF,1000)

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

def distByID(A,B):
    aCoords = getApt(A)['coords']
    bCoords = getApt(B)['coords']
    return distance(aCoords,bCoords)

#get computed trip distance:
def tripDistance(list):
    dist = 0
    for i in range(0,len(list)-1):
        dist += distByID(list[i],list[i+1])
    return dist

def triDist(list):
    try:
        A = list[0]
    except:
        iPrev(list)
    C = list[-1]
    ABC = tripDistance(list)
    AC= distByID(A,C)
    return ABC-AC

#========A* Graph Search Implementation=======
def closestRoute(A,B,verbose1,verbose2):
    #initializations
    if (A==B):
        return ("Already there!",0)

    aCoords = getApt(A)['coords']
    bCoords = getApt(B)['coords']
      
    #unexpl is list of 'frontier' nodes
    unexpl=[getApt(A)]
    result = []
    start = t()
    dirtied = []
    bestFirst = 1;
    while (unexpl and result == []):
        #find unexplored with minimum f, call it q
        if verbose1:
            printNames(unexpl)
        
        q = unexpl[0]
        if not bestFirst:
            for i in unexpl:
                if i['f'] < q['f']:
                    q = i
        minF = q['f']        
        #see if q is destinaton
        if (q['ID'] == B):
            result = q['path']
            break
               
        if verbose1:
            iPrev("I chose {}".format(getApt(q['ID'])['geoInfo']))
        #add q to closed list, remove it from unexpl list
        unexpl.remove(q)
        bestFirst = 0
        q['closed'] = 1
        dirtied.append(q)

        #establish successors of q
        for i in q['cnx']:
            iID,iTime,iAirline,iNode = i[0],i[1],i[2],getApt(i[0])
            
            #if in closed list, ignore
            if iNode['closed']:
                continue
           
            #not in open, add it
            elif not iNode['open']:
                iNode['path'] = q['path'] + [(q['ID'],iTime,iAirline)]
                iNode['g']    = q['g'] + iTime
                iNode['open'] = 1
                distToGo = distance(iNode['coords'],bCoords)
                if (distToGo < 5):
                    iNode['h']=0
                else:
                    iNode['h']=(distToGo/930)+1.5
                iNode['f'] = iNode['g'] + iNode['h']
                if (iNode['f'] < minF):
                    minF = iNode['f']
                    unexpl.insert(0,iNode)
                    bestFirst = 0
                else:
                    unexpl.append(iNode)
                dirtied.append(iNode)

            #if in open, see if this path is better, else ignore
            else:
                new_g = q['g'] + iTime
                if (new_g < iNode['g']):
                    betterPath = q['path'] + [(q['ID'],iTime,iAirline)]
                    modNode(iID,new_g,betterPath)
                    if iNode['f']< minF:
                        bestFirst = 0
                        minF = iNode['f']
    finish = t()
    cleanUp(dirtied)
    if result == []:
        return ("No path between {} and {}".format(getApt(A)['geoInfo'],getApt(B)['geoInfo']),finish-start)
    else:
        return result+[(B,finish-start)]

def cleanUp(dirtied):
    for i in dirtied:
        resetNode(i)

#turn results of closestRoute into English
def parseResults(raw,outMode):
    if (raw[0][0:2] == "No" or raw[0][0:2] == "Al"):
        if (outMode == "full"):
            return raw[0]
        elif (outMode == "trios"):
            return []
        else:
            return raw[1]
    elif (outMode == "trios"):
        trios = []
        passSum = 0
        for i in raw:
            trios.append(i[0])
        return trios
    else:
        def apt(n):
            targetID = raw[n][0]
            return getApt(targetID)
        n=0
        totalTime = 0
        resultString = "\n=======================\n"
        for n in range(0,len(raw)-1):
            fmtString = apt(n)['geoInfo'],raw[n][2],round(raw[n][1],2),apt(n+1)['geoInfo']
            resultString += "In {}, take {} for {} hours to {}.\n".format(*fmtString)
            totalTime += raw[n][1]
        layovers = len(raw)-2
        totalTime += (layovers * 1.5)
        if layovers == 1:
            resultString += "Trip summary: {} hours with 1 layover.\n".format(round(totalTime,2))
        else:
            resultString += "Trip summary: {} hours with {} layovers.\n".format(round(totalTime,2),layovers)
        resultString += "Result computed in {} seconds.\n".format(raw[n+1][1])
        resultString += "=======================\n"
        if outMode == "full":
            return resultString
        else:
            return raw[n+1][1]

#=========Define network analysis helper functions=======

#select random, valid airport:
def randO():
    i = random.randint(0,validOLen-1)
    return validOrig[i][0]
      
def randD():
    i = random.randint(0,validDLen-1)
    return validDest[i][0]

#from trip data, extract all airport trios:
def extractTrios(list):
    if list == None:
        return None
    last = len(list) - 1
    trios=[]
    n = 0
    while (n+2 <= last):
        trios.append((list[n],list[n+1],list[n+2]))
        n+=1
    return trios

#=========Begin network testing and analysis==========

def singleConnect(A,B):
    return parseResults(closestRoute(A,B,0,0),"full")

#singleConnect(10754,14843)

def timingTests(nMax):
    testStart = t()
    toRoute = []
    for runs in range(0,nMax):
        orig = randO()
        dest = randD()
        toRoute.append((orig,dest)) 
    toRouteRDD = spark.parallelize(toRoute).cache()
    routed = toRouteRDD.map(lambda x: Row(blank=1,time=parseResults(closestRoute(x[0],x[1],0,0),"time")))
    meanT = sqlc.createDataFrame(routed,['blank','time']).groupBy("blank").mean("time").first()[1]
    testTime= t()-testStart
    iPrev("{} trips averaging {} secs. Test took {} secs at {} per trip".format(nMax,meanT,testTime,testTime/nMax))

def trioRandAnalysis(nMax):
    toRoute = []
    for runs in range(0,nMax):
        orig = randO()
        dest = randD()
        toRoute.append((orig,dest)) 
    toRouteRDD = spark.parallelize(toRoute).cache()
    routed = toRouteRDD.map(lambda x: parseResults(closestRoute(x[0],x[1],0,0),"trios"))
    rPrev(routed,100)
    routed = routed.flatMap(lambda x: extractTrios(x)).map(lambda x: (x,1))
    routed = routed.reduceByKey(lambda x,y: x+y)
    routed = routed.map(lambda x: (x[0],x[1],triDist(x[0]*x[1])))
    routedDF=sqlc.createDataFrame(routed,['trio','count','kmSaved']).orderBy('kmSaved',ascending=False)
    rPrev(routedDF,100)
    testTime= t()-testStart

#wait("Im waiting")
def trioAnalysis(nMax):
    routed = itens.map(lambda x: (x[0],x[1],(parseResults(closestRoute(x[0],x[1],0,0),"trios")),x[2],x[3]))
    routed = routed.map(lambda x: (x[0],x[1],x[2],(x[4]-tripDistance(x[2]))*x[3]))
    print("Calculating optimal routes... (this could take a minute, but let's see you try to give directions to 5.8 million people!)")
    schemaString = ['ORIGIN_AIRPORT_ID','Dest','Trip','kmSaved']
    kmSavedDF=sqlc.createDataFrame(routed,schemaString).groupBy("ORIGIN_AIRPORT_ID").agg(sum("kmSaved").alias("kmSaved"))
    print("Looking up airport names...")
    kmSavedDF=kmSavedDF.map(lambda x: Row(Name=getName(x[0]),kmPerDept=x[1]/getApt(x[0])['depts']))
    kmSavedDF=sqlc.createDataFrame(kmSavedDF,['Name','kmPerDept'])
    print("Sorting results...")
    kmSavedDF=kmSavedDF.orderBy("kmPerDept",ascending=False)
    routes.write.parquet("FlightOptRoutes.parquet")
    itens.write.parquet("FlightOptItens.parquet")
    kmSavedDF.write.parquet("FlightOptResults.parquet")

#===============================================
#==========Begin User Interface=================
#===============================================

#========Setup===========
def setup():
    print("\nImporting data...")
    firstTime = beginSetup()
    if firstTime:
        print("Beginning first time setup...")
        print("Formatting routes...")
        formatRoutes()
        print("Formatting iteneraries...")
        formatItens()
        print("Creating dictionaries...")
    else:
        print("Loading data from previous session...")
    createDicts()
    print("Creating node graph... (this may take a few seconds)")
    makeMapping()
    printTitle()
    print("\nSetup Complete!")

#======I/O===========
def printTitle():
    os.system('clear')
    print("===============================================================")
    print("Welcome to")
    print("  ______   _   _           _       _      ____            _    ")
    print(" |  ____| | | (_)         | |     | |    / __ \          | |   ")
    print(" | |__    | |  _    __ _  | |__   | |_  | |  | |  _ __   | |_  ") 
    print(" |  __|   | | | |  / _` | | '_ \  | __| | |  | | | '_ \  | __| ") 
    print(" | |      | | | | | (_| | | | | | | |_  | |__| | | |_) | | |_  ")
    print(" |_|      |_| |_|  \__, | |_| |_|  \__|  \____/  | .__/   \__| ")
    print("                    __/ |                        | |           ")
    print("                   |___/                         |_|           ")
    print("====Version 3.0==============7/19/2016=========================")

printTitle()
#cont = raw_input("\nBegin setup? (y/n) ")
cont = "y"
if cont == "n":
    sys.exit()
else:
    setup()

while 1:
    query = raw_input("\nRoute between cities (r) or analyze network (a)? ")
    if query == "r":
        while cont != "n": 
            printTitle()
            print("\nIndividual Route Testing")
            city1 = raw_input("\nRoute from: ")
            city2 = raw_input("        to: ")
            try:
                A = getID(city1)
                A = int(A)
            except:
                print("{} is not a valid airport!".format(city1))
                cont = raw_input("Test another route? (y/n) ")
                continue
            try:
                B = getID(city2)
                B = int(B)
            except:
                print("{} is not a valid airport!".format(city2))
                cont = raw_input("Test another route? (y/n) ")
                continue
            try:
                print(singleConnect(A,B))
            except:
                print("Invalid airport!")
            cont = raw_input("Test another route? (y/n) ")
        printTitle()
    else:
        cwd=os.getcwd()
        if os.path.isdir(cwd+"/FlightOptResults.parquet"):
            result = sqlc.read.parquet("FlightOptResults.parquet").orderBy("kmPerDept",ascending=False)
        else:
            result = trioAnalysis(10000000)
        printTitle()
        seeMore = raw_input("\nAnalysis Completed! View the ten airports with most room for improvement? (y/n) ")
        print("\nSorting data...")
        if seeMore == "y":
            print("")
            for i in result.take(10):
                print("{} wastes {} kilometers per outbound flight".format(i[0],i[1]))
