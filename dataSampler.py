from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
import ast
from ast import literal_eval
import datetime
import statistics
from boto3.dynamodb.conditions import Key, Attr
import psycopg2 as pg
#from sampleFunctions import nSampler
#################
# Global Soils Postgres Connection
conn = pg.connect(database="global_soils", user="", password="", host="", port="5432")
cur = conn.cursor()
#################
# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

#connect to dynamo table w flattened sensor data
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('teralytic-sensor-readings-prod')
current_time = datetime.datetime.now(datetime.timezone.utc)
unix_timestamp = current_time.timestamp()
#take off decimal when passing in unix timestamp
currentTime = int(unix_timestamp)
#1440 minutes in a day * 60 seconds.
days = 14
timeWindow = 1440 * days
past = int(unix_timestamp - (timeWindow*60))
reservedKey = {"#rw": "raw", }
numeric_types  = [int, float, complex]
depthList =  ["in6", "in18", "in36"]
# add devices
# deviceName  = event['deviceName']
# Co-Op Farmers Elevator
deviceList  = ['02373DF', 
               '0239B81', 
               '023812D']

####FUNCTIONS########

def nSampler(depth,device):
    try:
        nSample = table.query(
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="n."+depth+".#rw",
        ExpressionAttributeNames=reservedKey,
        KeyConditionExpression=Key('deviceName').eq(device) & Key('epoch').between(labTime, futureTime)
        )

        nList = []
        for i in nSample[u'Items']:
            nList.append(json.dumps(i, cls=DecimalEncoder))
        #print(nList)
        # remove null types in Dynamo
        nListAppend = []
        for i in range(len(nList)):
            if not 'nul' in nList[i]:
                nListAppend.append(nList[i])
        #print(nListAppend)
        # convert string to Dictionary using AST literal
        nCluster = []
        for i in range(len(nListAppend)):
            nCluster.append(ast.literal_eval(nListAppend[i]))
        #print(nCluster)
        #convert Dic to simple list
        nFormat = []
        for dic in nCluster:
            for val in dic.values():
                nFormat.append(val[depth])
        nFinal = [f['raw'] for f in nFormat]
        # remove outlies from list
        nFinal = list(filter(lambda a: a!= 0 and a!= 3300, nFinal))
        #print(nFinal)
        try:
            nAvg = statistics.mean(nFinal)
            nStDev = statistics.stdev(nFinal)
            # filter out readings more or less than 3 SDs from mean
            #print("N Raw StDev" + " " + str(depth) + " : " + str(nStDev))
            for i in nFinal:
                if nAvg > i +  (3*nStDev):
                    nFinal.remove(i)
                elif nAvg < i - (3*nStDev):
                    nFinal.remove(i)
            # recalculate mean
            nAvg = statistics.mean(nFinal)
        except:
            nAvg = None
        #print("N Avg: " + str(nAvg))
        #rawSampleList.append(nAvg)
        #print(rawSampleList)
        return nAvg
    except:
        print("Error sampling raw N data")
#############################################################################
def moistSampler(depth,device):
    try:
        moistSample = table.query(
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="moisture."+depth+".converted",
        KeyConditionExpression=Key('deviceName').eq(device) & Key('epoch').between(labTime, futureTime)
        )

        moistList = []
        for i in moistSample[u'Items']:
            moistList.append(json.dumps(i, cls=DecimalEncoder))
        #print(moistList)
        # remove null types in Dynamo
        moistListAppend = []
        for i in range(len(moistList)):
            if not 'nul' in moistList[i]:
                moistListAppend.append(moistList[i])
        #print(moistListAppend)
        # convert string to Dictionary using AST literal
        moistCluster = []
        for i in range(len(moistListAppend)):
            moistCluster.append(ast.literal_eval(moistListAppend[i]))
        #print(moistCluster)
        #convert Dic to simple list
        moistFormat = []
        for dic in moistCluster:
            for val in dic.values():
                moistFormat.append(val[depth])
        moistFinal = [f['converted'] for f in moistFormat]
        # remove outlies from list
        moistFinal = list(filter(lambda a: not a < 0 and a!= 1, moistFinal))
        #print(moistFinal)
        try:
            moistAvg = statistics.mean(moistFinal)
            moistStDev = statistics.stdev(moistFinal)
            # filter out readings more or less than 3 SDs from mean
            #print("Moist StDev" + " " + str(depth) + " : " + str(moistStDev))
            for i in moistFinal:
                if moistAvg > i +  (3*moistStDev):
                    moistFinal.remove(i)
                elif moistAvg < i - (3*moistStDev):
                    moistFinal.remove(i)
            # recalculate mean
            moistAvg = statistics.mean(moistFinal)
        except:
            moistAvg = None
        #print("N Avg: " + str(nAvg))
        #rawSampleList.append(nAvg)
        #print(rawSampleList)
        return moistAvg
    except:
        print("Error sampling VWC data")
##############################################################################
def tempSampler(depth,device):
    try:
        tempSample = table.query(
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="soilTemp."+depth+".converted",
        KeyConditionExpression=Key('deviceName').eq(device) & Key('epoch').between(labTime, futureTime)
        )

        tempList = []
        for i in tempSample[u'Items']:
            tempList.append(json.dumps(i, cls=DecimalEncoder))
        #print(tempList)
        # remove null types in Dynamo
        tempListAppend = []
        for i in range(len(tempList)):
            if not 'nul' in tempList[i]:
                tempListAppend.append(tempList[i])
        #print(tempListAppend)
        # convert string to Dictionary using AST literal
        tempCluster = []
        for i in range(len(tempListAppend)):
            tempCluster.append(ast.literal_eval(tempListAppend[i]))
        #print(tempCluster)
        #convert Dic to simple list
        tempFormat = []
        for dic in tempCluster:
            for val in dic.values():
                tempFormat.append(val[depth])
        tempFinal = [f['converted'] for f in tempFormat]
        # remove outlies from list
        tempFinal = list(filter(lambda a: a!= -100 and a!= 100, tempFinal))
        #print(tempFinal)
        try:
            tempAvg = statistics.mean(tempFinal)
            tempStDev = statistics.stdev(tempFinal)
            # filter out readings more or less than 3 SDs from mean
            #print("Temp StDev" + " " + str(depth) + " : " + str(tempStDev))
            for i in tempFinal:
                if tempAvg > i +  (3*tempStDev):
                    tempFinal.remove(i)
                elif tempAvg < i - (3*tempStDev):
                    tempFinal.remove(i)
            # recalculate mean
            tempAvg = statistics.mean(tempFinal)
        except:
            tempAvg = None
        return tempAvg
    except:
        print("Error sampling SoilTemp data")
#################################################################
def o2Sampler(depth,device):
    try:
        o2Sample = table.query(
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="o2."+depth+".converted",
        KeyConditionExpression=Key('deviceName').eq(device) & Key('epoch').between(labTime, futureTime)
        )

        o2List = []
        for i in o2Sample[u'Items']:
            o2List.append(json.dumps(i, cls=DecimalEncoder))
        #print(o2List)
        # remove null types in Dynamo
        o2ListAppend = []
        for i in range(len(o2List)):
            if not 'nul' in o2List[i]:
                o2ListAppend.append(o2List[i])
        #print(o2ListAppend)
        # convert string to Dictionary using AST literal
        o2Cluster = []
        for i in range(len(o2ListAppend)):
            o2Cluster.append(ast.literal_eval(o2ListAppend[i]))
        #print(o2Cluster)
        #convert Dic to simple list
        o2Format = []
        for dic in o2Cluster:
            for val in dic.values():
                o2Format.append(val[depth])
        o2Final = [f['converted'] for f in o2Format]
        # remove outlies from list
        o2Final = list(filter(lambda a: not a < 0, o2Final))
        #print(o2Final)
        try:
            o2Avg = statistics.mean(o2Final)
            o2StDev = statistics.stdev(o2Final)
            # filter out readings more or less than 3 SDs from mean
            #print("Temp StDev" + " " + str(depth) + " : " + str(tempStDev))
            for i in o2Final:
                if o2Avg > i +  (3*o2StDev):
                    o2Final.remove(i)
                elif o2Avg < i - (3*o2StDev):
                    o2Final.remove(i)
            # recalculate mean
            o2Avg = statistics.mean(o2Final)
        except:
            o2Avg = None
        return o2Avg
    except:
        print("Error sampling o2 data")
############################################################################
def etoSampler(device):
    try:
        etoSample = table.query(
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="ETo.ETo",
        KeyConditionExpression=Key('deviceName').eq(device) & Key('epoch').between(labTime, futureTime)
        )

        etoList = []
        for i in etoSample[u'Items']:
            etoList.append(json.dumps(i, cls=DecimalEncoder))
        #print(etoList)
        # remove null types in Dynamo
        etoListAppend = []
        for i in range(len(etoList)):
            if not 'nul' in etoList[i]:
                etoListAppend.append(etoList[i])
        #print(etoListAppend)
        # convert string to Dictionary using AST literal
        etoCluster = []
        for i in range(len(etoListAppend)):
            etoCluster.append(ast.literal_eval(etoListAppend[i]))
        #print(etoCluster)
        #convert Dic to simple list
        etoFormat = []
        for dic in etoCluster:
            for val in dic.values():
                etoFormat.append(val["ETo"])
        #print(etoFormat)
        # remove outlies from list
        etoFinal = list(filter(lambda a: not a < 0, etoFormat))
        #print(etoFinal)
        try:
            etoAvg= statistics.mean(etoFinal)
        except:
            etoAvg = None
        return etoAvg
    except:
        print("Error sampling eto data")

########################################################################
def etcSampler(device):
    try:
        etcSample = table.query(
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="ETo.ETc",
        KeyConditionExpression=Key('deviceName').eq(device) & Key('epoch').between(labTime, futureTime)
        )

        etcList = []
        for i in etcSample[u'Items']:
            etcList.append(json.dumps(i, cls=DecimalEncoder))
        #print(etcList)
        # remove null types in Dynamo
        etcListAppend = []
        for i in range(len(etcList)):
            if not 'nul' in etcList[i]:
                etcListAppend.append(etcList[i])
        #print(etcListAppend)
        # convert string to Dictionary using AST literal
        etcCluster = []
        for i in range(len(etcListAppend)):
            etcCluster.append(ast.literal_eval(etcListAppend[i]))
        #print(etcCluster)
        #convert Dic to simple list
        etcFormat = []
        for dic in etcCluster:
            for val in dic.values():
                etcFormat.append(val["ETc"])
        #print(etcFormat)
        # remove outlies from list
        etcFinal = list(filter(lambda a: not a < 0, etcFormat))
        #print(etcFinal)
        try:
            etcAvg= statistics.mean(etcFinal)
        except:
            etcAvg = None
        return etcAvg
    except:
        print("Error sampling etc data")

###########################################################################
def rainSampler(device):
    try:
        rainSample = table.query(
        ReturnConsumedCapacity='TOTAL',
        ProjectionExpression="weatherData.precipIntensity",
        KeyConditionExpression=Key('deviceName').eq(device) & Key('epoch').between(labTime, futureTime)
        )

        rainList = []
        for i in rainSample[u'Items']:
            rainList.append(json.dumps(i, cls=DecimalEncoder))
        #print(rainList)
        # remove null types in Dynamo
        rainListAppend = []
        for i in range(len(rainList)):
            if not 'nul' in rainList[i]:
                rainListAppend.append(rainList[i])
        #print(rainListAppend)
        # convert string to Dictionary using AST literal
        rainCluster = []
        for i in range(len(rainListAppend)):
            rainCluster.append(ast.literal_eval(rainListAppend[i]))
        #print(rainCluster)
        #convert Dic to simple list
        rainFormat = []
        for dic in rainCluster:
            for val in dic.values():
                rainFormat.append(val["precipIntensity"])
        #print(etcFormat)
        # remove outlies from list
        rainFinal = list(filter(lambda a: not a < 0, rainFormat))
        #print(rainFinal)
        try:
            rainAvg= statistics.mean(rainFinal)
        except:
            rainAvg = None
        return rainAvg
    except:
        print("Error sampling rain data")

##########################################################################

for device in deviceList:
    print("Device: "  + str(device))
    values = {'device': device.lower()}
	# find lab timestamp for each device
    SQL = """
    SELECT lab_date,om6,om18,om36,clay6,clay18,clay36,
	salts6, salts18, salts36, ph6, ph18, ph36
	FROM groundtruth WHERE sensor ~ %(device)s
    """
    cur.execute(SQL, values)
    results = cur.fetchall()
    #print(results)
    try:
        labTime = results[0][0]
        # 2 weeks in future from sample time
        futureTime = labTime + (days*1440*60)
    except:
        labTime = past
        futureTime = currentTime
    try:
        om6 = results[0][1]
    except:
        om6 = None
    try:
        om18 = results[0][2]
    except:
        om18 = None
    try:
        om36 = results[0][3]
    except:
        om36 = None
    try:
        clay6 = results[0][4]
    except:
        clay6 = None
    try:
        clay18 = results[0][5]
    except:
        clay18 = None
    try:
        clay36 = results[0][6]
    except:
        clay36 = None
    try:
        salts6 = results[0][7]
    except:
        salts6 = None
    try:
        salts18 = results[0][8]
    except:
        salts18 = None
    try:
        salts36 = results[0][9]
    except:
        salts36 = None
    try:
        ph6 = results[0][10]
    except:
        ph6 = None
    try:
        ph18 = results[0][11]
    except:
        ph18 = None
    
    try:
        ph36 = results[0][12]
    except:
        ph36 = None
    
    ##########################################
    print("Lab Sample Time: " + str(labTime))
    print("Future Time: " + str(futureTime))
    print("OM6: " + str(om6))
    print("OM18: " + str(om18))
    print("OM36: " + str(om36))
    print("Clay6: " + str(clay6))
    print("Clay18: " + str(clay18))
    print("Clay36: " + str(clay36))
    print("Salts6: " +  str(salts6))
    print("Salts18: " + str(salts18))
    print("Salts36: " + str(salts36))
    print("pH6: " + str(ph6))
    print("pH18: " + str(ph18))
    print("pH36: " + str(ph36))
    nRaw6 = round(nSampler(depthList[0],device),5)
    print("nRaw6: " + str(nRaw6))
    nRaw18 = round(nSampler(depthList[1],device),5)
    print("nRaw18: " + str(nRaw18))
    nRaw36 = round(nSampler(depthList[2],device),5)
    print("nRaw36: " + str(nRaw36))
    VWC6 = round(moistSampler(depthList[0],device),5)
    print("VWC6: "  + str(VWC6))
    VWC18 = round(moistSampler(depthList[1],device),5)
    print("VWC18: " + str(VWC18))
    VWC36 = round(moistSampler(depthList[2],device),5)
    print("VWC36: " + str(VWC36))
    soilTemp6 = round(tempSampler(depthList[0],device),5)
    print("SoilTemp6: " + str(soilTemp6))
    soilTemp18 = round(tempSampler(depthList[1],device),5)
    print("SoilTemp18: "  + str(soilTemp18))
    soilTemp36 = round(tempSampler(depthList[2],device),5)
    print("SoilTemp36: "  + str(soilTemp36))
    o2 = round(o2Sampler(depthList[1],device),5)
    print("o2: " +  str(o2))
    eto = round(etoSampler(device),5)
    print("ETo: " + str(eto))
    etc = round(etcSampler(device),5)
    print("ETc: " + str(etc))
    rain = round(rainSampler(device),5)
    print("PrecipIntensity: " + str(rain))
    print("################################")
    ####NOW ADD Data as rows in Postgres
    SQL = """
    CREATE TABLE IF NOT EXISTS random_forest_n
    (device text,
    sample_time double precision,
    update_time double precision,
    n_raw_6 double precision,
    n_raw_18 double precision,
    n_raw_36 double precision,
    vwc_6 double precision,
    vwc_18 double precision,
    vwc_36 double precision,
    temp_6 double precision,
    temp_18 double precision,
    temp_36 double precision,
    o2 double precision,
    eto double precision,
    etc double precision,
    precip double precision
    );
    """
    cur.execute(SQL)
    conn.commit()
    # now upload data 
    if cur:
        values = {
            'device': device,
            'sample_time': labTime,
            'update_time': currentTime,
            'n_raw_6': nRaw6,
            'n_raw_18': nRaw18,
            'n_raw_36': nRaw36,
            'vwc_6': VWC6,
            'vwc_18': VWC18,
            'vwc_36': VWC36,
            'temp_6': soilTemp6,
            'temp_18': soilTemp18,
            'temp_36': soilTemp36,
            'o2': o2,
            'eto': eto,
            'etc': etc,
            'precip': rain
        }
        sql = """
        INSERT INTO random_forest_n (device, sample_time, update_time, n_raw_6, n_raw_18, n_raw_36, 
        vwc_6, vwc_18, vwc_36, temp_6, temp_18, temp_36, o2, eto, etc, precip) 
        VALUES (%(device)s, %(sample_time)s, %(update_time)s, 
        %(n_raw_6)s, %(n_raw_18)s, %(n_raw_36)s, %(vwc_6)s, 
        %(vwc_18)s, %(vwc_36)s, %(temp_6)s, %(temp_18)s, 
        %(temp_36)s, %(o2)s, %(eto)s, %(etc)s, %(precip)s
        )
        """
        cur.execute(sql,values)
        conn.commit()
        print("Values added to N Random Forest Dataset")
cur.close()
conn.close()
    
    
