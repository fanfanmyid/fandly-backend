import pandas as pd
from datetime import datetime
from typing import Optional
import json
import math
import datetime
from datetime import timedelta
from urllib.request import urlopen
import pandas as pd
import pytz

def getDataCrypto(symbol,interval,limit,timeStart):
    
    # store the URL in url as 
    # parameter for urlopen
    url = "https://api.binance.me/api/v3/klines?symbol="+str(symbol)+"&interval="+str(interval)+"&limit="+limit+"&startTime="+str(timeStart)
    
    response = urlopen(url)

    # storing the JSON response 
    # from url in data
    data_json = json.loads(response.read())

    # print the json response
#     print(data_json)
    tm,op,hi,lo,cl,v = [],[],[],[],[],[]

    for x in range(len(data_json)):
    #     print(data_json[x][0])
        tm.append(data_json[x][0])
        op.append(data_json[x][1])
        hi.append(data_json[x][2])
        lo.append(data_json[x][3])
        cl.append(data_json[x][4])
        v.append(data_json[x][5])
    data = {"time": tm, "open":op,"high":hi,"low":lo,"close":cl,"volume":v}
    df = pd.DataFrame(data)
    df['time'] = df['time'].transform(lambda x: unix2utc(x))
    df["open"] = pd.to_numeric(df["open"], downcast="float")
    df["high"] = pd.to_numeric(df["high"], downcast="float")
    df["low"] = pd.to_numeric(df["low"], downcast="float")
    df["close"] = pd.to_numeric(df["close"], downcast="float")
    df["volume"] = pd.to_numeric(df["volume"], downcast="float")
    df["close"].round(decimals=2)
    return df

def utcToUnix(timeData):
    timeData = datetime.datetime.timestamp(timeData)*1000
    timeData = math.trunc(timeData)
#     print(timeData)
    return timeData

def unix2utc(timestamp):
    your_dt = datetime.datetime.fromtimestamp(int(timestamp)/1000)  # using the local timezone
    utc_dt= your_dt.astimezone(pytz.UTC)
    return(utc_dt.strftime("%Y-%m-%d %H:%M:%S"))  # 2018-04-07 20:48:08, YMMV