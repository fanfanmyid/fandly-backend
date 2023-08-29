from doctest import Example
from fastapi import FastAPI,Body
from fastapi.middleware.cors import CORSMiddleware
from deta import Deta
import pandas as pd
from typing import Optional
import json
import math
import datetime
from datetime import timedelta
from urllib.request import urlopen
import pandas as pd
import pytz
from cryptomodule import unix2utc,getDataCrypto,utcToUnix
from identify_candlestick import recognize_candlestick

import crypto_module as cm

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

candle_names = ['CDL2CROWS',
 'CDL3BLACKCROWS',
 'CDL3INSIDE',
 'CDL3LINESTRIKE',
 'CDL3OUTSIDE',
 'CDL3STARSINSOUTH',
 'CDL3WHITESOLDIERS',
 'CDLABANDONEDBABY',
 'CDLADVANCEBLOCK',
 'CDLBELTHOLD',
 'CDLBREAKAWAY',
 'CDLCLOSINGMARUBOZU',
 'CDLCONCEALBABYSWALL',
 'CDLCOUNTERATTACK',
 'CDLDARKCLOUDCOVER',
 'CDLDOJI',
 'CDLDOJISTAR',
 'CDLDRAGONFLYDOJI',
 'CDLENGULFING',
 'CDLEVENINGDOJISTAR',
 'CDLEVENINGSTAR',
 'CDLGAPSIDESIDEWHITE',
 'CDLGRAVESTONEDOJI',
 'CDLHAMMER',
 'CDLHANGINGMAN',
 'CDLHARAMI',
 'CDLHARAMICROSS',
 'CDLHIGHWAVE',
 'CDLHIKKAKE',
 'CDLHIKKAKEMOD',
 'CDLHOMINGPIGEON',
 'CDLIDENTICAL3CROWS',
 'CDLINNECK',
 'CDLINVERTEDHAMMER',
 'CDLKICKING',
 'CDLKICKINGBYLENGTH',
 'CDLLADDERBOTTOM',
 'CDLLONGLEGGEDDOJI',
 'CDLLONGLINE',
 'CDLMARUBOZU',
 'CDLMATCHINGLOW',
 'CDLMATHOLD',
 'CDLMORNINGDOJISTAR',
 'CDLMORNINGSTAR',
 'CDLONNECK',
 'CDLPIERCING',
 'CDLRICKSHAWMAN',
 'CDLRISEFALL3METHODS',
 'CDLSEPARATINGLINES',
 'CDLSHOOTINGSTAR',
 'CDLSHORTLINE',
 'CDLSPINNINGTOP',
 'CDLSTICKSANDWICH',
 'CDLTAKURI',
 'CDLTASUKIGAP',
 'CDLTHRUSTING',
 'CDLTRISTAR',
 'CDLUNIQUE3RIVER',
 'CDLUPSIDEGAP2CROWS',
 'CDLXSIDEGAP3METHODS']

def utcToUnix(timeData):
    timeData = datetime.datetime.timestamp(timeData)*1000
    timeData = math.trunc(timeData)
    return timeData

deta = Deta("qzcL9FRu_SuXXrzGZ8Xh4v4EKa5WqBjD8T1QqkgAx")

def unix2utc(timestamp):
    your_dt = datetime.datetime.fromtimestamp(int(timestamp)/1000)  # using the local timezone
    utc_dt= your_dt.astimezone(pytz.UTC)
    return(utc_dt.strftime("%Y-%m-%d %H:%M:%S"))  # 2018-04-07 20:48:08, YMMV


@app.get("/")
async def root():
    return {"test" :"OK","docs" :"https://fanfan-api.deta.dev/docs"}

@app.get("/bnb")
async def bnb(timeframe:str,limit:Optional[str] = None):

    tf = "bnb"+ timeframe + "_db"
    tf2 = "bnb"+ timeframe + "_gru_db"
    bnb = deta.Base(tf)
    bnb_gru = deta.Base(tf2)
    # datetimenow_first= datetime.datetime(2022, 8, 31, 0, 0, 0) + timedelta(minutes=1) # format (yyyy, mm, dd, hh, mm, ss)
    if(timeframe == '1m'):
        datetimenow = datetime.datetime.now() - timedelta(hours=10)
        print(datetimenow)
        # datetimenow = datetimenow_first - timedelta(hours=10)
    else:
        datetimenow = datetime.datetime.now() - timedelta(hours=20)
        # datetimenow = datetimenow_first - timedelta(hours=10)
        
    datetimenow1 = datetime.datetime.now()+timedelta(minutes=20)
    # datetimenow1 = datetimenow_first +timedelta(minutes=20)
    
    if(timeframe == '1m'):
        res = bnb.fetch(query=[{"key?r": [str(utcToUnix(datetimenow)),str(utcToUnix(datetimenow1))]}],limit=1000)
        res_gru = bnb_gru.fetch(query=[{"key?r": [str(utcToUnix(datetimenow)),str(utcToUnix(datetimenow1))]}],limit=1000)
        # res = bnb.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
        # res_gru = bnb_gru.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
    else:
        res = bnb.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
        res_gru = bnb_gru.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
    
    all_item = res.items
    all_item_gru = res_gru.items
    # while res.last:
    #     res = bnb.fetch(last=res.last)
    #     all_item+=res.items
        
    # while res_gru.last:
    #     res_gru = bnb_gru.fetch(last=res_gru.last)
    #     all_item_gru+=res_gru.items
        
    data = pd.DataFrame(all_item)
    data_gru = pd.DataFrame(all_item_gru)
    
    data['time'] = data['time'].transform(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    data_gru['time'] = data_gru['time'].transform(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    
    data.rename(columns ={'close_predict':'close_predict_lstm'}, inplace = True)
    data_gru.rename(columns ={'close_predict':'close_predict_gru'}, inplace = True)
    
    data =data[['time','close_predict_lstm']]
    data_gru =data_gru[['time','close_predict_gru']]
    
    data = data.sort_values(by="time")
    data['time'] = data['time'].transform(lambda x: str(x))
    data = data.reset_index()
        # data = data.set_index('time')
    data = data.iloc[:,1:]
    
    data_gru = data_gru.sort_values(by="time")
    data_gru['time'] = data_gru['time'].transform(lambda x: str(x))
    data_gru = data_gru.reset_index()
        # data = data.set_index('time')
    data_gru = data_gru.iloc[:,1:]
    
    if(limit is None):
        data = data.tail(6)
        data_gru = data_gru.tail(6)
    elif(int(limit) < 1000):
        data = data.tail(int(limit))
        data_gru = data_gru.tail(int(limit))
    
    data = pd.merge(data,data_gru, on='time',how='outer') 
    currency = "BNBBUSD"
    interval = "1m"
    len_data = "1000"  
    start_time = data.head(1)
    start_time = start_time['time'].values[0]
    print(start_time)
    start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(hours=8)
    start_time_dt = str(utcToUnix(start_time_dt))
    df_scrap = cm.getDataCrypto(currency,interval,len_data,start_time_dt,None)
    df_scrap = df_scrap[['time','close']]
    df_scrap = pd.DataFrame(df_scrap)
    df_scrap['time'] = df_scrap['time'].transform(lambda x: unix2utc(x))
    df_scrap['close'] = df_scrap['close'].transform(lambda x: round(x,2))
    new_data = pd.merge(data,df_scrap, on='time',how='left')
    new_data = new_data.fillna('TBD')
    new_data['close'] = new_data['close'].transform(lambda x: str(x))
    json2data = json.loads(new_data.to_json(orient='records'))
    json2scrap = json.loads(df_scrap.to_json(orient='records'))
    # json2data = json.loads(data.to_json(orient='records'))
    return { "data": json2data}

    
@app.get("/eth")
async def eth(timeframe:str,limit:Optional[str] = None):
    tf = "eth"+ timeframe + "_db"
    tf2 = "eth"+ timeframe + "_gru_db"
    bnb = deta.Base(tf)
    bnb_gru = deta.Base(tf2)
    
    if(timeframe == '1m'):
        datetimenow = datetime.datetime.now() - timedelta(hours=10)
    else:
        datetimenow = datetime.datetime.now() - timedelta(hours=20)
        
    datetimenow1 = datetime.datetime.now()+timedelta(minutes=20)
    
    if(timeframe == '1m'):
        res = bnb.fetch(query=[{"key?r": [str(utcToUnix(datetimenow)),str(utcToUnix(datetimenow1))]}],limit=1000)
        res_gru = bnb_gru.fetch(query=[{"key?r": [str(utcToUnix(datetimenow)),str(utcToUnix(datetimenow1))]}],limit=1000)
        # res = bnb.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
        # res_gru = bnb_gru.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
    else:
        res = bnb.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
        res_gru = bnb_gru.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
    
    all_item = res.items
    all_item_gru = res_gru.items
    # while res.last:
    #     res = bnb.fetch(last=res.last)
    #     all_item+=res.items
        
    # while res_gru.last:
    #     res_gru = bnb_gru.fetch(last=res_gru.last)
    #     all_item_gru+=res_gru.items
        
    data = pd.DataFrame(all_item)
    data_gru = pd.DataFrame(all_item_gru)
    
    data['time'] = data['time'].transform(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    data_gru['time'] = data_gru['time'].transform(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    
    data.rename(columns ={'close_predict':'close_predict_lstm'}, inplace = True)
    data_gru.rename(columns ={'close_predict':'close_predict_gru'}, inplace = True)
    
    data =data[['time','close_predict_lstm']]
    data_gru =data_gru[['time','close_predict_gru']]
    
    data = data.sort_values(by="time")
    data['time'] = data['time'].transform(lambda x: str(x))
    data = data.reset_index()
        # data = data.set_index('time')
    data = data.iloc[:,1:]
    
    data_gru = data_gru.sort_values(by="time")
    data_gru['time'] = data_gru['time'].transform(lambda x: str(x))
    data_gru = data_gru.reset_index()
        # data = data.set_index('time')
    data_gru = data_gru.iloc[:,1:]
    
    if(limit is None):
        data = data.tail(6)
        data_gru = data_gru.tail(6)
    elif(int(limit) < 1000):
        data = data.tail(int(limit))
        data_gru = data_gru.tail(int(limit))
    
    data = pd.merge(data,data_gru, on='time',how='outer') 
    currency = "ETHBUSD"
    interval = "1m"
    len_data = "1000"  
    start_time = data.head(1)
    start_time = start_time['time'].values[0]
    print(start_time)
    start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(hours=8)
    start_time_dt = str(utcToUnix(start_time_dt))
    df_scrap = cm.getDataCrypto(currency,interval,len_data,start_time_dt,None)
    df_scrap = df_scrap[['time','close']]
    df_scrap = pd.DataFrame(df_scrap)
    df_scrap['time'] = df_scrap['time'].transform(lambda x: unix2utc(x))
    df_scrap['close'] = df_scrap['close'].transform(lambda x: round(x,2))
    new_data = pd.merge(data,df_scrap, on='time',how='left')
    new_data = new_data.fillna('TBD')
    new_data['close'] = new_data['close'].transform(lambda x: str(x))
    
    df_candlepattern = cm.getDataCrypto(currency,timeframe,len_data,start_time_dt,start_time_dt)
    df_candlepattern['time'] = df_candlepattern['time'].transform(lambda x: unix2utc(x))
    df_candlepattern = df_candlepattern.tail(50)
    candle_pattern = recognize_candlestick(df_candlepattern)
    # candle_pattern = candle_pattern.tail(6)
    
    new_data_cdl = pd.merge(new_data,candle_pattern[['time','candlestick_pattern','candlestick_match_count']], on='time',how='outer')
    
    json2data = json.loads(new_data.to_json(orient='records'))
    json2scrap = json.loads(df_scrap.to_json(orient='records'))
    json2candle = json.loads(candle_pattern.to_json(orient='records'))
    json2newdata = json.loads(new_data_cdl.to_json(orient='records'))
    # json2data = json.loads(data.to_json(orient='records'))
    return { "data": json2data,
            "data_candle" : json2candle
            }
        
   
        
    
@app.get("/btc")
async def btc(timeframe:str,limit:Optional[str] = None):
    tf = "btc"+ timeframe + "_db"
    tf2 = "btc"+ timeframe + "_gru_db"
    bnb = deta.Base(tf)
    bnb_gru = deta.Base(tf2)
    
    if(timeframe == '1m'):
        datetimenow = datetime.datetime.now() - timedelta(hours=10)
    else:
        datetimenow = datetime.datetime.now() - timedelta(hours=20)
        
    datetimenow1 = datetime.datetime.now()+timedelta(minutes=20)
    
    if(timeframe == '1m'):
        res = bnb.fetch(query=[{"key?r": [str(utcToUnix(datetimenow)),str(utcToUnix(datetimenow1))]}],limit=1000)
        res_gru = bnb_gru.fetch(query=[{"key?r": [str(utcToUnix(datetimenow)),str(utcToUnix(datetimenow1))]}],limit=1000)
        # res = bnb.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
        # res_gru = bnb_gru.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
    else:
        res = bnb.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
        res_gru = bnb_gru.fetch(query=[{"time?r": [str(datetimenow),str(datetimenow1)]}],limit=1000)
    
    all_item = res.items
    all_item_gru = res_gru.items
    # while res.last:
    #     res = bnb.fetch(last=res.last)
    #     all_item+=res.items
        
    # while res_gru.last:
    #     res_gru = bnb_gru.fetch(last=res_gru.last)
    #     all_item_gru+=res_gru.items
        
    data = pd.DataFrame(all_item)
    data_gru = pd.DataFrame(all_item_gru)
    
    data['time'] = data['time'].transform(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    data_gru['time'] = data_gru['time'].transform(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    
    data.rename(columns ={'close_predict':'close_predict_lstm'}, inplace = True)
    data_gru.rename(columns ={'close_predict':'close_predict_gru'}, inplace = True)
    
    data =data[['time','close_predict_lstm']]
    data_gru =data_gru[['time','close_predict_gru']]
    
    data = data.sort_values(by="time")
    data['time'] = data['time'].transform(lambda x: str(x))
    data = data.reset_index()
        # data = data.set_index('time')
    data = data.iloc[:,1:]
    
    data_gru = data_gru.sort_values(by="time")
    data_gru['time'] = data_gru['time'].transform(lambda x: str(x))
    data_gru = data_gru.reset_index()
        # data = data.set_index('time')
    data_gru = data_gru.iloc[:,1:]
    
    if(limit is None):
        data = data.tail(6)
        data_gru = data_gru.tail(6)
    elif(int(limit) < 1000):
        data = data.tail(int(limit))
        data_gru = data_gru.tail(int(limit))
    
    data = pd.merge(data,data_gru, on='time',how='outer') 
    currency = "BTCBUSD"
    interval = "1m"
    len_data = "1000"  
    start_time = data.head(1)
    start_time = start_time['time'].values[0]
    print(start_time)
    start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(hours=8)
    start_time_dt = str(utcToUnix(start_time_dt))
    df_scrap = cm.getDataCrypto(currency,interval,len_data,start_time_dt,None)
    df_scrap = df_scrap[['time','close']]
    df_scrap = pd.DataFrame(df_scrap)
    df_scrap['time'] = df_scrap['time'].transform(lambda x: unix2utc(x))
    df_scrap['close'] = df_scrap['close'].transform(lambda x: round(x,2))
    new_data = pd.merge(data,df_scrap, on='time',how='left')
    new_data = new_data.fillna('TBD')
    new_data['close'] = new_data['close'].transform(lambda x: str(x))
    json2data = json.loads(new_data.to_json(orient='records'))
    json2scrap = json.loads(df_scrap.to_json(orient='records'))
    # json2data = json.loads(data.to_json(orient='records'))
    return { "data": json2data}
  

@app.get("/testing")
async def testing():
    currency = "BTCBUSD"
    interval = "1m"
    len_data = "1"  
    start_time = "2022-07-20 00:41:00"
    print(start_time)
    start_time_dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(hours=8)
    start_time_dt = str(utcToUnix(start_time_dt))
    # df_scrap  = getDataCrypto(currency,interval,len_data,start_time_dt)
    df_scrap = cm.getDataCrypto(currency,interval,len_data,start_time_dt,None)
    print(df_scrap)
    df_scrap = df_scrap[['time','close']]
    json2scrap = json.loads(df_scrap.to_json(orient='records'))
    return {"scrap":json2scrap}
