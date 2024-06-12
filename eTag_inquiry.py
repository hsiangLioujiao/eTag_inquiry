import datetime
import streamlit as st
import pandas as pd
import requests
import gzip


# 交通部高速公路局交通資料庫
# https://tisvcloud.freeway.gov.tw/
# 整理歷史資料庫資料(從str處理)
def data_ETag(data):
    f=data.split('\n')

    list_ETagGantryID=[]
    list_LinkID=[]
    list_LocationType=[]
    list_PositionLon=[]
    list_PositionLat=[]
    list_RoadID=[]
    list_RoadName=[]
    list_RoadClass=[]
    list_RoadDirection=[]
    list_Start=[]
    list_End=[]
    list_LocationMile=[]

    for line in f:
        if "<ETagGantryID>" in line:
            list_ETagGantryID.append(line.split('>')[1].split('<')[0])
        if "<LinkID>" in line:
            list_LinkID.append(line.split('>')[1].split('<')[0])
        if "<LocationType>" in line:
            list_LocationType.append(line.split('>')[1].split('<')[0])
        if "<PositionLon>" in line:
            list_PositionLon.append(line.split('>')[1].split('<')[0])
        if "<PositionLat>" in line:
            list_PositionLat.append(line.split('>')[1].split('<')[0])             
        if "<RoadID>" in line:
            list_RoadID.append(line.split('>')[1].split('<')[0])             
        if "<RoadName>" in line:
            list_RoadName.append(line.split('>')[1].split('<')[0])             
        if "<RoadClass>" in line:
            list_RoadClass.append(line.split('>')[1].split('<')[0])
        if "<RoadDirection>" in line:
            list_RoadDirection.append(line.split('>')[1].split('<')[0])
        if "<Start>" in line:
            list_Start.append(line.split('>')[1].split('<')[0])
        if "<End>" in line:
            list_End.append(line.split('>')[1].split('<')[0])
        if "<LocationMile>" in line:
            list_LocationMile.append(line.split('>')[1].split('<')[0])

    df_ETag=pd.DataFrame({"ETagGantryID":list_ETagGantryID,
                          "LinkID":list_LinkID,
                          "LocationType":list_LocationType,
                          "PositionLon":list_PositionLon,
                          "PositionLat":list_PositionLat,
                          "RoadID":list_RoadID,
                          "RoadName":list_RoadName,
                          "RoadClass":list_RoadClass,
                          "RoadDirection":list_RoadDirection,
                          "Start":list_Start,
                          "End":list_End,
                          "LocationMile":list_LocationMile})
    df_ETag.to_csv("ETag.csv", index=False)
    return df_ETag


def data_ETagPairLive(data):
    f=data.split('\n')

    X_ETagPairID=''
    X_StartETagStatus=''
    X_EndETagStatus=''
    list_ETagPairID=[]
    list_StartETagStatus=[]
    list_EndETagStatus=[]
    list_VehicleType=[]
    list_TravelTime=[]
    list_StandardDeviation=[]
    list_SpaceMeanSpeed=[]
    list_VehicleCount=[]
    list_StartTime=[]
    list_EndTime=[]
    list_DataCollectTime=[]

    for line in f:
        if "<ETagPairID>" in line:
            X_ETagPairID=line.split('>')[1].split('<')[0]
        if "<StartETagStatus>" in line:
            X_StartETagStatus=line.split('>')[1].split('<')[0]
        if "<EndETagStatus>" in line:
            X_EndETagStatus=line.split('>')[1].split('<')[0]
        if "<VehicleType>" in line:
            list_ETagPairID.append(X_ETagPairID)
            list_StartETagStatus.append(X_StartETagStatus)            
            list_EndETagStatus.append(X_EndETagStatus)            
            list_VehicleType.append(line.split('>')[1].split('<')[0])
        if "<TravelTime>" in line:
            list_TravelTime.append(line.split('>')[1].split('<')[0])
        if "<StandardDeviation>" in line:
            list_StandardDeviation.append(line.split('>')[1].split('<')[0])
        if "<SpaceMeanSpeed>" in line:
            list_SpaceMeanSpeed.append(line.split('>')[1].split('<')[0])
        if "<VehicleCount>" in line:
            list_VehicleCount.append(line.split('>')[1].split('<')[0])
        if "<StartTime>" in line:
            for i in range(5):
                list_StartTime.append(line.split('>')[1].split('<')[0])
        if "<EndTime>" in line:
            for i in range(5):
                list_EndTime.append(line.split('>')[1].split('<')[0])
        if "<DataCollectTime>" in line:
            for i in range(5):
                list_DataCollectTime.append(line.split('>')[1].split('<')[0])

    df_ETagPairLive=pd.DataFrame({"ETagPairID":list_ETagPairID,
                                  "StartETagStatus":list_StartETagStatus,
                                  "EndETagStatus":list_EndETagStatus,
                                  "VehicleType":list_VehicleType,
                                  "TravelTime":list_TravelTime,
                                  "StandardDeviation":list_StandardDeviation,                              
                                  "SpaceMeanSpeed":list_SpaceMeanSpeed,                              
                                  "VehicleCount":list_VehicleCount,                              
                                  "StartTime":list_StartTime,
                                  "EndTime":list_EndTime,
                                  "DataCollectTime":list_DataCollectTime})
    df_ETagPairLive.to_csv("ETagPairLive.csv", index=False)
    return df_ETagPairLive


def time_0_5(time_input):
    x = int(time_input[-1])
    if x>=5:
        x=5
    else:
        x=0
    return time_input[0:3]+str(x)


def data_http(date_input, time_input, road_input, roadDir_input, km_input):
    http_ETag = "https://tisvcloud.freeway.gov.tw/history/motc20/ETag/" + date_input + "/ETag_0000.xml.gz"
    r_ETag = requests.get(http_ETag, timeout=60)
    if r_ETag.status_code != 200:
        print("請求失敗...")
        print(r_ETag.status_code)
        print()
    else:
        data = gzip.decompress(r_ETag.content).decode("utf-8")
        print("get ETag!")
        df_ETag = data_ETag(data)
        df_ETag.loc[:,"GantryMile"]=df_ETag["LocationMile"].apply(lambda x: int(x.split("K+")[0])+0.001*int(x.split("K+")[1]))
        print(len(df_ETag))
        print()
 
    
    http_ETagPairLive = "https://tisvcloud.freeway.gov.tw/history/motc20/ETag/" + date_input + "/ETagPairLive_" + time_input + ".xml.gz"
    r_ETagPairLive = requests.get(http_ETagPairLive, timeout=60)
    if r_ETagPairLive.status_code != 200:
        print("請求失敗...")
        print(r_ETagPairLive.status_code)     
        print()
    else:
        data = gzip.decompress(r_ETagPairLive.content).decode("utf-8")
        print("get ETagPairLive!")
        df_ETagPairLive = data_ETagPairLive(data)
        print(len(df_ETagPairLive))
        print()


    
    if (r_ETag.status_code==200)&(r_ETagPairLive.status_code==200):
        df_look = df_ETag[(df_ETag['RoadName']==road_input) & (df_ETag['RoadDirection']==roadDir_input)]

        if roadDir_input=="S":
            X = (df_look[df_look["GantryMile"]<=km_input].sort_values("GantryMile", ascending=False).iloc[0]["ETagGantryID"]
                 + "-" +
                 df_look[df_look["GantryMile"]>km_input].sort_values("GantryMile").iloc[0]["ETagGantryID"])
            print(X)
            print()
        elif roadDir_input=="N":
            X = (df_look[df_look["GantryMile"]>km_input].sort_values("GantryMile").iloc[0]["ETagGantryID"]
                 + "-" +
                 df_look[df_look["GantryMile"]<=km_input].sort_values("GantryMile", ascending=False).iloc[0]["ETagGantryID"])
            print(X)
            print()
    
        df_X = df_ETagPairLive[df_ETagPairLive["ETagPairID"]==X]
        print(len(df_X))
        print()
        return df_X, r_ETag.status_code, r_ETagPairLive.status_code


# 主程式
date_input = st.sidebar.date_input("輸入查詢日期", datetime.date(2023, 2, 10))
time_input = st.sidebar.time_input("輸入查詢時間", datetime.time(3, 36))
road_input = st.sidebar.selectbox("輸入查詢國道", ("國道1號", "國道3號"))
roadDir_input = st.sidebar.selectbox("輸入車輛行駛方向", ("南向", "北向"))
km_input = st.sidebar.number_input("輸入查詢位置的國道里程數？(數字)", value=6)

st.subheader(f"{date_input.year}年{date_input.month}月{date_input.day}日{time_input.hour}點{time_input.minute}分{road_input}{roadDir_input}{km_input}公里處")

date_input=f"{str(date_input.year):0>4s}{str(date_input.month):0>2s}{str(date_input.day):0>2s}"
time_input=time_0_5(f"{str(time_input.hour):0>2s}{str(time_input.minute):0>2s}")
if roadDir_input=="南向":
    roadDir_input="S"
else:
    roadDir_input="N"

df_X, r_ETag_code, r_ETagPairLive_code= data_http(date_input, time_input, road_input, roadDir_input, km_input)

if (r_ETag_code==200) & (r_ETagPairLive_code==200):
    df_X['SpaceMeanSpeed']=df_X['SpaceMeanSpeed'].astype(float)
    df_X['VehicleCount']=df_X['VehicleCount'].astype(int)
    df_X["X"]=df_X['SpaceMeanSpeed']*df_X['VehicleCount']
    AllVehicleCount=df_X['VehicleCount'].sum()
    MaxSpeed=df_X['SpaceMeanSpeed'].max()
    MeanSpeed=df_X["X"].sum()/AllVehicleCount
    
    df_X = df_X[['VehicleType', 'SpaceMeanSpeed', 'VehicleCount']]
    dict_VehicleType={'31':"小客車", '32':"小貨車", '41':"大客車", '42':"大貨車", '5':"聯結車"}
    df_X['VehicleType']=df_X['VehicleType'].map(dict_VehicleType)
    df_X.columns=["車輛種類", "平均車速[km/h]", "車流量[輛]"]

    st.table(df_X)
    st.write(f"該時段總車流量{AllVehicleCount}輛, 最大平均車速{MaxSpeed:.1f}公里、加權平均車速{MeanSpeed:.1f}公里")
