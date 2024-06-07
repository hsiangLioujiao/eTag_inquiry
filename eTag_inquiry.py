# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 08:58:01 2024

@author: g_s_s
"""

import datetime
import streamlit as st
import pandas as pd
import requests
import gzip

# 整理歷史資料庫資料(從str處理)
def data_ETag(data):
# 交通部高速公路局交通資料庫
# https://tisvcloud.freeway.gov.tw/

# eTag 靜態資訊(ETag.xml)
# https://tisvcloud.freeway.gov.tw/history/motc20/ETag.xml
# ETag資料樣式
    """
          <ETagGantryID>03F2899N</ETagGantryID> # eTag偵測站代碼
          <LinkID>0000300129000Q</LinkID>       # 基礎路段代碼 
          <LocationType>4</LocationType>        # 設置地點位置類型
          <PositionLon>120.48633</PositionLon>  # 設備架設位置 X 坐標
          <PositionLat>23.511683</PositionLat>  # 設備架設位置 Y 坐標
          <RoadID>000030</RoadID>               # 道路代碼
          <RoadName>國道3號</RoadName>           # 道路名稱
          <RoadClass>0</RoadClass>              # 道路分類
          <RoadDirection>N</RoadDirection>      # 基礎路段所屬道路方向
          <RoadSection>
            <Start>竹崎(縣道159線)</Start>       # 路段起點描述
            <End>竹崎(縣道166線)</End>           # 路段迄點描述
          </RoadSection>
          <LocationMile>289K+900</LocationMile> # 所在方向里程數
    """
# 基礎路段代碼表 https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao
# 設置地點位置類型 1: 路側, 2: 道路中央分隔島, 3: 快慢分隔島, 4: 車道上門架, 5: 車道鋪面, 6: 其他
# 路名碼基本資料 https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao
# 道路分類 0: 國道, 1: 快速道路, 2: 市區快速道路, 3: 省道, 4: 縣道, 5: 鄉道, 6: 市區一般道路, 7: 匝道
# 道路方向資料表 https://traffic-api-documentation.gitbook.io/traffic/xiang-dai-zhao-biao
    f=data.split('\n')

    # 取巧方式
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


def data_ETagPair(data):
# eTag配對路徑靜態資訊(ETagPair.xml)
# https://tisvcloud.freeway.gov.tw/history/motc20/ETagPair.xml
# ETagPair資料樣式
    """
    <ETagPairList xmlns="http://traffic.transportdata.tw/standard/traffic/schema/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://traffic.transportdata.tw/standard/traffic/schema/">
    <UpdateTime>2024-05-07T00:00:00+08:00</UpdateTime>
    <UpdateInterval>86400</UpdateInterval>
    <AuthorityCode>NFB</AuthorityCode>
    <LinkVersion>23.05.1</LinkVersion>
    <ETagPairs>
    <ETagPair>
    <ETagPairID>03F4232S-03F4263S</ETagPairID>        # eTag 配對路徑代碼
    <StartETagGantryID>03F4232S</StartETagGantryID>   # eTag配對起始點偵測站代碼
    <EndETagGantryID>03F4263S</EndETagGantryID>       # eTag配對結束點偵測站代碼
    <Description>崁頂-南州</Description>               # 配對路徑文字描述
    <Distance>3.08</Distance>                         # 配對路徑距離, GIS提供的配對路徑距離(KM)
    <StartLinkID>0000300042300T</StartLinkID>         # 起點基礎路段代碼
    <EndLinkID>0000300042600T</EndLinkID>             # 迄點基礎路段代碼
    <Geometry>LINE                                    # 配對路徑線型圖資資料, 格式為WKT
    """

    f=data.split('\n')

    # 取巧方式
    list_ETagPairID=[]
    list_StartETagGantryID=[]
    list_EndETagGantryID=[]
    list_Description=[]
    list_Distance=[]
    
    for line in f:
        if "<ETagPairID>" in line:
            list_ETagPairID.append(line.split('>')[1].split('<')[0])
        if "<StartETagGantryID>" in line:
            list_StartETagGantryID.append(line.split('>')[1].split('<')[0])
        if "<EndETagGantryID>" in line:
            list_EndETagGantryID.append(line.split('>')[1].split('<')[0])
        if "<Description>" in line:
            list_Description.append(line.split('>')[1].split('<')[0])
        if "<Distance>" in line:
            list_Distance.append(line.split('>')[1].split('<')[0])             
    
    df_ETagPair=pd.DataFrame({"ETagPairID":list_ETagPairID,
                              "StartETagGantryID":list_StartETagGantryID,
                              "EndETagGantryID":list_EndETagGantryID,
                              "Description":list_Description,
                              "Distance":list_Distance})
    df_ETagPair.to_csv("ETagPair.csv", index=False)
    return df_ETagPair


def data_ETagPairLive(data):
# eTag配對路徑動態資訊(ETagPairLive.xml)
# https://tisvcloud.freeway.gov.tw/history/motc20/ETagPairLive.xml
# ETagPairLive資料樣式
    """
    <ETagPairLiveList xmlns="http://traffic.transportdata.tw/standard/traffic/schema/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://traffic.transportdata.tw/standard/traffic/schema/">
    <UpdateTime>2024-05-07T14:00:00+08:00</UpdateTime>
    <UpdateInterval>300</UpdateInterval>
    <AuthorityCode>NFB</AuthorityCode>
    <ETagPairLives>
    <ETagPairLive>
    <ETagPairID>03F4232S-03F4263S</ETagPairID> # ETag 配對路徑編號
    <StartETagStatus>0</StartETagStatus>       # 配對起始點設備狀態
    <EndETagStatus>0</EndETagStatus>           # 配對結束點設備狀態
    <Flows>
    <Flow>
    <VehicleType>31</VehicleType>              # 車種代碼
    <TravelTime>107</TravelTime>               # 平均旅行時間(指定車種下), 單位:秒
    <StandardDeviation>0</StandardDeviation>   # 配對樣本數之旅行時間標準差, 單位:秒
    <SpaceMeanSpeed>102</SpaceMeanSpeed>       # 平均車速 (指定車種下), 單位:KM/Hr
    <VehicleCount>14</VehicleCount>            # 配對樣本數(指定車種下), 單位:輛
    </Flow>
    <Flow>
    <VehicleType>32</VehicleType>
    <TravelTime>104</TravelTime>
    <StandardDeviation>0</StandardDeviation>
    <SpaceMeanSpeed>106</SpaceMeanSpeed>
    <VehicleCount>6</VehicleCount>
    </Flow>
    <StartTime>2024-05-07T13:30:00+08:00</StartTime>              # 資料蒐集起始時間(指通過迄點資料)
    <EndTime>2024-05-07T13:35:00+08:00</EndTime>                  # 資料蒐集結束時間(指通過迄點資料)
    <DataCollectTime>2024-05-07T13:35:00+08:00</DataCollectTime>  # 資料蒐集時間
    </ETagPairLive>
    """

# (此方式未完成待處理)
# df = pd.read_xml("https://tisvcloud.freeway.gov.tw/history/motc20/ETagPairLive.xml")

# (此方式未完成待處理)
# with open("C:\\Users\\g_s_s\\Desktop\\113年國道智慧交通管理創意競賽\\pages\\ETagPairLive.xml", "r") as f:
#     sio = StringIO(f.read())
#     df = pd.read_xml(sio,
#                      xpath="//pandas:ETagPairLive",
#                      namespaces={"pandas": "http://traffic.transportdata.tw/standard/traffic/schema/"})

    f=data.split('\n')

    # 取巧方式
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


# 上網抓歷史資料庫資料，取出對應時間、地點的df資料
def data_http(date_input, time_input, road_input, roadDir_input, km_input):
    http_ETag = "https://tisvcloud.freeway.gov.tw/history/motc20/ETag/" + date_input + "/ETag_0000.xml.gz"

    r = requests.get(http_ETag, timeout=60) # 上網抓檔案，格式為.xml.gz
    if r.status_code != 200:
        print("請求失敗...")
        print(r.status_code)
        print()
    else:
        data = gzip.decompress(r.content).decode("utf-8")
        print("get ETag!")

    df_ETag = data_ETag(data)
    

    http_ETagPairLive = "https://tisvcloud.freeway.gov.tw/history/motc20/ETag/" + date_input + "/ETagPairLive_" + time_input + ".xml.gz"
    
    r = requests.get(http_ETagPairLive, timeout=60) # 上網抓檔案，格式為gz
    if r.status_code != 200:
        print("請求失敗...")
        print(r.status_code)     
        print()
    else:
        data = gzip.decompress(r.content).decode("utf-8")
        print("get ETagPairLive!")
        print()
    
    df_ETagPairLive = data_ETagPairLive(data)


# 新增GantryMile特徵
    df_ETag.loc[:,"GantryMile"]=df_ETag["LocationMile"].apply(lambda x: int(x.split("K+")[0])+0.001*int(x.split("K+")[1]))

# 取出欲查詢的道路名稱及方向資料
    df_look = df_ETag[(df_ETag['RoadName']==road_input) & (df_ETag['RoadDirection']==roadDir_input)]

# 找出鄰近里程的ETagPairID
    if roadDir_input=="S":
        X = (df_look[df_look["GantryMile"]<=km_input].sort_values("GantryMile", ascending=False).iloc[0]["ETagGantryID"]
             + "-" +
             df_look[df_look["GantryMile"]>km_input].sort_values("GantryMile").iloc[0]["ETagGantryID"])
    elif roadDir_input=="N":
        X = (df_look[df_look["GantryMile"]>km_input].sort_values("GantryMile").iloc[0]["ETagGantryID"]
             + "-" +
             df_look[df_look["GantryMile"]<=km_input].sort_values("GantryMile", ascending=False).iloc[0]["ETagGantryID"])

    df_X = df_ETagPairLive[df_ETagPairLive["ETagPairID"]==X]
    return df_X


# 配合歷史資料庫(每隔5分鐘上傳一次資料)的檔案時間調整函式
def time_0_5(time_input):
    x = int(time_input[-1])
    if x>=5:
        x=5
    else:
        x=0
    return time_input[0:3]+str(x)




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

df_X = data_http(date_input, time_input, road_input, roadDir_input, km_input)

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
