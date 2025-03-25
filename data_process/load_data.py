"""
load_data - 

Author: cavit
Date: 2025/3/2
"""
import os
import pandas as pd
# create table task_action
# (
#     `index`       bigint null,
#     csvTimeMinute text   null,
#     actionName    text   null,
#     deviceName    text   null,
#     actionType    text   null,
#     csvTime       text   null
# );


dfx = pd.read_csv("task_action.csv",header=None)
# print(df)
dfx.columns =["","csvTimeMinute","actionName","deviceName","actionType","csvTime"]
del dfx[""]
# 数据表
import glob
import pandas as pd
paths = glob.glob("../data/v1/[a-zA-Z]*.csv")
import pymysql
from sqlalchemy import create_engine

# 后续需要自行处理字段 需要增加库表情况 设置 数据库 ship1
engine = create_engine('mysql+pymysql://root:qweasd@10.5.101.152:3306/ship2')

def dtype_col(df):
    for i in df.columns:
        if "Ajia" in i:
            df[i] = df[i].astype(float)
        if "PLC" in i:
            df[i] = df[i].astype(float)

for path in paths:
    print(path)

    if "Ajia" in path:
        file_name = os.path.basename(path)
        file_name = file_name.split(".")[0]
        name = file_name[:-2]
        df = pd.read_csv(path)
        if "Unnamed: 0" in df.columns:
            del df["Unnamed: 0"]
        df = df.replace("error",-1)
        dtype_col(df)
        df.to_sql(name,con=engine,if_exists="append",index=False)
    else:
        file_name = os.path.basename(path)
        file_name = file_name.split(".")[0]
        name = file_name[:-2]
        df = pd.read_csv(path)
        if "Unnamed: 0" in df.columns:
            del df["Unnamed: 0"]
        df = df.replace("error", -1)
        dtype_col(df)
        df.to_sql(name,con=engine,if_exists="append",index=False)

dfx.to_sql("task_action",con=engine,if_exists="replace",index=False)