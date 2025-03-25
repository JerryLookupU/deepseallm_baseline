"""
base_llm - 
基础工具类
Author: cavit
Date: 2025/2/18
"""
import time

from loguru import logger
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy import text
import pymysql
import re
import json
from zhipuai import ZhipuAI
from tabulate import tabulate
import  pandas as pd
from classify_answer.class_source import format_classify_info,classes_info
import numpy as np
from decimal import Decimal, ROUND_HALF_UP
sql_match = re.compile("""```sql\n([\s\S]*?)\n```""")




apikey = ""
if apikey == "":
    raise ValueError("API KEY ERROR")
# 数据库连接
ModelName = "glm-4-plus"
def connect_db():
    """创建mysql数据库连接"""
    co = create_engine('mysql+pymysql://root:qweasd@10.5.101.152:3306/ship3').connect()
    logger.info("connect to sqlite success!")
    return co

client = ZhipuAI(api_key=apikey)

def flash_zhipuai():
    global client
    client = ZhipuAI(api_key=apikey)

conn = connect_db()
def get_table_names(conn):
    # 执行SQL查询以获取所有表名
    query = text("SHOW TABLES")
    result = conn.execute(query)
    # 获取查询结果中的表名
    table_names = [row[0] for row in result]
    return table_names


def get_table_columns(conn, table_name):
    # 使用DESCRIBE查询获取表的字段信息
    query = text(f"DESCRIBE {table_name};")
    result = conn.execute(query)
    # 获取查询结果中的字段信息
    columns_info = [dict(zip(result.keys(), row))["Field"] for row in result.fetchall()]
    return columns_info


def get_create_table(conn,table_name):
    with conn.cursor() as cursor:
        f = cursor.execute(f"SHOW CREATE TABLE {table_name}")
        result = cursor.fetchone()
        create_table_sql = result['Create Table']
    return create_table_sql


def llm_invoke(messages,if_answer=False,retry=3):
    for i in range(retry):
        try:
            completion = client.chat.completions.create(
                model=ModelName,
                messages=messages,
                temperature=0.0
            )
            return completion.choices[0].message.content
        except Exception as e:
            time.sleep(1)
            completion = client.chat.completions.create(
                model=ModelName,
                messages=messages,
                temperature=0.0
            )
            return completion.choices[0].message.content
        return "请求出错"

def json_extract(x):
    match = re.search(r'```json(.*)```', x, re.DOTALL)
    if match is None:
        return "未返回json结构"
    v = match.group(1)
    return json.loads(v)


def llm_invoke_fix(messages, retry=1):
    if retry > 3:
        return {}
    response = llm_invoke(messages)
    try:
        result = json_extract(response)
        return result
    except Exception as e:
        user_prompt = f"""
        返回错误结果如下：
        {e}
        请根据报错情况，重新生成结果
        """
        extend_messages = [
            {"role": "assistant", "content": response},
            {"role": "user", "content": user_prompt}
        ]
        messages = messages + extend_messages
        result = llm_invoke_fix(messages, retry + 1)
        return result


def check_content(x):
    if x[:3] == "```":
        return x.strip("`")
    return x


def error_classes(x, classes_info=classes_info):
    if x not in classes_info:
        logger.info("找到问题的未知分类")
        return "未知分类"
    else:
        return x
def tail_sub(r_x):
    pattern = r'(\d+)\s([a-zA-Z]+)'
    x = re.sub(pattern, r'\1\2', r_x)
    if len(x) > 15:
        return x[:-15] + x[-15:].replace(" ","")
    else:
        n = len(x)
        h = int(n/2)
        return x[:-n] + x[-n:].replace(" ","")

def query_with_sql(conn, sql, i):
    """执行SQL,获取结果"""
    gsql = text(sql)
    df = pd.read_sql(gsql, con=conn)
    logger.info(f"查询到数据{df}")
    table_str = tabulate(df, headers='keys', tablefmt='grid', showindex=False)
    return table_str, df

def check_sql(sql):
    if "sql" in sql:
        sql = sql.replace("sql", "")
    if "```" in sql:
        sql = sql.replace("```", "")
    return sql

def query_sql_with_correction(conn, messages, sql, max_retry=3):
    """将执行失败的sql通过LLM进行整改"""
    results = "未能查询到数据"
    df = None
    for i in range(max_retry):
        try:
            df_str, df = query_with_sql(conn, sql, i)
            # sql能够正确运行就跳出循环
            break
        except Exception as e:
            logger.warning(f"sql 执行错误，报错信息为：{e}, 进行第{i + 1}次矫正。")
            sql = fix_sql_with_llm(sql, messages, e, i)
            sql = check_sql(sql)
            logger.debug(f"fixed sql: {sql}")
    if df is not None:
        if len(df) == 0:
            return sql, results, df
        return sql, df_str, df
    else:

        return sql, results, df

def fix_sql_with_llm(sql, messages, error_message, i):
    """
    调用LLM,利用工具调用能力,对sql语句进行矫正
    :client LLM客户端:
    :param sql: 待检查和修正的sql语句
    :return: 校正后的sql语句
    """
    query = f"""
    输入你的sql信息 数据库报错如下：请重新生成sql内容
    {error_message}
    """

    new_messages = messages + [
        {"role": "assistant", "content": sql},
        {"role": "user", "content": query}
    ]

    # 执行工具调用,获取结果
    completion = client.chat.completions.create(
        model=ModelName,
        messages=new_messages,
    )
    return completion.choices[0].message.content




def round(x, num=0):
    """
    四舍五入函数，将输入数字 x 四舍五入到指定的小数位数 num，并返回 float 类型结果。

    参数:
        x (float or int): 需要四舍五入的数字。
        num (int): 四舍五入到的小数位数，默认为 0。

    返回:
        float: 四舍五入后的结果。
    """
    # 将输入数字 x 转换为 Decimal 类型
    decimal_x = Decimal(str(x))

    # 构造四舍五入的目标格式，例如 num=2 时，格式为 "0.01"
    format_str = f"0.{'0' * num}" if num > 0 else "0"

    # 使用 quantize 方法进行四舍五入
    result = decimal_x.quantize(Decimal(format_str), rounding=ROUND_HALF_UP)
    if num == 0:
        return int(result)
    else:
        return float(result)


if  __name__ == "__main__":
    # table_names = get_table_names(conn)
    # print(table_names)
    # for table_name in table_names:
    #     columns_info = get_table_columns(conn, table_name)
    #     messages = [
    #         {
    #             "role": "user",
    #             "content": f"请根据以下表结构，生成一个sql查询语句，查询表名是{table_name}，查询字段是{columns_info}，查询条件是id<10，并且按照id倒序排列，并且返回json格式"}
    #     ]
    #     response = llm_invoke(messages)
    #     print(response)
    q = """
+------------------+---------------------+--------------+--------------+--------------+
| csvTimeMinute    | csvTime             | actionName   | deviceName   | actionType   |
+==================+=====================+==============+==============+==============+
| 2024-08-17 08:46 | 2024-08-17 08:46:27 | A架开机      | A架          | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-17 08:52 | 2024-08-17 08:52:43 | 折臂吊车开机 | 折臂吊车     | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-17 09:16 | 2024-08-17 09:16:54 | 折臂吊车开机 | 折臂吊车     | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-18 08:19 | 2024-08-18 08:19:27 | A架开机      | A架          | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-18 09:08 | 2024-08-18 09:08:32 | 折臂吊车开机 | 折臂吊车     | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-19 08:31 | 2024-08-19 08:31:27 | A架开机      | A架          | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-19 09:12 | 2024-08-19 09:12:47 | 折臂吊车开机 | 折臂吊车     | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-19 13:34 | 2024-08-19 13:34:27 | A架开机      | A架          | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-19 13:53 | 2024-08-19 13:53:20 | 折臂吊车开机 | 折臂吊车     | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-19 21:41 | 2024-08-19 21:41:26 | A架开机      | A架          | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-19 21:45 | 2024-08-19 21:45:06 | 折臂吊车开机 | 折臂吊车     | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 06:20 | 2024-08-20 06:20:09 | A架开机      | A架          | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 06:27 | 2024-08-20 06:27:09 | A架开机      | A架          | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 09:12 | 2024-08-20 09:12:09 | A架开机      | A架          | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 09:17 | 2024-08-20 09:17:17 | 折臂吊车开机 | 折臂吊车     | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 14:10 | 2024-08-20 14:10:22 | 折臂吊车开机 | 折臂吊车     | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 15:32 | 2024-08-20 15:32:38 | 折臂吊车开机 | 折臂吊车     | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 17:19 | 2024-08-20 17:19:09 | A架开机      | A架          | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 17:21 | 2024-08-20 17:21:04 | 折臂吊车开机 | 折臂吊车     | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-20 19:47 | 2024-08-20 19:47:45 | 折臂吊车开机 | 折臂吊车     | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-21 07:03 | 2024-08-21 07:03:09 | A架开机      | A架          | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-21 07:17 | 2024-08-21 07:17:37 | 折臂吊车开机 | 折臂吊车     | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-21 08:25 | 2024-08-21 08:25:55 | 折臂吊车开机 | 折臂吊车     | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-21 16:44 | 2024-08-21 16:44:09 | A架开机      | A架          | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-21 16:47 | 2024-08-21 16:47:10 | 折臂吊车开机 | 折臂吊车     | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-23 08:03 | 2024-08-23 08:03:08 | A架开机      | A架          | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-23 08:16 | 2024-08-23 08:16:04 | 折臂吊车开机 | 折臂吊车     | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-23 09:30 | 2024-08-23 09:30:24 | 折臂吊车开机 | 折臂吊车     | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-23 17:58 | 2024-08-23 17:58:08 | A架开机      | A架          | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-23 18:00 | 2024-08-23 18:00:44 | 折臂吊车开机 | 折臂吊车     | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-23 19:16 | 2024-08-23 19:16:02 | 折臂吊车开机 | 折臂吊车     | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-24 07:55 | 2024-08-24 07:55:08 | A架开机      | A架          | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-24 08:08 | 2024-08-24 08:08:58 | 折臂吊车开机 | 折臂吊车     | 下放         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-24 16:03 | 2024-08-24 16:03:08 | A架开机      | A架          | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-24 16:04 | 2024-08-24 16:04:28 | 折臂吊车开机 | 折臂吊车     | 回收         |
+------------------+---------------------+--------------+--------------+--------------+
| 2024-08-24 18:47 | 2024-08-24 18:47:01 | 折臂吊车开机 | 折臂吊车     | 其他         |
+------------------+---------------------+--------------+--------------+--------------+
2024-08-17到2024-08-24期间，每天A架和折臂吊车的第一次开机时间 （返回以 XX:XX 格式，英文逗号隔开）
    """
    messages_demo = [{"role":"user","content":q}]
    response = llm_invoke(messages_demo,True)
    print(response)