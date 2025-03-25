"""
meta_solver - 

Author: cavit
Date: 2025/2/21
"""
import pandas as pd
from loguru import logger
from classify_answer.base_llm import query_sql_with_correction,llm_invoke
# 仅解决每个品类的原子问题
from classify_answer.llm_text2sql import action_sql,device_sql,question_key_match,check_sql
from classify_answer.llm_text2sql import total_device_sql as device_energy_sql
from classify_answer.llm_text2sql import time_parser,duration_parser,sql_to_str,energy_table_match,sql_energy,list_desc,energy_time_parser
from classify_answer.base_llm import connect_db
from classify_answer.classify_function_call import table_function_call,agg_function_call
from classify_answer.table_answer import middle_answer, db_table_answer, info_table_answer, angle_answer, final_answer,fast_answer
from classify_answer.retriver_sql_generate import retriver_llm
# 考虑一个强大的解决单天的action
import numpy as np
import re
conn = connect_db()
from tabulate import tabulate

def meta_action_solver(question,conn,question_type=""):
    time_array = time_parser(question)
    date_str = sql_to_str(time_array)
    message,sql = action_sql(question,date_str)
    sql,df_str,df = query_sql_with_correction(conn=conn, messages=message, sql=sql)
    logger.info("sql:"+sql)
    table_result_str = []
    logger.info("读取到数据：\n" + df_str)
    if df is not None:
        table_result_list = table_function_call(question, df)
        for table_result in table_result_list:
            table_answer = middle_answer(question,table_result,question_type)
            table_result_str.append(table_answer)
        table_total_answer = "\n".join(table_result_str)
        board_label = "以下是数据库查询数据：\n" + df_str + "\n"
        board_label += "以下是函数处理分析得到结果：\n-----\n" + table_total_answer + "\n-----\n"
        logger.info(f"得到表数据函数处理结果：{table_result_str}")
        agg_result = agg_function_call(question,board_label)
        logger.info(f"得到聚合函数结果：{agg_result}")
        agg_result_str = "\n".join(str(agg_result))
        board_label += "以下是聚合函数处理得到结果：\n" + agg_result_str
        answer = middle_answer(question,board_label,question_type)
        return answer
    answer = middle_answer(question,df_str)
    return answer

def meta_action_fast_solver(question,conn,question_type=""):
    time_array = time_parser(question)
    date_str = sql_to_str(time_array)
    message,sql = action_sql(question,date_str)
    sql,df_str,df = query_sql_with_correction(conn=conn, messages=message, sql=sql)
    logger.info("sql:"+sql)
    table_result_str = []
    logger.info("读取到数据：\n" + df_str)
    if df is not None:
        table_result_list = table_function_call(question, df)
        for table_result in table_result_list:
            table_answer = middle_answer(question,table_result,question_type)
            table_result_str.append(table_answer)
        table_total_answer = "\n".join(table_result_str)
        board_label = "以下是数据库查询数据：\n" + df_str + "\n"
        board_label += "以下是函数处理分析得到结果：\n-----\n" + table_total_answer + "\n-----\n"
        logger.info(f"得到表数据函数处理结果：{table_result_str}")
        agg_result = agg_function_call(question,board_label)
        logger.info(f"得到聚合函数结果：{agg_result}")
        agg_result_str = "\n".join(str(agg_result))
        board_label += "以下是聚合函数处理得到结果：\n" + agg_result_str
        answer = final_answer(question,board_label,final=True)
        return answer
    answer = final_answer(question,df_str)
    return answer




def meta_energy_solver(question,conn,question_type=""):
    try:
        table_info_data = energy_table_match(question)
    except Exception as e:
        logger.info("meta_energy_solver：question:"+question)
        rewrite_question_data = """上轮回答报错：""" + str(e) + """请重新生成问题匹配"""
        table_info_data = energy_table_match(rewrite_question_data)
    action_time_list = energy_time_parser(question)
    sql_array_result = device_energy_sql(table_info_data,action_time_list)
    logger.info("生成能耗类问题sql:" + str(sql_array_result))
    result = []
    total_sum = False
    total_list = []
    if len(sql_array_result) == 2:
        """计算两个总量的比例，和差"""
        total_sum = True
    for gitem in sql_array_result:
        temp_list = []
        for item in gitem:
            if item["type"] == "理论发电量":
                midu = item["油密度"]
                rezhi = item["油热值"]
                v = sql_energy(item["sql"], item["type"], conn, midu, rezhi)
                temp_list.append(v)
                result.append({"子问题描述": item["desc"], "计算得到结果": str(v) + "kWh"})
            else:
                v = sql_energy(item["sql"], item["type"], conn, None, None)
                temp_list.append(v)
                result.append({"子问题描述": item["desc"], "计算得到结果": v})
        result.append(list_desc(temp_list))
        total_list.append(np.sum(temp_list))
    if total_sum:
        result.append({"子问题描述":"请计算上述分组的总量之和，总量差值和总量比例","计算得到结果":"总量之和为："+str(sum(total_list)) + " 总量之差为: "+ str(total_list[0] - total_list[1]) + " 总量比值为：" + str(total_list[0]/total_list[1]) })
    df_energy_table = pd.DataFrame(result)
    df_energy_table_str = tabulate(df_energy_table, headers='keys', tablefmt='grid', showindex=False,floatfmt=".6f")
    logger.info("数据获取结果:\n" + df_energy_table_str)
    function_result = agg_function_call(question,df_energy_table_str)
    if function_result is None:
        function_result = ["未进行聚合计算"]
    answer = middle_answer(question,"数据获取：\n" + df_energy_table_str + "\n" + "聚合函数计算结果\n" + "\n".join([str(i) for i in function_result]),question_type)
    return answer



def meta_device_info_solver(question,conn):
    current_answer =  info_table_answer(question)
    agg_result = agg_function_call(question,current_answer)
    if agg_result is None:
        agg_result = ["未进行聚合计算"]
    agg_result_str = "\n".join([str(i) for i in agg_result])
    min_answer = fast_answer(question,current_answer+"进行函数调用计算\n:" + agg_result_str,final=True)
    return min_answer

def meta_device_solver(question,conn):
    table_create_info,messages,sql = device_sql(question)
    sql, df_str, df = query_sql_with_correction(conn, messages, sql)
    logger.info("执行 sql:"+sql)
    if ("摆回到位" in question) or ("摆出到位" in question) or ("角度" in question):
        answer = angle_answer(question,df_str)
        return answer
    if ("A架" in question) and ("实际" in question) and (("时长" in question) or ("多久" in question) or ("开机效率" in question)):
        table_result_str = []
        table_result_list = table_function_call(question, df)
        for table_result in table_result_list:
            table_answer = middle_answer(question,table_result)
            table_result_str.append(table_answer)
        table_total_answer = "\n".join(table_result_str)
        board_label = "以下是数据库查询数据：\n" + df_str + "\n"
        board_label += "以下是函数处理分析得到结果：\n-----\n" + table_total_answer + "\n-----\n"
        logger.info(f"得到表数据函数处理结果：{table_result_str}")
        agg_result = agg_function_call(question,board_label)
        logger.info(f"得到聚合函数结果：{agg_result}")
        agg_result_str = "\n".join(str(agg_result))
        board_label += "以下是聚合函数处理得到结果：\n" + agg_result_str
        answer = middle_answer(question,board_label)
        return answer
    answer = db_table_answer(sql, df_str, question, db_info=table_create_info)
    return answer

sql_match = re.compile("""```sql\n([\s\S]*?)\n```""")

# TODO 用 RAG的方式引入经常出错和改正内容 （工作量：知识库整理）
def meta_sql_solver(question,conn,question_type=""):
    database_info = question_key_match(question)
    if "伸缩推" in question:
        search_qustion = """伸缩推使用次数为 允许功率>0开启 允许功率<=0 为关闭，计算方式类似A架开机"""
    else:
        search_qustion = question
    example_document = retriver_llm(search_qustion)
    time_parser_data = time_parser(question)
    logger.info("当前sql查询问题："+question)
    logger.info("时间建议："+str(time_parser_data))
    if len(time_parser_data) == 0:
        time_parser_data = "问题中暂无时间筛选条件"
    else:
        time_parser_data = "时间筛选条件可以参考" + "\n".join(time_parser_data)
    system_prompt = f"""
        你是一名数据库专家，你能根据用户的sql语句进行pandas.read_sql 进行数据查询，并根据数据结果回答问题，注意大小写：
        {database_info}
        请你根据 数据库信息 生成 sql 查询语句
        未提及年份默认为2024
        {example_document}
        你可以参考上述案例sql结构进行回答
    ```
    你还需要注意以下内容：
    1、只有返回结果的sql才可以用格式: ```sql\n xxx ``` 封装 
    2、！！！！！注意: 返回结果仅能处理单条sql, 一定只能返回一条sql 如果有多条sql情况，请合并这些sql
    3、多个不同动作，使用 ROW_NUMBER() 判断次数时候需要增加 PARTITION BY deviceName, actionName ORDER BY STR_TO_DATE(csvTime, '%Y-%m-%d %H:%i:%s') 来进行动作分组和排序
    4、特别注意：比较时间，一定使用TIME函数来进行判断
    5、生成结果尽量参考已经给定的sql结构，如果遇到未知的问题请根据 数据库schema 生成，不要乱猜表和字段值
    6、注意task_action字段值 布放 即 下放
    """
    user_prompt = f"""
    要求：
    1、请直接生成sql, 不解释不说明
    2、尽量参考案例sql
    3、csvTime粒度到秒为止，通常问题询问时间点为分钟粒度 TIME(csvTime) = 'xx:xx:00' 是无效筛选字段，
        可以采用 csvTime LIKE "2024-01-01 XX:XX%" 或者 csvTime >= '2024-01-01 XX:XX:00' and csvTime <= '2024-01-01 XX:XX:59' 的方式进行查询
        {time_parser_data}
    问题如下：
    {question}
    请生成sql
    """
    if question_type == "动作数据查询":
        extend_prompt = f"""
    注意如果只是查询动作类的sql,参考下列sql查询方式：
    问题 2024-08-xx 深海作业过程中,小艇检查完毕和入水的时间？
    ```sql
    SELECT
     actionName,
     TIME_FORMAT(STR_TO_DATE(csvTime, '%Y-%m-%d %H:%i:%s'), '%H:%i') as action_time
    FROM task_action
    WHERE
     actionName IN ('小艇检查完毕', '小艇入水')
     AND DATE(csvTime) = '2024-08-17'
        """
        user_prompt += extend_prompt
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    response = llm_invoke(messages)
    sql = check_sql(response)
    sql, df_str, df = query_sql_with_correction(conn, messages, sql)
    logger.info("执行 sql:\n"+sql)
    current_answer = fast_answer(question, f"问题的查询sql如下：{sql}\n 数据库查询数据结果：\n" + df_str)
    return current_answer

if __name__ == "__main__":
    question = "2024/05/26深海作业A布放阶段在7:00~10:00之间进行，回收过程中小艇入水和征服者出水的时间分别是（以XX:XX输出，逗号隔开）？"
    answer = meta_sql_solver(question,conn)
    print(answer)

