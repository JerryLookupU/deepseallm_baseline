import re
import json
from classify_answer.base_llm import llm_invoke_fix,llm_invoke
from loguru import logger
from classify_answer.config import keys_tables,device_info_table,device_keyword,energy_table_dict
import pandas as pd
from tabulate import tabulate
from config import table_info
import numpy as np



table_name_com = re.compile("CREATE TABLE `(.*)` .*")
table_info_dict = {}
for i in table_info:
    tbn = table_name_com.findall(i)[0]
    table_info_dict[tbn] = i

def word_in(x,key):
    if key in x:
        return True
    else:
        return False

def keyword_match(fields,keywords=device_keyword):
    words = []
    for word in keywords:
        if word in fields:
            words.append(word)
    return words

def filterx(x):
    if (pd.isnull(x["安全保护设定值"])) and (pd.isnull(x["参数下限"])) and (pd.isnull(x["参数上限"])) and (x["报警值"].strip() == ""):
        return False
    return True


def question_key_match(question,keywords=keys_tables,table_dict = table_info_dict):
    table = []
    for key in keywords.keys():
        if key in question:
            table += keywords[key]
    uni_table = list(set(table))
    table_min_info = [table_dict[tb].strip() for tb in uni_table]
    return "\n\n".join(table_min_info)

def check_sql(sql):
    if "sql" in sql:
        sql = sql.replace("sql", "")
    if "```" in sql:
        sql = sql.replace("```", "")
    return sql

df = pd.DataFrame(device_info_table)
df["match_words"] = df["参数中文名"].apply(lambda x:keyword_match(x))
device_df = df[df.apply(lambda x:filterx(x),axis=1)]
def keyword_question_match_device(question,df=device_df,keyword=device_keyword):
    keyword_matchs = keyword_match(question,keyword)
    logger.info("匹配到字段"+str(keyword_matchs))
    def keyword_match_one(x,nkv):
        res = []
        for i in nkv:
            if i in x:
                res.append(i)
        return len(res)
    patch_temp = df["match_words"].apply(lambda x:keyword_match_one(x,keyword_matchs)).sort_values()[::-1]
    patch_temp = patch_temp[patch_temp > 0]
    data = df.loc[patch_temp.index][["参数名", "参数中文名", "参数下限", "参数上限", "报警值单位", "报警值", "屏蔽值", "报警信号延迟值",
                   "安全保护设定值", "超过安全保护设定值之后动作"]]
    table_str = tabulate(data, headers='keys', tablefmt='grid', showindex=False)
    return table_str

def time_parser(question,desc_ctx=None):
    system_prompt = """
    你能根据历史信息和问题
    未提及年份默认为2024
    请你根据问题生成sql语句的条件字符串，现在时间的字段名称为 csvTime,你需要根据问题生成 条件查询字符串，不要生成完整sql语句，但是一定要严格保障结构完整准确。
    说明： 上午 通常是指 00:00:00 ~ 12:00:59 下午则为 12:00:00 ~ 23:59:59
    注意时间秒数的开闭：均是  xx:xx:00 ~ xx:xx:59 为闭区间
    问题分成一下集中情况：

    一、从历史信息中获取时间段并查询问题
    案例1：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 下放阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 下放阶段开始时间是 2024/xx/xx aa:aa:aa分 结束是 2024/xx/xx bb:bb:bb分"}
    {"子问题"："2024/xx/xx 回收阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 回收阶段开始时间是 2024/xx/xx cc:cc:cc分 结束是 2024/xx/xx dd:dd:dd分"}
    问题：
    2024/xx/xx 作业能耗是多少（单位化成kWh，保留2位小数，下放阶段以ON DP和OFF DP为标志，回收阶段以A架开机和关机为标志）？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")", "(csvTime >= "2024-xx-xx cc:cc:00") and (csvTime <= "2024-xx-xx dd:dd:59")"]
    ```

    案例2：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 下放阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 下放阶段开始时间是 2024/xx/xx aa:aa:aa分 结束是 2024/xx/xx bb:bb:bb分"}
    {"子问题"："2024/xx/xx 回收阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 回收阶段开始时间是 2024/xx/xx cc:cc:cc分 结束是 2024/xx/xx dd:dd:dd分"}
    问题：
    2024/xx/xx 下放作业能耗是多少（单位化成kWh，保留2位小数，下放阶段以ON DP和OFF DP为标志，回收阶段以A架开机和关机为标志）？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")"]
    ```

    案例3：
    ```
    历史信息：
    {"子问题"："2024/xx/xx ON DP阶段什么时候开始？什么时候结束","回答":"2024/xx/xx ON DP 开始时间为 aa:aa:aa,bb:bb:bb,cc:cc:cc 结束时间为 dd:dd:dd,ee:ee:ee和ff:ff:ff"}
    问题：
    2024/xx/xx DP时间段的能耗是多少 ？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx dd:dd:59")","(csvTime >= "2024-xx-xx bb:bb:00") and (csvTime <= "2024-xx-xx ee:ee:59")","(csvTime >= "2024-xx-xx cc:cc:00") and (csvTime <= "2024-xx-xx ff:ff:59")"]
    ```

    案例4：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 征服者什么时候落座","回答":"2024/xx/xx 征服者落座在2024-xx-xx aa:aa:aa"}
    {"子问题"："2024/xx/xx 征服者落座后A架什么时候关机","回答":"2024/xx/xx 征服者落座后A架在bb:bb:bb关机"}  
    ```
    2024-xx-xx 下午 征服者落座后A架关机的这段时间 折臂吊车的能耗是多少？
    /**
    子问题的时间信息更为具体，下午的时间范围明显包含了更具体的时间，问题的时间条件为 下午的征服者落座后A架关机的这段时间，所以选择更为细致历史信息中回答的时间
    **/
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")"]
    ```

    二、分段或者多天的时间查询问题
    案例：
    20240101 20240102 和 20240103 哪天折臂吊车工作时间最长
    /**
    需要按照天来判断，那么就需要将天拆分成离散的三个时间段
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime >= "2024-01-01 23:59:59")","(csvTime >= "2024-01-02 00:00:00") and (csvTime >= "2024-01-02 23:59:59")","(csvTime >= "2024-01-03 00:00:00") and (csvTime >= "2024-01-03 23:59:59")"]
    ```

    三、询问该天发生什么动作问题
    案例1：
    20240101 01:00 有什么设备发生了什么动作
    /**
    如果查询时间点的信息，则 转化成时间段，将其设置为前后一小时查询
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime >= "2024-01-01 02:00:59")"]
    ```

    案例2：
    20240101 什么动作同时发生
    /**
    如果查询时间点的信息，则 转化成时间段，将其设置为前后一小时查询
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime >= "2024-01-01 23:59:59")"]
    ```

    四、问题案例中有干扰时间或者干扰动作
    /**
    需要明确对于问题的时间筛选和对于问题条件判断
    **/
    2024/8/xx xx:00 以后A架的第一次开启时间？A：aa:00；B：bb:00；C：无
    ```json
    ["(csvTime >= "2024/8/xx xx:00:00") and (csvTime >= "2024/8/xx 23:59:59")"]
    ```

    五、当问题中无时间字段
    案例：
    什么时候系统发生故障？
    ```json
    [""]
    ```
    
    判断某天是否有深海作业A，直接判断该天是否有下放并且有回收动作类型
    """

    if desc_ctx is None:
        user_prompt = f"""
        请根据问题返回,必须式python json.loads 能够解析的json结构(禁止使用单引号)
        {question}
        """
    else:
        user_prompt = f"""
        请根据问题返回,必须式python json.loads 能够解析的json结构(禁止使用单引号)
        历史信息如下：
        {desc_ctx}
        问题
        {question}
        """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]

    result = llm_invoke_fix(messages)
    sql_result = []
    for ir in result:
        if "xx" in ir:
            continue
        sql_result.append(ir)
    if (sql_result == [""]) or (sql_result == []):
        return []
    return sql_result

def duration_parser(question,desc_ctx=None):
    system_prompt = """
    你能根据历史信息和问题
    未提及年份默认为2024
    请你根据问题生成sql语句的条件字符串，现在时间的字段名称为 csvTime,你需要根据问题生成 条件查询字符串，不要生成完整sql语句，一定要严格保障结构完整准确。
    说明： 上午 通常是指 00:00:00 ~ 12:00:59 下午则为 12:00:00 ~ 23:59:59

    问题分成一下几种情况：
    一、从历史信息中获取时间段并查询问题
    案例1：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 下放阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 下放阶段开始时间是 2024/xx/xx aa:aa:aa分 结束是 2024/xx/xx bb:bb:bb分"}
    {"子问题"："2024/xx/xx 回收阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 回收阶段开始时间是 2024/xx/xx cc:cc:cc分 结束是 2024/xx/xx dd:dd:dd分"}
    问题：
    2024/xx/xx 作业能耗是多少（单位化成kWh，保留2位小数，下放阶段以ON DP和OFF DP为标志，回收阶段以A架开机和关机为标志）？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")", "(csvTime >= "2024-xx-xx cc:cc:00") and (csvTime <= "2024-xx-xx dd:dd:59")"]
    ```

    案例2：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 下放阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 下放阶段开始时间是 2024/xx/xx aa:aa:aa分 结束是 2024/xx/xx bb:bb:bb分"}
    {"子问题"："2024/xx/xx 回收阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 回收阶段开始时间是 2024/xx/xx cc:cc:cc分 结束是 2024/xx/xx dd:dd:dd分"}
    问题：
    2024/xx/xx 下放作业能耗是多少（单位化成kWh，保留2位小数，下放阶段以ON DP和OFF DP为标志，回收阶段以A架开机和关机为标志）？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")"]
    ```

    案例3：
    ```
    历史信息：
    {"子问题"："2024/xx/xx ON DP阶段什么时候开始？什么时候结束","回答":"2024/xx/xx ON DP 开始时间为 aa:aa:aa,bb:bb:bb,cc:cc:cc 结束时间为 dd:dd:dd,ee:ee:ee和ff:ff:ff"}
    问题：
    2024/xx/xx DP时间段的能耗是多少 ？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx dd:dd:59")","(csvTime >= "2024-xx-xx bb:bb:00") and (csvTime <= "2024-xx-xx ee:ee:59")","(csvTime >= "2024-xx-xx cc:cc:00") and (csvTime <= "2024-xx-xx ff:ff:59")"]
    ```

    案例4：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 征服者什么时候落座","回答":"2024/xx/xx 征服者落座在2024-xx-xx aa:aa:aa"}
    {"子问题"："2024/xx/xx 征服者落座后A架什么时候关机","回答":"2024/xx/xx 征服者落座后A架在bb:bb:bb关机"}  
    ```
    2024-xx-xx 下午 征服者落座后A架关机的这段时间 折臂吊车的能耗是多少？
    /**
    子问题的时间信息更为具体，下午的时间范围明显包含了更具体的时间，问题的时间条件为 下午的征服者落座后A架关机的这段时间，所以选择更为细致历史信息中回答的时间
    **/
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")"]
    ```

    二、分段或者多天的时间查询问题
    案例：
    20240101 20240102 和 20240103 哪天折臂吊车工作时间最长
    /**
    需要按照天来判断，那么就需要将天拆分成离散的三个时间段
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime >= "2024-01-01 23:59:59")","(csvTime >= "2024-01-02 00:00:00") and (csvTime <= "2024-01-02 23:59:59")","(csvTime >= "2024-01-03 00:00:00") and (csvTime <= "2024-01-03 23:59:59")"]
    ```

    三、询问该天发生什么动作问题
    案例1：
    20240101 01:00 有什么设备发生了什么动作
    /**
    如果查询时间点的信息，可以转化成时间段，将其设置为前后一小时查询
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime <= "2024-01-01 02:00:59")"]
    ```

    案例2：
    20240101 什么动作同时发生
    /**
    如果查询时间点的信息，则 转化成时间段，将其设置为前后一小时查询
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime <= "2024-01-01 23:59:59")"]
    ```

    案例3：
    20240102 01:00 有什么动作发生？
    /**
    csvTime 的时间点粒度均是秒级别，单对分钟级别提问动作发生,查询这个范围内的时间
    **/
    ```json
    ["(csvTime >= "2024-01-02 01:00:00") and (csvTime <= "2024-01-02 01:00:59")"]
    ```

    四、问题案例中有干扰时间或者干扰动作
    /**
    需要明确对于问题的时间筛选和对于问题条件判断
    **/
    2024/8/xx 18:00以后A架的第一次开启时间？A：19:00；B：20:00；C：无
    ```json
    ["(csvTime >= "2024-08-xx 18:00:00") and (csvTime <= "2024-08-xx 23:59:59")"]
    ```

    五、当问题中无时间字段
    案例：
    什么时候系统发生故障？
    ```json
    [""]
    ```
    
    六、特殊时段筛选问题：
    案例：
    统计2024/01/01-1/2在9点前开始作业的比例
    /**
    一、如果筛选时间是 上午 则时间选择段为上午  时间选择 00:00:00 ~ 12:00:59 
    二、如果筛选时间为 下午时间段 时间设置为 12:00:00 ~ 23:59:59
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime <= "2024-01-01 12:00:59")","(csvTime >= "2024-01-02 00:00:00") and (csvTime <= "2024-01-02 12:00:59")"]
    ```

    统计2024/01/01-1/2在征服者16点前落座的比例
    ```json
    ["(csvTime >= "2024-01-01 12:00:00") and (csvTime <= "2024-01-01 23:59:59")","(csvTime >= "2024-01-02 12:00:00") and (csvTime <= "2024-01-02 23:59:59")"]
    ```
    
    如果连续时间段，没有筛选条件可以合并，不用按照天粒度拆分，例如：
    统计2024/01/01-1/2在小艇落座时间点
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime <= "2024-01-02 23:59:59")"]
    ```
    """



    if desc_ctx is None:
        user_prompt = """
         请根据问题返回,必须式python json.loads 能够解析的json结构(禁止使用单引号)
         {question}
         注意：
         1、csvTime的数据粒度到秒级别，如果询问分钟级别的信息 采用 (csvTime >= xx:xx:00 AND csvTime <= xx:xx:59) 的方法查询
         2、如果问题中出现选项，不要被选项干扰
         """
    else:
        user_prompt = f"""
        请根据问题返回,必须式python json.loads 能够解析的json结构(禁止使用单引号)
        历史信息如下：
        {desc_ctx}
        问题
        {question}
        注意：
        1、csvTime的数据粒度到秒级别，如果询问分钟级别的信息 采用 (csvTime >= xx:xx:00 AND csvTime <= xx:xx:59) 的方法查询
        2、如果问题中出现选项，不要被选项干扰
        """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]

    result = llm_invoke_fix(messages)
    sql_result = []
    for ir in result:
        if "xx" in ir:
            continue
        sql_result.append(ir)
    if (sql_result == [""]) or (sql_result == []):
        return []
    return sql_result


def energy_time_parser(question,desc_ctx=None):
    system_prompt = """
    你能根据历史信息和问题
    未提及年份默认为2024
    请你根据问题生成sql语句的条件字符串，现在时间的字段名称为 csvTime,你需要根据问题生成 条件查询字符串，不要生成完整sql语句，但是一定要严格保障结构完整准确。
    说明： 上午 通常是指 00:00:00 ~ 12:00:59 下午则为 12:00:00 ~ 23:59:59

    问题分成一下集中情况：

    一、从历史信息中获取时间段并查询问题
    案例1：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 下放阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 下放阶段开始时间是 2024/xx/xx aa:aa:aa分 结束是 2024/xx/xx bb:bb:bb分"}
    {"子问题"："2024/xx/xx 回收阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 回收阶段开始时间是 2024/xx/xx cc:cc:cc分 结束是 2024/xx/xx dd:dd:dd分"}
    问题：
    2024/xx/xx 作业能耗是多少（单位化成kWh，保留2位小数，下放阶段以ON DP和OFF DP为标志，回收阶段以A架开机和关机为标志）？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")", "(csvTime >= "2024-xx-xx cc:cc:00") and (csvTime <= "2024-xx-xx dd:dd:59")"]
    ```

    案例2：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 下放阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 下放阶段开始时间是 2024/xx/xx aa:aa:aa分 结束是 2024/xx/xx bb:bb:bb分"}
    {"子问题"："2024/xx/xx 回收阶段什么时候开始？什么时候结束","回答":"2024/xx/xx 回收阶段开始时间是 2024/xx/xx cc:cc:cc分 结束是 2024/xx/xx dd:dd:dd分"}
    问题：
    2024/xx/xx 下放作业能耗是多少（单位化成kWh，保留2位小数，下放阶段以ON DP和OFF DP为标志，回收阶段以A架开机和关机为标志）？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")"]
    ```

    案例3：
    ```
    历史信息：
    {"子问题"："2024/xx/xx ON DP阶段什么时候开始？什么时候结束","回答":"2024/xx/xx ON DP 开始时间为 aa:aa:aa,bb:bb:bb,cc:cc:cc 结束时间为 dd:dd:dd,ee:ee:ee和ff:ff:ff"}
    问题：
    2024/xx/xx DP时间段的能耗是多少 ？
    ```
    回答：
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx dd:dd:59")","(csvTime >= "2024-xx-xx bb:bb:00") and (csvTime <= "2024-xx-xx ee:ee:59")","(csvTime >= "2024-xx-xx cc:cc:00") and (csvTime <= "2024-xx-xx ff:ff:59")"]
    ```

    案例4：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 征服者什么时候落座","回答":"2024/xx/xx 征服者落座在2024-xx-xx aa:aa:aa"}
    {"子问题"："2024/xx/xx 征服者落座后A架什么时候关机","回答":"2024/xx/xx 征服者落座后A架在bb:bb:bb关机"}  
    ```
    2024-xx-xx 下午 征服者落座后A架关机的这段时间 折臂吊车的能耗是多少？
    /**
    子问题的时间信息更为具体，下午的时间范围明显包含了更具体的时间，问题的时间条件为 下午的征服者落座后A架关机的这段时间，所以选择更为细致历史信息中回答的时间
    **/
    ```json
    ["(csvTime >= "2024-xx-xx aa:aa:00") and (csvTime <= "2024-xx-xx bb:bb:59")"]
    ```

    二、分段或者多天的时间查询问题
    案例：
    20240101 20240102 和 20240103 哪天折臂吊车工作时间最长
    /**
    需要按照天来判断，那么就需要将天拆分成离散的三个时间段
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime >= "2024-01-01 23:59:59")","(csvTime >= "2024-01-02 00:00:00") and (csvTime >= "2024-01-02 23:59:59")","(csvTime >= "2024-01-03 00:00:00") and (csvTime >= "2024-01-03 23:59:59")"]
    ```

    三、询问该天发生什么动作问题
    案例1：
    20240101 01:00 有什么设备发生了什么动作
    /**
    如果查询时间点的信息，则 转化成时间段，将其设置为前后一小时查询
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime >= "2024-01-01 02:00:59")"]
    ```

    案例2：
    20240101 什么动作同时发生
    /**
    如果查询时间点的信息，则 转化成时间段，将其设置为前后一小时查询
    **/
    ```json
    ["(csvTime >= "2024-01-01 00:00:00") and (csvTime >= "2024-01-01 23:59:59")"]
    ```

    四、问题案例中有干扰时间或者干扰动作
    /**
    需要明确对于问题的时间筛选和对于问题条件判断
    **/
    2024/8/xx xx:00 以后A架的第一次开启时间？A：aa:00；B：bb:00；C：无
    ```json
    ["(csvTime >= "2024/8/xx xx:00:00") and (csvTime >= "2024/8/xx 23:59:59")"]
    ```

    五、当问题中无时间字段
    案例：
    什么时候系统发生故障？
    ```json
    [""]
    ```
    """
    if desc_ctx is None:

        user_prompt = f"""
        请根据问题返回,必须式python json.loads 能够解析的json结构(禁止使用单引号)
        {question}
        """
    else:
        user_prompt = f"""
        请根据问题返回,必须式python json.loads 能够解析的json结构(禁止使用单引号)
        历史信息如下：
        {desc_ctx}
        问题
        {question}
        """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]

    result = llm_invoke_fix(messages)
    sql_result = []
    for ir in result:
        if "xx" in ir:
            continue
        sql_result.append(ir)
    if (sql_result == [""]) or (sql_result == []):
        return []
    return sql_result

def sql_to_str(sql_array):
    sql_result = [ "(" +i + ")"  for i in sql_array]
    if len(sql_result) == 1:
        return sql_result
    else:
        sql_result = " or ".join(sql_result)
        sql_result = "(" + sql_result + ")"
        return sql_result


# todo
def time_rewrite(question,time_list):
    system_prompt = """
    请根据以下案例和注意事项，对时间筛选逻辑进行重写（局限重写案例中出现的类似情况，稍微扩展下时间范围，其他情况如果 sql的日期逻辑查询没有问题，不需要重写）
    
    问题：统计2024/1/1-1/3在9点前开始作业的比例
    当前时间筛选结果
    ```
    (((csvTime >= "2024-01-01 00:00:00") and (csvTime <= "2024-01-01 09:00:59")) or ((csvTime >= "2024-01-02 00:00:00") and (csvTime <= "2024-01-02 09:00:59")) or ((csvTime >= "2024-01-03 00:00:00") and (csvTime <= "2024-01-03 09:00:59")))
    ```
    生成时间条件查询语句为：
    ```sql
    (((csvTime >= "2024-01-01 00:00:00") and (csvTime <= "2024-01-01 12:00:59")) or ((csvTime >= "2024-01-02 00:00:00") and (csvTime <= "2024-01-02 12:00:59")) or ((csvTime >= "2024-01-03 00:00:00") and (csvTime <= "2024-01-03 12:00:59")))
    ```
    
    问题：统计2024/1/1-1/3 下午在14点前小艇入水的比例
    当前时间筛选结果
    ```
    (((csvTime >= "2024-01-01 12:00:00") and (csvTime <= "2024-01-01 14:00:59")) or ((csvTime >= "2024-01-02 12:00:00") and (csvTime <= "2024-01-02 14:00:59")) or ((csvTime >= "2024-01-03 12:00:00") and (csvTime <= "2024-01-03 14:00:59")))
    ```
    生成时间条件查询语句为：
    ```sql
    (((csvTime >= "2024-01-01 12:00:00") and (csvTime <= "2024-01-01 23:59:59")) or ((csvTime >= "2024-01-02 12:00:00") and (csvTime <= "2024-01-02 23:59:59")) or ((csvTime >= "2024-01-03 12:00:00") and (csvTime <= "2024-01-03 23:59:59")))
    ```
    
    其他情况原样返回即可
    如：
    问题：
    """

    user_prompt = f"""
    问题：{question}
    当前时间筛选结果：
    ```
    {time_list}
    ```
    注意：
    1、csvTime的数据粒度到秒级别，如果询问分钟级别的信息 采用 (csvTime >= xx:xx:00 AND csvTime <= xx:xx:59) 的方法查询
    2、如果问题中出现选项，不要被选项干扰
    3、直接生成重写结果，不解释不说明
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt.format(question=question)}
    ]

    result = llm_invoke_fix(messages)

def action_parser(question):
    system_prompt = """
    你是一名深海作业A数据查询专家，你能根据问题的要求，准确得到问题得动作需求
    未提及年份默认为2024
    以下是 task_action 建表语句
    CREATE TABLE `task_action` (
          `index` bigint DEFAULT NULL COMMENT '索引',
          `csvTimeMinute` text COMMENT '动作发生时间，精确到分钟',
          `actionName` text COMMENT '动作名称,判断动作，值可取OFF DP、折臂吊车关机、折臂吊车开机、ON DP、A架关机、A架开机、小艇检查完毕、小艇入水、征服者起吊、征服者入水、缆绳解除、A架摆回、小艇落座、A架摆出、缆绳挂妥、征服者出水、征服者落座。举例：A架关机',
          `deviceName` text COMMENT '设备名称,动作涉及的器械，值可取折臂吊车、A架、DP，举例：折臂吊车',
          `actionType` text COMMENT '动作类型,动作涉及的深海作业A过程,值取 [下放、回收、其他]，举例：下放',
          `csvTime` text COMMENT '动作发生时间，精确到秒，举例：2024-05-16 23:49:01',
          KEY `ix_task_action_index` (`index`) COMMENT '索引键'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        /**
        描述：该表存储了深海作业A中的动作记录，包括动作名称、设备名称、动作类型及时间戳等信息。这些动作涉及A架、绞车、折臂吊车等设备的下放和回收操作。
        数据案例如下：
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |   index | csvTimeMinute    | actionName   | deviceName   | actionType   | csvTime             |
        +=========+==================+==============+==============+==============+=====================+
        |     157 | 2024-05-29 08:53 | 征服者入水   | A架          | 下放         | 2024-05-29 08:53:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |     158 | 2024-05-29 08:54 | 缆绳解除     | A架          | 下放         | 2024-05-29 08:54:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |     159 | 2024-05-29 09:00 | A架摆回      | A架          | 下放         | 2024-05-29 09:00:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        **/
    你需要根据问题生成【动作查询语句】(注意不考虑时间条件)，以下是我给你的注意事项和参考样例：
    注意事项：
        一、仅考虑task_action 表相关动作查询，如果非task_action表，则返回空结果
    问题： 2024/xx/xx 下午A架的第一次开机时间？
    回答：
    ```
    (actionName = "A架开机")
    ```
    
    问题：  2024/8/aa ~ 2024/8/bb 早上A架在8点前开机的有几天？
    回答：
    ```
    (actionName = "A架开机")
    ```
    
    问题：  2024/8/aa 早上A架在第一次开机时间？
    回答：
    ```
    (actionName = "A架开机")
    ```
    
    
    问题： 2024/xx/xx 下午A架开机后，还进行过什么动作？
    /**
    案例说明： 该案例是查询 某日下午A架开机后，其他设备还进行了什么动作，所查询动作是除了A架开机外的其他动作，所以不要进行条件返回
    **/
    回答:
    ```
    ```
    
    问题 2024-01-01 10点~11点 什么设备发生什么动作
    回答:
    ```
    ```
    """
    if "开机" in question:
        extend_prompt = """
        问题 2024-01-01 A架开机时长？
        /**
        开机时长 ===> 需要获取A架开机和A架关机 (类似的有A架、折臂吊车、DP)
        **/
        回答:
        ```
        (actionName = "A架开机" or  actionName = "A架关机")
        ```
        """
        system_prompt += extend_prompt

    if "DP" in question:
        extend_prompt = """
        DP过程 均是以下放阶段为条件
        如案例：
        问题 2024-01-01 DP开启时间?
        回答
        ```
        (actionName = "ON DP" and actionType = "下放")
        ```
        """
        system_prompt += extend_prompt

    if "作业" in question:
        extend_prompt = """
        需要注意作业问题即深海作业A的特殊问题
        - 深海作业A问题增加  (actionType = "下放" or  actionType = "回收")  的筛选条件
        - 深海作业A开始时间问题， 增加 (actionName="ON DP") 的筛选条件，例如： 2024年01月01日开始作业时间 筛选条件为： actionName="ON DP"
        - 深海作业A结束时间问题，增加 (actionName="A架关机") 的筛选条件，例如： 2024年01月01日开始作业时间 筛选条件为： actionName="A架关机"
        - 深海作业A下放阶段结束时间问题，增加 (actionName="OFF DP") 的筛选条件，例如： 2024年01月01日下放阶段结束时间 筛选条件为： actionName="OFF DP"
        - 深海作业A回收阶段结束时间问题，增加 (actionName="A架开机") 的筛选条件，例如： 2024年01月01日回收阶段开始时间 筛选条件为： actionName="A架开机"
        
        例如
        问题 2024-01-01 作业A开始时间？
        /**
        如果没有特殊指定，则深海作业A 以 ON DP 开始 A架关机结束 （其中下放阶段结束以OFF DP 结束，回收阶段以A架开机开始）
        **/
        回答:
        ```
        (actionName = "ON DP")
        ```
        
        问题 2024-01-01 作业A时长？ （涉及到深海作业A相关时长问题 均以此筛选逻辑为准）
        回答:
        ```
        (((actionName = "ON DP" or actionName = "OFF DP") AND actionType = "下放") OR ((actionName = "A架开机" or actionName = "A架关机") AND actionType = "回收"))
        ```
        
        问题 2024-01-01 作业下放阶段时长？
        回答:
        ```
        ((actionName = "ON DP" or actionName = "OFF DP") AND actionType = "下放") 
        ``` 
        
        问题 2024-01-01 作业回收阶段时长？
        回答:
        ```
        ((actionName = "A架开机" or actionName = "A架关机") AND actionType = "回收")
        ```
        """
        system_prompt += extend_prompt
    user_prompt = """
    请根据问题返回 查询条件
    {question}
    注意：
    1、注意除非特殊情况 尽量仅使用 actionName和 actionType 进行查询，需要保证。
    2、如果是多条件，尽量加上()。
    3、禁止使用 actionName,actionType和 deviceName 以外字段查询。
    4、除涉及DP动作、作业A相关以外不需要 actionType 筛选条件。
    5、发生什么动作类型问题，问题中没说明，则返回空字符串。
    """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt.format(question=question)}
    ]
    result = llm_invoke(messages)
    result = result.strip("`")
    return result

"""
    # 正确案例（不需要修改sql直接返回）如：
    # 2024/8/24 深海作业A作业开始的时间（请以XX:XX输出）？
    # /**
    # 该问题需要查询 2024年08月24日 深海作业A的开始时间，数据的查询段为该天所有数据, 深海作业A作业开始的时间 以 ON DP 开始 以 A架关机结束 （下放阶段 ON DP 开始 OFF DP结束，回收阶段 A架开机开始 A架关机结束 ）
    # **/
    # ```
    # SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTimeMinute >= "2024-08-24 00:00" and csvTimeMinute <= "2024-08-24 23:59" and (actionType = "下放" or  actionType = "回收");
    # ```
    # 回答
    # ```sql
    # SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTimeMinute >= "2024-08-24 00:00" and csvTimeMinute <= "2024-08-24 23:59" and (actionType = "下放" or  actionType = "回收");
    # ```
    # 
    # 2024/09/01 A架开机之后A架摆出？（只有在 什么设备发生什么动作或者关键动作 这种询问方式，才需要去掉actioname）
    # ```
    # SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTimeMinute >= "2024-09-01 00:00" and csvTimeMinute <= "2024-09-01 23:59" and (actionName = "A架开机" or  actionName = "A架摆出");
    # ```
    # 回答
    # ```sql
    # SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTimeMinute >= "2024-09-01 00:00" and csvTimeMinute <= "2024-09-01 23:59" and (actionName = "A架开机" or  actionName = "A架摆出");
    # ```
    # 
    # 2024/09/01 什么动作同时发生
    # ```
    # SELECT csvTimeMinute, csvTime, actionName, deviceName, actionType 
    # FROM task_action 
    # WHERE csvTime >= "2024-08-24 00:00:00" 
    # AND csvTime <= "2024-08-24 23:59:59"
    # GROUP BY csvTimeMinute 
    # HAVING COUNT(DISTINCT actionName) > 1;
    # ```
    # /**
    # 这个步骤生成的sql是用于问题的数据范围查询，不要做复杂的数据聚合计算和函数计算，仅仅只需要 将问题可能包含的数据段的动作查询出来即可，不要试图去解决问题
    # **/
    # 回答
    # ```sql
    # SELECT csvTimeMinute, csvTime, actionName, deviceName, actionType FROM task_action WHERE csvTime >= "2024-08-24 00:00:00" AND csvTime <= "2024-08-24 23:59:59"
    # ```
    # 
    # 统计2024/1/1-1/2在9点前开始作业的比例
    # ```
    # SELECT csvTimeMinute, csvTime, actionName, deviceName, actionType 
    # FROM task_action 
    # WHERE (csvTime >= "2024-01-01 00:00:00" AND csvTime < "2024-01-02 09:00:00") 
    # AND (actionType = "下放" OR actionType = "回收");
    # ```
    # 回答:
    # ```
    # SELECT csvTimeMinute, csvTime, actionName, deviceName, actionType 
    # FROM task_action 
    # WHERE ((csvTime >= "2024-01-01 00:00:00" AND csvTime < "2024-01-01 09:00:59") or (csvTime >= "2024-01-02 00:00:00" AND csvTime < "2024-01-02 09:00:59"))
    # AND (actionName = "ON DP")
    # AND (actionType = "下放" OR actionType = "回收"); 
    # ```
"""

def rewrite_sql(question,sql):
    system_prompt = """
    你是一名sql专家，请帮忙判断sql是否正确，如果sql正确保持sql不变返回，如果sql不正确，则帮忙修正错误,注意给定的sql是基于问题进行范围查询，不是生成解决问题的sql(切记)，你需要解决的是sql的逻辑错误：
    未提及年份默认为2024
    已知目前sql 主要有以下几类错误,注意 如果没有发生这类错误 不需要修改sql,如果发生这类错误，按照案例模式修改sql不要试图解决用户问题，严格按照要求生成sql。
    不要试图用sql解决全部问题，你只要检查sql是否有明显语法错误
    `actionName` text COMMENT '动作名称,判断动作，值可取OFF DP、折臂吊车关机、折臂吊车开机、ON DP、A架关机、A架开机、小艇检查完毕、小艇入水、征服者起吊、征服者入水、缆绳解除、A架摆回、小艇落座、A架摆出、缆绳挂妥、征服者出水、征服者落座。举例：A架关机',
    `deviceName` text COMMENT '设备名称,动作涉及的器械，值可取折臂吊车、A架、DP，举例：折臂吊车',
    `actionType` text COMMENT '动作类型,动作涉及的深海作业A过程,值取下放和回收，举例：下放',
    1、判断动作发生之后，还进行什么动作。
    案例：
    2024/xx/xx 上午折臂吊车关机后还发生了什么动作
    ```
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 12:00" and (actionName = "折臂吊车关机");
    ```
    /**
    目前无法立刻知道折臂吊车关机时间，折臂吊车关机后，其他动作有可能发生，所以不应该限制添加折臂吊车关机限制
    **/
    回答
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 12:00";
    ```

    2、多天日期嵌套不清
    案例：
    2024-01-12 和 2024-01-15 折臂吊车开机关机的时长是多少？
    ```
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where (csvTimeMinute >= "2024-01-12 00:00" and csvTimeMinute <= "2024-01-12 23:59") or   (csvTimeMinute >= "2024-01-15 00:00" and csvTimeMinute <= "2024-01-15 23:59")  and (actionName = "折臂吊车关机") or  (actionName = "折臂吊车开机");
    ```
    /**
    不同条件之间条件逻辑混乱，仅需要修改条件范围
    **/
    回答
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where ((csvTimeMinute >= "2024-01-12 00:00" and csvTimeMinute <= "2024-01-12 23:59") or (csvTimeMinute >= "2024-01-15 00:00" and csvTimeMinute <= "2024-01-15 23:59"))  and ((actionName = "折臂吊车关机") or  (actionName = "折臂吊车开机"));
    ```
    
    常见sql修改错误情况：
    1、某个时间点查询，csvTime的查询为  (csvTime >= "2024-xx-xx xx:xx:00") and (csvTime <= "2024-xx-xx xx:xx:59")
    2、A架是否运行判断（通A架是否开机判断），需要判断时间段中 是否有 deviceName = "A架" 情况 （征服者起吊入水都是由A架执行）
    3、XXX开机时长 通常需要 计算 XXX开机 和 XXX关机 条件
    """
    user_prompt = f"""
    生成sql 不解释不说明，直接返回sql，不要试图去解决问题，注意是区域段，
    1、修改sql时候不要修改where之前的信息
    2、如果出现案例中的错误，按照回答修改
    3、重写sql时候不要画蛇添足 增加limit order by 等方法
    3、只有在多天情况和某个动作之后，判断动作发生需要重写sql，其他情况不要重写sql
    4、注意时间的澄清 多天条件问题 均是每天附带情况，例如 02/01-02/03上午A架开机时长 ==> 为 02/01上午A架开机时长 、02/02上午A架开机时长、02/03上午A架开机时长，类似的还有N点之前（之后）开机情况
    5、如果sql符合问题则不要对sql进行修改，直接返回sql
    {question}
    ```
    {sql}
    ```
    """
    messages = [
        {"role":"system","content":system_prompt},
        {"role":"user","content":user_prompt}
    ]

    response = llm_invoke(messages)
    rw_sql = check_sql(response)
    return rw_sql


def action_sql(question,date_str=None):
    if date_str is None:
        date_str = time_parser(question)
    action_schema = """
    CREATE TABLE `task_action` (
    `index` bigint DEFAULT NULL COMMENT '索引',
    `csvTimeMinute` text COMMENT '动作发生时间，精确到分钟',
          `actionName` text COMMENT '动作名称,判断动作，值可取OFF DP、折臂吊车关机、折臂吊车开机、ON DP、A架关机、A架开机、小艇检查完毕、小艇入水、征服者起吊、征服者入水、缆绳解除、A架摆回、小艇落座、A架摆出、缆绳挂妥、征服者出水、征服者落座。举例：A架关机',
          `deviceName` text COMMENT '设备名称,动作涉及的器械，值可取折臂吊车、A架、DP，举例：折臂吊车',
          `actionType` text COMMENT '动作类型,动作涉及的深海作业A过程,值取下放和回收，举例：下放',
          `csvTime` text COMMENT '动作发生时间，精确到秒，举例：2024-05-16 23:49:01',
          KEY `ix_task_action_index` (`index`) COMMENT '索引键'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        描述：该表存储了深海作业A中的动作记录，包括动作名称、设备名称、动作类型及时间戳等信息。这些动作涉及A架、绞车、折臂吊车等设备的下放和回收操作。
        数据案例如下：
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |   index | csvTimeMinute    | actionName   | deviceName   | actionType   | csvTime             |
        +=========+==================+==============+==============+==============+=====================+
        |     157 | 2024-05-29 08:53 | 征服者入水   | A架          | 下放         | 2024-05-29 08:53:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |     158 | 2024-05-29 08:54 | 缆绳解除     | A架          | 下放         | 2024-05-29 08:54:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |     159 | 2024-05-29 09:00 | A架摆回      | A架          | 下放         | 2024-05-29 09:00:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+"""

    system_prompt = """你是一名数据库专家，你现在需要根据问题，提取出深海作业相关的数据，并进行分析：
    数据表结构如下：
        {action_schema}
    你需要根据表结构生成数据提取sql,注意sql查询的时间范围，你需要尽可能囊括出时间范围，注意边界时间，由于字段为字符串，
    未提及年份默认为2024
    不要使用 时间处理函数；
    不要使用 LIMIT,ORDER BY；
    数据查询开始时间 不要和结束时间相同；
    （actionType 字段中）需要带有下放或者回收 才表示当天有深海作业A
    例如：
    2024/8/24 深海作业A作业开始的时间（请以XX:XX输出）？
    /**
    该问题需要查询 2024年08月24日 深海作业A的开始时间，数据的查询段为该天所有数据, 深海作业A作业开始的时间 以 ON DP 开始 以 A架关机结束 （下放阶段 ON DP 开始 OFF DP结束，回收阶段 A架开机开始 A架关机结束 ）
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTime >= "2024-08-24 00:00:00" and csvTime <= "2024-08-24 23:59:59" and (actionType = "下放" or  actionType = "回收");
    ```

    2024/5/23 下午 什么动作同时发生？
    /**
       当询什么动作同时发生，或者询问什么时间点发生什么动作，需要先将这个时间段或者 这个时间点前后一小的时间段内的数据取出
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-23 12:00:00" and csvTime <= "2024-05-23 23:59:59" and (actionType = "下放" or  actionType = "回收");
    ```

    2024/5/25 下午16点54分 什么动作同时发生？
    /**
       如果是下放过程，需要DP是一个持续发生过程，16点54分有可能无法查询到动作的状态发生，所有我们先 这个时间点前后一小的时间段内的数据取出 (DP 过程一定是需要补充 (actionType = "下放" or  actionType = "回收") 田间 )
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-25 15:54:00" and csvTime <= "2024-05-25 17:54:59" and (actionType = "下放" or  actionType = "回收");
    ```

    2024/5/25 下午1点~2点 A架是否启动？
    /**
       这是一个判断 设备状态的问题，需要扩大 问题的边界进行查询
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-25 12:00:00" and csvTime <= "2024-05-25 15:00:00" and ( deviceName = "A架");
    ```

    2024/05/01 A架运行时间？
    /**
    注意 询问设备运行时间则判断动作 开机/关机时间 （DP 为 ON DP /OFF DP 并且  (actionType = "下放" or  actionType = "回收")）,对应关系,A架开机/A架关机, 折臂吊车开机/折臂吊车关机
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-01 00:00:00" and csvTime <= "2024-05-01 23:59:59" and (actionName ="A架开机" or actionName ="A架关机");
    ```

    """.format(action_schema=action_schema)

    if "之后" in question:
        zhihou_prompt = """
        2024/xx/xx A架开机之后，发生了什么动作(关键动作)
        /**
         注意 由于A架开机动作时间未知，需要返回单天所有的的动作，再进行判断
        **/
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-23 12:00:00" and csvTime <= "2024-05-23 23:59:59";
        ```
        注意，如果没有特殊说明 某个时间点之后 还是在这一天，例如 17:00之后 则 csvTime >= "2024-05-23 17:00:00" and csvTime <= "2024-05-23 23:59:59";
        """
        system_prompt = system_prompt + zhihou_prompt

    if ("摆动" in question) or ("摆次" in question):
        baidong_prompt = """
        2024/xx/xx 和 2024/yy/yy 总共摆动了多少次
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from ((csvTime >= "2024-xx-xx 00:00:00" and csvTime <= "2024-xx-xx 23:59:59") or (csvTime >= "2024-yy-yy 00:00:00" and csvTime <= "2024-yy-yy 23:59:59")) and ( actionName = "A架摆出" or actionName = "A架摆回"  );
        ```
        """
        system_prompt = system_prompt + baidong_prompt

    if (("到" in question) or (("-" in question) and ("/" in question)) or (
        ("-" in question) and ("~" in question))) and (
        ("前" in question) or ("之前" in question) or ("之后" in question) or ("后" in question)) and (
        ("比例" in question) or ("占比" in question) or ("次数" in question)):
        rank_prompt = """
        有问题形式如下：
        2024/xx/xx-2024/xx/xx (XX设备)在 (xx时间/xx动作) 之前进行(xx动作)的（比例/占比/次数）
        如：
        2024/xx/xx ~ 2024/yy/yy A架在08:00之前开机的占比
        /**
        注意： n点之前 条件考虑 csvTime <= n:00:59 例如 ==》 8点前 csvTime <= 08:00:59
        **/
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTime >= "2024-xx-xx 00:00:00" and csvTime <= "2024-xx-xx 23:59:59") and ( RIGHT(csvTimeMinute,5) < "08:00" ) and ( actionName = "A架开机" );
        ```

        如：
        2024/xx/xx ~ 2024/yy/yy 征服者在小艇落座之后落座的次数
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTime >= "2024-xx-xx 00:00:00" and csvTime <= "2024-xx-xx 23:59:59") and ( actionName = "小艇落座" or actionName = "征服者落座");
        ```

        2024/xx/xx ~ 2024/yy/yy A架摆出之后小艇检查完毕的次数
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTime >= "2024-xx-xx 00:00:00" and csvTime <= "2024-xx-xx 23:59:59") and ( actionName = "A架摆出" or actionName = "小艇检查完毕");
        ```
        """
        logger.info("使用RankPrompt")
        system_prompt = system_prompt + rank_prompt

    user_prompt = """
        用户问题：
        {question}
        要求：
        生成sql,生成sql,生成sql,生成sql 不解释不说明，不回答问题，直接返回sql，注意是区域段，
        1、如果提问是时间段查询时间段内信息。
        2、禁止 DATE_FORMAT、 limit , order by 和 sql嵌套，仅返回一条sql。
        3、返回 csvTimeMinute,csvTime,actionName,deviceName,actionType 字段，并且不能对返回字段进行处理。
        4、日期字段，参考的时间筛选条件为 ： {date_str}
        5、注意多天时间的提问，需要澄清条件的注意例如  01/02~01/03下午16点后A架关机的比例 澄清条件为： 01/02~01/03 每一天下午16点后A架关机的比例
        """
    if ("DP" in question) or ("深海" in question) or ("作业" in question):
        dp = """6、增加  (actionType = "下放" or  actionType = "回收")  的筛选条件"""
        user_prompt = user_prompt + dp
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt.format(question=question, date_str=date_str)}
    ]
    response = llm_invoke(messages)
    sql = check_sql(response)
    sql = rewrite_sql(question, sql)
    return messages,sql


def fuzzy_action_sql(question,date_str=None):
    if date_str is None:
        date_str = duration_parser(question)
        date_str = sql_to_str(date_str)
    action_schema = """
    CREATE TABLE `task_action` (
          `index` bigint DEFAULT NULL COMMENT '索引',
          `csvTimeMinute` text COMMENT '动作发生时间，精确到分钟',
          `actionName` text COMMENT '动作名称,判断动作，值可取OFF DP、折臂吊车关机、折臂吊车开机、ON DP、A架关机、A架开机、小艇检查完毕、小艇入水、征服者起吊、征服者入水、缆绳解除、A架摆回、小艇落座、A架摆出、缆绳挂妥、征服者出水、征服者落座。举例：A架关机',
          `deviceName` text COMMENT '设备名称,动作涉及的器械，仅可取 折臂吊车、A架、DP，举例：折臂吊车',
          `actionType` text COMMENT '动作类型,动作涉及的深海作业A过程,值取下放和回收，举例：下放',
          `csvTime` text COMMENT '动作发生时间，精确到秒，举例：2024-05-16 23:49:01',
          KEY `ix_task_action_index` (`index`) COMMENT '索引键'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        /**
        描述：该表存储了深海作业A中的动作记录，包括动作名称、设备名称、动作类型及时间戳等信息。这些动作涉及A架、绞车、折臂吊车等设备的下放和回收操作。
        数据案例如下：
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |   index | csvTimeMinute    | actionName   | deviceName   | actionType   | csvTime             |
        +=========+==================+==============+==============+==============+=====================+
        |     157 | 2024-05-29 08:53 | 征服者入水   | A架          | 下放         | 2024-05-29 08:53:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |     158 | 2024-05-29 08:54 | 缆绳解除     | A架          | 下放         | 2024-05-29 08:54:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |     159 | 2024-05-29 09:00 | A架摆回      | A架          | 下放         | 2024-05-29 09:00:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        **/
        """
    system_prompt = """你是一名数据库专家，你现在需要根据问题，提取出深海作业相关的数据，并进行分析：
    数据表结构如下：
        {action_schema}
    你需要根据表结构生成数据提取sql,注意sql查询的时间范围，你需要尽可能囊括出时间范围，注意边界时间，由于字段为字符串，
    未提及年份默认为2024
    不要使用 时间处理函数；
    不要使用 LIMIT,ORDER BY；
    数据查询开始时间 不要和结束时间相同；
    例如：
    2024/8/24 深海作业A作业开始的时间（请以XX:XX输出）？
    /**
    该问题需要查询 2024年08月24日 深海作业A的开始时间，数据的查询段为该天所有数据, 深海作业A作业开始的时间 以 ON DP 开始 以 A架关机结束 （下放阶段 ON DP 开始 OFF DP结束，回收阶段 A架开机开始 A架关机结束 ）
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTime >= "2024-08-24 00:00:00" and csvTime <= "2024-08-24 23:59:59" and (actionType = "下放" or  actionType = "回收") and deviceName = "DP";
    ```

    2024/5/23 下午 什么动作同时发生？
    /**
       当询什么动作同时发生，或者询问什么时间点发生什么动作，需要先将这个时间段或者 这个时间点前后一小的时间段内的数据取出
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-23 12:00:00" and csvTime <= "2024-05-23 23:59:59" and (actionType = "下放" or  actionType = "回收");
    ```

    2024/5/25 下午16点54分 什么动作同时发生？
    /**
       如果是下放过程，需要DP是一个持续发生过程，16点54分有可能无法查询到动作的状态发生，所有我们先 这个时间点前后一小的时间段内的数据取出 (DP 过程一定是需要补充 (actionType = "下放" or  actionType = "回收") )
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-25 15:54:00" and csvTime <= "2024-05-25 17:54:59" and (actionType = "下放" or  actionType = "回收");
    ```

    2024/1/25 下午1点~2点 A架是否启动？
    /**
       这是一个判断 设备状态的问题，需要扩大 问题的边界进行查询
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-01-25 12:00:00" and csvTime <= "2024-01-25 15:00:00" and ( deviceName = "A架");
    ```

    2024/05/01 13:15 发生了什么动作？
    /**
    这个是根据分钟级的 时间查询动作表中的数据
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-01 13:15:00" and csvTime <= "2024-05-01 13:15:59"
    ```

    """.format(action_schema=action_schema)

    if "之后" in question:
        zhihou_prompt = """
        2024/xx/xx A架开机之后，发生了什么动作(关键动作)
        /**
         注意 由于A架开机动作时间未知，需要返回单天所有的的动作，再进行判断
        **/
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTimeMinute >= "2024-05-23 12:00" and csvTimeMinute <= "2024-05-23 23:59";
        ```

        """
        system_prompt = system_prompt + zhihou_prompt

    if ("摆动" in question) or ("摆次" in question):
        baidong_prompt = """
        2024/xx/xx 和 2024/yy/yy 总共摆动了多少次
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from ((csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 23:59") or (csvTimeMinute >= "2024-yy-yy 00:00" and csvTimeMinute <= "2024-yy-yy 23:59")) and ( actionName = "A架摆出" or actionName = "A架摆回"  );
        ```
        """
        system_prompt = system_prompt + baidong_prompt

    if (("到" in question) or (("-" in question) and ("/" in question)) or (
        ("-" in question) and ("~" in question))) and (
        ("前" in question) or ("之前" in question) or ("之后" in question) or ("后" in question)) and (
        ("比例" in question) or ("占比" in question) or ("次数" in question)):
        rank_prompt = """
        有问题形式如下：
        2024/xx/xx-2024/xx/xx (XX设备)在 (xx时间/xx动作) 之前进行(xx动作)的（比例/占比/次数）
        如：
        2024/xx/xx ~ 2024/yy/yy A架在08:00之前开机的占比
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 23:59") and ( actionName = "A架开机" );
        ```

        如：
        2024/xx/xx ~ 2024/yy/yy 征服者在小艇落座之后落座的次数
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 23:59") and ( actionName = "小艇落座" or actionName = "征服者落座");
        ```

        2024/xx/xx ~ 2024/yy/yy A架摆出之后小艇检查完毕的次数
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 23:59") and ( actionName = "A架摆出" or actionName = "小艇检查完毕");
        ```
        """
        logger.info("使用RankPrompt")
        system_prompt = system_prompt + rank_prompt

    if "作业" in question:
        zuoye_prompt = """
        20XX-XX-XX ~ 20XX-XX-XX 作业A开始时间
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTime >= "20xx-xx-xx 00:00:00" and csvTime <= "20xx-xx-xx 23:59:59") and actionName="ON DP";
        ```

        20XX-XX-XX ~ 20XX-XX-XX 作业A在7点前开始的时间
        ```sql
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTime >= "20xx-xx-xx 00:00:00" and csvTime <= "20xx-xx-xx 23:59:59") and actionName="ON DP";
        ```
        """
        system_prompt = system_prompt + zuoye_prompt

    user_prompt = """
        用户问题：
        {question}
        要求：
        生成sql 不解释不说明，不回答问题，直接返回sql，注意是区域段，
        不要试图用sql解决全部问题，只是生成数据查询sql
        1、如果提问是时间段查询时间段内信息。
        2、如果提问是时间点，查询时间点前后一小时。
        3、不使用limit , order by 和 sql嵌套。
        4、返回 csvTimeMinute,csvTime,actionName,deviceName,actionType 字段，并且不能对返回字段进行处理。
        5、日期字段，可以参考的时间筛选条件为 ： {date_str}
        """
    if ("DP" in question) or ("深海" in question) or ("作业" in question):
        dp = """6、深海作业A问题增加  (actionType = "下放" or  actionType = "回收")  的筛选条件
        7、深海作业A开始时间问题， 增加 (actionName="ON DP") 的筛选条件，例如： 2024年01月01日开始作业时间 筛选条件为： actionName="ON DP"
        8、深海作业A结束时间问题，增加 (actionName="A架关机") 的筛选条件，例如： 2024年01月01日开始作业时间 筛选条件为： actionName="A架关机"
        9、深海作业A下放阶段结束时间问题，增加 (actionName="OFF DP") 的筛选条件，例如： 2024年01月01日下放阶段结束时间 筛选条件为： actionName="OFF DP"
        10、深海作业A回收阶段结束时间问题，增加 (actionName="A架开机") 的筛选条件，例如： 2024年01月01日回收阶段开始时间 筛选条件为： actionName="A架开机"
        """
        user_prompt = user_prompt + dp
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt.format(question=question, date_str=date_str)}
    ]
    response = llm_invoke(messages)
    sql = check_sql(response)
    sql = rewrite_sql(question, sql)
    return messages,sql


def duration_sql(question,date_str=None):
    if date_str is None:
        date_str = duration_parser(question)
    # date_str = duration_parser(question)
    action_schema = """
    CREATE TABLE `task_action` (
          `index` bigint DEFAULT NULL COMMENT '索引',
          `csvTimeMinute` text COMMENT '动作发生时间，精确到分钟',
          `actionName` text COMMENT '动作名称,判断动作，值可取OFF DP、折臂吊车关机、折臂吊车开机、ON DP、A架关机、A架开机、小艇检查完毕、小艇入水、征服者起吊、征服者入水、缆绳解除、A架摆回、小艇落座、A架摆出、缆绳挂妥、征服者出水、征服者落座。举例：A架关机',
          `deviceName` text COMMENT '设备名称,动作涉及的器械，值可取折臂吊车、A架、DP，举例：折臂吊车',
          `actionType` text COMMENT '动作类型,动作涉及的深海作业A过程,值取下放和回收，举例：下放',
          `csvTime` text COMMENT '动作发生时间，精确到秒，举例：2024-05-16 23:49:01',
          KEY `ix_task_action_index` (`index`) COMMENT '索引键'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        /**
        描述：该表存储了深海作业A中的动作记录，包括动作名称、设备名称、动作类型及时间戳等信息。这些动作涉及A架、绞车、折臂吊车等设备的下放和回收操作。
        数据案例如下：
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |   index | csvTimeMinute    | actionName   | deviceName   | actionType   | csvTime             |
        +=========+==================+==============+==============+==============+=====================+
        |     157 | 2024-05-29 08:53 | 征服者入水   | A架          | 下放         | 2024-05-29 08:53:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |     158 | 2024-05-29 08:54 | 缆绳解除     | A架          | 下放         | 2024-05-29 08:54:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        |     159 | 2024-05-29 09:00 | A架摆回      | A架          | 下放         | 2024-05-29 09:00:48 |
        +---------+------------------+--------------+--------------+--------------+---------------------+
        **/
        """
    system_prompt = """你是一名数据库专家，你现在需要根据问题，提取出深海作业相关的数据，并进行分析：
    数据表结构如下：
        {action_schema}
    你需要根据表结构生成数据提取sql,注意sql查询的时间范围，你需要尽可能囊括出时间范围，注意边界时间，由于字段为字符串，
    未提及年份默认为2024
    不要使用 时间处理函数；
    不要使用 LIMIT,ORDER BY；
    数据查询开始时间 不要和结束时间相同；
    例如：
    2024/8/24 深海作业A作业开始的时间（请以XX:XX输出）？
    /**
    该问题需要查询 2024年08月24日 深海作业A的开始时间，数据的查询段为该天所有数据, 深海作业A作业开始的时间 以 ON DP 开始 以 A架关机结束 （下放阶段 ON DP 开始 OFF DP结束，回收阶段 A架开机开始 A架关机结束 ）
    **/
    ```sql
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from task_action where csvTime >= "2024-08-24 00:00:00" and csvTime <= "2024-08-24 23:59:59" and (actionType = "下放" or  actionType = "回收");
    ```

    2024/5/23 下午 什么动作同时发生？
    /**
       当询什么动作同时发生，或者询问什么时间点发生什么动作，需要先将这个时间段或者 这个时间点前后一小的时间段内的数据取出
    **/
    ```
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-23 12:00:00" and csvTime <= "2024-05-23 23:59:59" and (actionType = "下放" or  actionType = "回收");
    ```

    2024/5/25 下午16点54分 什么动作同时发生？
    /**
       如果是下放过程，需要DP是一个持续发生过程，16点54分有可能无法查询到动作的状态发生，所有我们先 这个时间点前后一小的时间段内的数据取出 (DP 过程一定是需要补充 (actionType = "下放" or  actionType = "回收") 田间 )
    **/
    ```
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-25 15:54:00" and csvTime <= "2024-05-25 17:54:59" and (actionType = "下放" or  actionType = "回收");
    ```

    2024/5/25 下午1点~2点 A架是否启动？
    /**
       这是一个判断 设备状态的问题，需要扩大 问题的边界进行查询
    **/
    ```
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-25 12:00:00" and csvTime <= "2024-05-25 15:00:00" and ( deviceName = "A架");
    ```

    2024/05/01 A架运行时间？
    /**
    注意 询问设备运行时间则判断动作 开机/关机时间 （DP 为 ON DP /OFF DP 并且  (actionType = "下放" or  actionType = "回收")）,对应关系,A架开机/A架关机, 折臂吊车开机/折臂吊车关机
    **/
    ```
    SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTimeMinute >= "2024-05-01 00:00" and csvTimeMinute <= "2024-05-01 23:59" and (actionName ="A架开机" or actionName ="A架关机");
    ```

    """.format(action_schema=action_schema)

    if "之后" in question:
        zhihou_prompt = """
        2024/xx/xx A架开机之后，发生了什么动作(关键动作)
        /**
         注意 由于A架开机动作时间未知，需要返回单天所有的的动作，再进行判断
        **/
        ```
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from csvTime >= "2024-05-23 12:00:59" and csvTime <= "2024-05-23 23:59:59";
        ```

        """
        system_prompt = system_prompt + zhihou_prompt

    if ("摆动" in question) or ("摆次" in question):
        baidong_prompt = """
        2024/xx/xx 和 2024/yy/yy 总共摆动了多少次
        ```
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from ((csvTime >= "2024-xx-xx 00:00:00" and csvTime <= "2024-xx-xx 23:59:59") or (csvTime >= "2024-yy-yy 00:00" and csvTime <= "2024-yy-yy 23:59:59")) and ( actionName = "A架摆出" or actionName = "A架摆回"  );
        ```
        """
        system_prompt = system_prompt + baidong_prompt

    if (("到" in question) or (("-" in question) and ("/" in question)) or (
        ("-" in question) and ("~" in question))) and (
        ("前" in question) or ("之前" in question) or ("之后" in question) or ("后" in question)) and (
        ("比例" in question) or ("占比" in question) or ("次数" in question)):
        rank_prompt = """
        有问题形式如下：
        2024/xx/xx-2024/xx/xx (XX设备)在 (xx时间/xx动作) 之前进行(xx动作)的（比例/占比/次数）
        如：
        2024/xx/xx ~ 2024/yy/yy A架在08:00之前开机的占比
        ```
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 23:59") and ( RIGHT(csvTimeMinute,5) < "08:00" ) and ( actionName = "A架开机" );
        ```

        如：
        2024/xx/xx ~ 2024/yy/yy 征服者在小艇落座之后落座的次数
        ```
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 23:59") and ( actionName = "小艇落座" or actionName = "征服者落座");
        ```

        2024/xx/xx ~ 2024/yy/yy A架摆出之后小艇检查完毕的次数
        ```
        SELECT csvTimeMinute,csvTime,actionName,deviceName,actionType from (csvTimeMinute >= "2024-xx-xx 00:00" and csvTimeMinute <= "2024-xx-xx 23:59") and ( actionName = "A架摆出" or actionName = "小艇检查完毕");
        ```
        """
        logger.info("使用RankPrompt")
        system_prompt = system_prompt + rank_prompt

    user_prompt = """
        用户问题：
        {question}
        要求：
        生成sql 不解释不说明，不回答问题，直接返回sql，注意是区域段，
        1、如果提问是时间段查询时间段内信息。
        2、如果提问是时间点，查询时间点前后一小时。
        3、不使用limit , order by 和 sql嵌套。
        4、返回 csvTimeMinute,csvTime,actionName,deviceName,actionType 字段，并且不能对返回字段进行处理。
        5、日期字段，参考的时间筛选条件为 ： {date_str}
        """
    if ("DP" in question) or ("深海" in question) or ("作业" in question):
        dp = """6、增加  (actionType = "下放" or  actionType = "回收")  的筛选条件"""
        user_prompt = user_prompt + dp
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt.format(question=question, date_str=date_str)}
    ]
    response = llm_invoke(messages)
    sql = check_sql(response)
    sql = rewrite_sql(question, sql)
    return messages,sql



def device_sql(question,date_str=None):
    database_info = question_key_match(question)
    if date_str is None:
        date_str = duration_parser(question)
    system_prompt = f"""
        你是一名数据库专家，你能根据用户的sql语句进行pandas.read_sql 进行数据查询，并根据数据结果回答问题，注意大小写：
        {database_info}
        请你根据 数据库信息 生成 sql 查询语句
        未提及年份默认为2024
        案例如下：
            在2024年9月8日4时29分，一号柴油发电机组的转速是多少？ 
            /**
            注意数据表 csvTime 的数据粒度到秒的结果
            **/
            ```
            SELECT P1_2 FROM Port1_ksbg_1 WHERE csvTime >= '2024-09-08 04:29:00' and csvTime <= '2024-09-08 04:29:59'
            ```

            在2024年9月3日7时28分艏推(侧推 or 艏侧推)主开关电流是多少？ 
            ```
            SELECT P1_80 FROM Port1_ksbg_4 WHERE csvTime >= '2024-09-03 07:28:00' and csvTime <= '2024-09-03 07:28:59'
            ```
    """

    if ("A架" in question) and ("角度" in question):
        data_angle_prompt = """
        案例：
        案例说明： 查找某个时间段A架角度数据，需要特别注意角度数据负数是外摆角度，正数是内置角度, 注意需要limit 20(可以探查top20的时间，可能需要计算角度持续时长)
        问题:
        2024-01-01 A架外摆最大角度是多少？
        /**
        注意一定要 limit 20
        **/
        ```
        SELECT  `Ajia-0_v`, `Ajia-1_v`, `Ajia-3_v`, `Ajia-5_v`, `csvTime`   FROM `Ajia_plc_1` where csvTime >= "2024-01-01 00:00:00" and csvTime >= "2024-01-01 23:59:59" and `Ajia-0_v` <0 and `Ajia-1_v` < 0  ORDER BY abs(`Ajia-0_v` + `Ajia-1_v`) DESC limit 20
        ```
        """
        system_prompt += data_angle_prompt

    if ("A架" in question) and ("数据" in question) and ("异常" in question):
        data_error_prompt = """
        案例：
        案例说明： 只有A架数据异常判断和A架角度异常判断采用下列sql形式（精度到天级）
        1、只要是判断角度异常情况 sql 返回字段为 日期和状态，不要进行其他操作
        问题：
        2024/xx/xx A架数据是否异常
        ```sql
        SELECT
            DATE(`csvTime`) AS `日期`,
            CASE
                WHEN COUNT(CASE WHEN `Ajia-0_v` > 0 THEN 1 END) > 0
                     AND COUNT(CASE WHEN `Ajia-0_v` < 0 AND `Ajia-0_v` != -1 THEN 1 END) > 0
                     AND COUNT(CASE WHEN `Ajia-1_v` > 0 THEN 1 END) > 0
                     AND COUNT(CASE WHEN `Ajia-1_v` < 0 AND `Ajia-1_v` != -1 THEN 1 END) > 0 THEN '正常'
                ELSE '异常'
            END AS `状态`
        FROM
            `Ajia_plc_1`
        WHERE
            `csvTime` >= '2024-xx-xx 00:00:00'
            AND `csvTime` <= '2024-xx-xx 23:59:59'
            AND `Ajia-3_v` > 60
            AND `Ajia-5_v` > 60
        GROUP BY
            DATE(`csvTime`);
        ```
        
        数据库表中有A架角度数据异常，请告诉我是哪一天开始哪一天结束（sql尽量保持以下格式）
        ```sql
SELECT
    DATE(`csvTime`) AS `日期`,
    CASE
        WHEN COUNT(CASE WHEN `Ajia-0_v` != -1 THEN 1 END) = 0
             AND COUNT(CASE WHEN `Ajia-1_v` != -1 THEN 1 END) = 0 THEN '异常'
        WHEN COUNT(CASE WHEN `Ajia-0_v` > 0 THEN 1 END) > 0
             AND COUNT(CASE WHEN `Ajia-0_v` < 0 AND `Ajia-0_v` != -1 THEN 1 END) > 0
             AND COUNT(CASE WHEN `Ajia-1_v` > 0 THEN 1 END) > 0
             AND COUNT(CASE WHEN `Ajia-1_v` < 0 AND `Ajia-1_v` != -1 THEN 1 END) > 0 THEN '正常'
        ELSE '异常'
    END AS `状态`
FROM
    `Ajia_plc_1`
GROUP BY
    DATE(`csvTime`);
        ```
        """
        system_prompt = system_prompt + data_error_prompt

    if ("A架" in question) and ("实际" in question) and (("时长" in question) or ("多久" in question) or ("开机效率" in question)):
        extend_prompt = """
        问题：2024-01-01 A架实际开机时间（注意关键字“实际”）
        /**
        案例说明： 返回该天的角度电流数据
        **/
        ```sql
        SELECT `Ajia-3_v`,`Ajia-5_v`,`csvTime`  FROM `Ajia_plc_1` where csvTime >= "2024-01-01 00:00:00" and csvTime >= "2024-01-01 23:59:59"
        ```
        """
        system_prompt = system_prompt + extend_prompt
    user_prompt = f"""
    请生成查询问题区域段的SQL语句，注意字段 csvTime 是字符串,生成sql 不解释不说明，直接返回sql结果，
    注意事项：
        1、可以参考 时间条件： {date_str}
        2、生成的 sql 多参考 案例说明, 不要使用 函数，直接返回原始数据字段
        3、不要使用 CAST 等各种函数对原始字段处理，直接返回原始字段值
        4、注意设备数据查询，均要带上csvTime字段，字段均需要带上反撇号(`字段`)
    问题：
    {question}
    涉及到 Ajia_plc_1 数据 都需要添加过滤条件 (`Ajia-3_v`  != -1 )
    """
    if ("A架" in question) and ("实际" in question) and (("时长" in question) or ("多久" in question) or ("开机效率" in question)):
        user_prompt += """
        A架实际数据需要 返回字段   `Ajia-3_v`,`Ajia-5_v`,`csvTime`
        """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    response = llm_invoke(messages)
    sql = check_sql(response)
    return database_info,messages,sql
def deduplicate_strict(data):
    seen = set()
    result = []
    for d in data:
        # 处理列表转为元组，保持顺序
        normalized = {k: tuple(v) if isinstance(v, list) else v for k, v in d.items()}
        # 生成排序后的键（避免键顺序影响）
        key = tuple(sorted(normalized.items()))
        if key not in seen:
            seen.add(key)
            result.append(d)
    return result

j_match = re.compile("""```json\n([\s\S]*?)\n```""")
def energy_table_match(question):
    system_prompt = """你是一名设备装置识别专家，你能根据用户的问题拆分出要进行计算的对应的的设备:
    已知 甲板设备包括： A架（一号门架和二号门架合起来称为A架），绞车，折臂吊车
         推进设备包括： 一号推进器，二号推进器，艏推(又叫艏侧推、侧推)，可伸缩推
         舵桨包括： 一号舵桨转舵A、一号舵桨转舵B、二号舵桨转舵A、二号舵桨转舵B
         发电机： 一号柴油发电机,二号柴油发电机,三号柴油发电机,四号柴油发电机
                停泊/应急发电机
    注意设备的能耗修饰的主语。
    返回结构中 type  只能取 功，油耗和理论发电量
    发电效率的计算是 实际发电量/理论发电量， 默认不特殊说明的发电量为实际发电量
    例如 xxxx/xx/xx 折臂吊车的能耗占甲板机械设备的比例？
    ```json
    [{"device":["折臂吊车","A架","绞车"],"type":"功"}]
    ```

    例如 xxxx/xx/xx 发电机组发电量是多少？
    ```json
    [{"device":["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"功"}]
    ```

    例如 xxxx/xx/xx 推进设备能耗是多少？
    /**
    注意 侧推、艏侧推、艏推、艏推推进器，都为 艏推，
    **/
    ```json
    [{"device": ["一号推进器","二号推进器","艏推","可伸缩推"],"type":"功"}]
    ```

    例如 xxxx/xx/xx 侧推过程中，发电机发电量是多少（单位化成kWh，保留2位小数）？
    /**
    注意 在xxx 过程中的表，不参与发电量 油耗 能量耗计算
    **/
    ```json
    [{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"功"}]
    ```

    例如 xxxx/xx/xx 甲板设备能耗占总发电量的比例？
    ```json
    [{"device": ["折臂吊车","A架","绞车"],"type":"功"},{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"功"}]
    ```

    例如 xxxx/xx/xx 当天的发电量和油耗分别是多少？
    ```json
    [{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"功"},{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"油耗"}]
    ```

    例如 xxxx/xx/xx 总能耗是多少？、xx日~xx日 平均能耗是多少
    /**未说明设备能耗的问题，则计算发电机发电量**/
    ```json
    [{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"功"}]
    ```

    例如：
    ```
    历史信息：
    {"子问题"："2024/xx/xx 征服者什么时候落座","回答":"2024/xx/xx 征服者落座在2024-xx-xx aa:aa:aa"}
    {"子问题"："2024/xx/xx 征服者落座后A架什么时候关机","回答":"2024/xx/xx 征服者落座后A架在bb:bb:bb关机"}  
    ```
    2024-xx-xx 征服者落座后A架关机的这段时间 折臂吊车的能耗是多少？
    /**
    能耗修饰的主语是折臂吊车，所以能耗计算设备是 折臂吊车 
    **/
    ```json
    [{"device": ["折臂吊车"],"type":"功"}]
    ```
    """
    if (("应急发电机" in question) and ("油耗" in question)) or (("停泊发电机" in question) and ("油耗" in question)):
        extend_promot = """
        例如：
        2024-xx-xx 发电机组的油耗是多少 （包含停泊发电机）？
        /**
        除非特殊说明否则不包括 应急发电机 
          (停泊/应急发电机只有油耗和转速数据 没有做功数据)
        **/
        ```json
        [{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机","停泊/应急发电机"],"type":"油耗"}]
        ```
        注意： 发电量和油耗 device 都为发电机，不要被问题中出现的其他设备干扰，例如：
        问题：
        2024年x月xx日折臂吊车第一次布放开始时间为08:08:58，结束时间为09:00:17，持续时间为51分钟；
        第二次布放开始时间为16:04:28，结束时间为17:24:44，持续时间为80分钟；
        第三次布放开始时间为18:47:01，结束时间为22:31:14，持续时间为274分钟。请计算每次布放阶段的燃油消耗量。
        ```json
        [{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"油耗"}]
        ```
        同样 发电量也是如此
        """
        system_prompt += extend_promot

    if ("理论发电量" in question) or ("发电效率" in question):
        extend_promot = """
        例如：
        2024-xx-xx 发电机组的发电效率是多少？
        /**
        发电效率是 实际发电量/理论发电量
        **/
        ```json
        [{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"功"},{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"理论发电量"}]
        ```

        例如：
        2024-xx-xx 发电机组的理论发电量是多少？
        /**
        理论发电量 如果未提及 则默认值为 "油密度":0.8448,"油热值":42.6
        **/
        ```json
        [{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"理论发电量","油密度":0.8448,"油热值":42.6}]
        ```

        例如：
        假设柴油的密度为0.8448kg/L，柴油热值为42.6MJ/kg，请计算2024/8/xx 0:00 ~ 2024/8/yy 0:00柴油机的发电效率（%，保留2位小数）？
        /**
        返回密度和热值
        **/
        ```json
        [{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"功"},{"device": ["一号柴油发电机","二号柴油发电机","三号柴油发电机","四号柴油发电机"],"type":"理论发电量","油密度":0.8448,"油热值":42.6}]
        ```
        """
        system_prompt += extend_promot

    user_prompt = """
    请根据用户返回相关设备及设备类型，不解释不说明
    =====以下为用户问题=========
    {question}
    =====用户提问结束==========
    注意名称标准化：
    ====
        A架，绞车，折臂吊车
        一号推进器、二号推进器、艏推、可伸缩推
        一号舵桨转舵A、一号舵桨转舵B、二号舵桨转舵A、二号舵桨转舵B
        一号柴油发电机、二号柴油发电机、三号柴油发电机、四号柴油发电机、停泊/应急发电机
    ====
    1、device 字段中必须使用上述名称，请注意 字母大小写、别名、中文数字号 不要写错
    2、type 选项：功、能耗、油耗，不允许有其他选项
    3、理论发电量中需要包含 "油热值" 和 "油密度" 两个字段
    4、不解释不说明，不追问不啰嗦，直接返回json结果
    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt.format(question=question)}
    ]
    response = llm_invoke(messages)
    match_value = j_match.findall(response)
    logger.info("设备识别结果："+response)
    try:
        table_about = json.loads(match_value[0])
    except Exception as e:
        raise ValueError("生成内容无法进行json解析" + e)
    logger.info("识别出相关设备：" + str(table_about))
    if len(table_about) > 0 :
        table_about = deduplicate_strict(table_about)
    return table_about



def device_compute(device_name,type_value_base,time_list):
    result = []
    type_value_query = type_value_base
    if type_value_base == "理论发电量":
        type_value_query = "油耗"
    if device_name == "A架":
        table_info = energy_table_dict["一号门架"+"-"+type_value_query]
        tbname = table_info["表"]
        fieldname = table_info["字段"]
        for time_condition in time_list:
            logger.info(time_condition)
            sql = "select " + fieldname + " from " + tbname + " where " + time_condition
            result.append({"desc":"A架-一号门架 "+ type_value_base + " 时间 " + time_condition,"sql":sql,"type":type_value_base})

        table_info = energy_table_dict["二号门架"+"-"+type_value_query]
        tbname = table_info["表"]
        fieldname = table_info["字段"]
        for time_condition in time_list:
            sql = "select " + fieldname + " from " + tbname + " where " + time_condition
            result.append({"desc":"A架-二号门架 "+ type_value_base + " 时间 " + time_condition,"sql":sql,"type":type_value_base})
    else:
        cu_type = device_name+ "-" + type_value_query
        if cu_type in ["停泊/应急发电机-功"]:
            return result
        table_info = energy_table_dict[cu_type]
        tbname = table_info["表"]
        fieldname = table_info["字段"]
        for time_condition in time_list:
            sql = "select " + fieldname + " from " + tbname + " where " + time_condition
            result.append({"desc":device_name + " "+ type_value_base + " 时间 " + time_condition,"sql":sql,"type":type_value_base})
    return result


def total_device_sql(action_list,time_list):
    result = []
    for item in action_list:
        type_value = item["type"]
        temp = []
        for idevice in item["device"]:
            sqllist = device_compute(idevice,type_value,time_list)
            if len(sqllist) != 0:
                temp += sqllist

        if type_value == "理论发电量":
            for v in temp:
                v["油热值"] = item["油热值"]
                v["油密度"] = item["油密度"]
        result.append(temp)
    # 我需要对 result 去重
    return result


def sql_energy(sql,type_value,conn,midu=0.0,rezhi=0.0,):
    if type_value == "理论发电量":
        dfk = pd.read_sql(sql,con=conn)
        dfk["diff"] = (pd.to_datetime(dfk["csvTime"]).shift(-1) - pd.to_datetime(dfk["csvTime"])).dt.seconds.fillna(0.00)
        # dfk.loc[dfk["diff"] > 3600.000,"diff"] = 0.000
        value = (dfk["v"]/3600. * dfk["diff"]*midu*rezhi).sum()/3.6
        return value
    else:
        dfk = pd.read_sql(sql,con=conn)
        dfk["diff"] = (pd.to_datetime(dfk["csvTime"]).shift(-1) - pd.to_datetime(dfk["csvTime"])).dt.seconds.fillna(0.00)
        value = (dfk["v"] * dfk["diff"]).sum()/3600.000
        return value


def list_desc(x):
    value_parser = "总量为:" + str(np.sum(x)) + "最大值: " + str(np.max(x)) + " 最小值:" + str(np.min(x)) + " 平均值:" + str(np.mean(x))
    return {"子问题描述":"计算列表：" + str(x),"计算得到结果":value_parser}




def llm_struct(question):
    system_prompt = """
    你能将问题整理生成模板，你需要转化的内容如下：
    1、时间
    2、深海作业A动作：OFF DP、折臂吊车关机、折臂吊车开机、ON DP、A架关机、A架开机、小艇检查完毕、小艇入水、征服者起吊、征服者入水、缆绳解除、A架摆回、小艇落座、A架摆出、缆绳挂妥、征服者出水、征服者落座。
    3、将设备模板化为设备： 例如 A架、折臂吊车、绞车、。。。
    4、用一阶逻辑整理问题
    5、补充问题是什么种类问题
    已知问题类型如下：
        "动作数据查询":  "根据动作查询时间点，深海作业A的动作时刻查询问题，需要要明确动作或者明确设备，例如：xxxx/xx/xx A架第一次开机时间在什么时候？ xxxx/xx/xx 缆绳解除的时间点是什么时候？,动作包括："A架开机", "A架摆出", "征服者起吊", "征服者入水", "征服者出水", "A架关机", "A架摆回","折臂吊车开机", "折臂吊车关机", "小艇入水", "缆绳解除", "小艇落座", "缆绳挂妥","ON DP","OFF DP" ",
        "设备数据查询":"具体设备在某个时刻或者个时间段内的数值情况，设备包括，推进器，侧推（艏推或者艏侧推），绞车，A架（一号二号门架），折臂吊车，发电机，舵桨 问题中一定会包含具体时间，主要是各种设备各个时刻数据指标状态，指标包括压力，电流电压，频率Hz，功率，相位，燃油。例如：在XXXX年XX月XX日XX时XX分，X号柴油发电机组燃油消耗率是多少？",
        "时长处理问题": "动作相隔时间判断问题，包括各类时长比较计算，例如：xxxx/xx/xx A架开机和关机时间间隔多久？, xxxx/xx/xx A架运行时长多长,xx/xx~xx/xx 平均时长, 包括深海作业A各类时长处理问题",
        "盘点动作": "什么设备执行了什么动作的问题。未知设备和未知动作，有具体日期或者时间时间段 例如：xxxx/xx/xx 什么设备进行了什么动作",
        "能耗问题": "计算各类设备的能耗数值问题, 例如：xxxx/xx/xx A架设备总能耗是多少？几月几日xx过程中xx设备能耗，xx日~xx日设备平均能耗等",
        "油耗问题": "计算发电机的油耗，例如：xxxx/xx/xx 上午 发电机油耗是多少L?",
        "资料查询问题": "查询设备的属性参数，查询设备的属性资料，包括给定设备指标查询数据表字段。例如：柴油发电机组滑油压力的范围是多少？一号柴油发电机组有功功率测量的范围是多少kW到多少kW？ 控制xxxx的字段名称是",
        "特殊条件时长问题": "A架，绞车，折臂吊车都存在待机情况，角度摆动存在持续存在，计算这些特殊情况的时常问题。例如：xxxx/xx/xx A架实际运行时长，xxxx/xx/xx 上午折臂吊车的待机时长是多少？ (注意实际运行时长和运行时长的区别)",
        "理论发电量计算": "计算发电机的理论发电量，包括发电效率，例如： xxxx/xx/xx ~ xxxx/xx/xx 时间 柴油发电机的理论发电量",
        "原始字段异常判断": "询问原始数据中的数据是否存在异常，例如： xxxx/xx/xx 上午 A架摆动数据是否异常",
        "未知分类": "不属于上述分类"
    例如：
    问题：请指出2024/01/01 深海作业A回收阶段小艇检查完毕和小艇下水相隔多久
    回答：
    ```
    【单天动作时长】 时间、阶段、动作和动作 时间相隔多久
    ```
    
    例如：
    2024/08/15~2024/08/20（含）A架第一次开机在上午8:00前的天数（以整数输出，当天没有开机动作的话不计数）？
    ```
    【多天动作筛选】 时间~时间 设备 第一次开机 在 某时间 之前 天数统计
    ```

    例如：
    2024/08/15~2024/08/20 A架第一次开机最早的那天
    ```
    【多天动作筛选汇总】 多天 设备 第n次动作 多天时刻最早 的那一天
    ```
    
    例如：
    2024/08/15 A架开机时长
    ```
    【单天动作时长】
    ```
    
    突出 统计指标和计算方法 内容类型,注意澄清问题表达的主体
    """
    user_prompt = """
    请抽取问题结构，不解释不说明直接返回结果
    问题：
    ===以下是问题内容=======
    {question}
    =====================
    注意：严格根据问题内容抽取
    """
    messages = [
        {"role":"user","content":system_prompt},
        {"role": "user", "content": user_prompt.format(question=question)}
    ]
    return llm_invoke(messages)


if __name__ == "__main__":
    import pandas as pd
    df = pd.read_excel("fewshot_content.xlsx")
    result = []
    for idx,item in df.iterrows():
        question = item["问题"]
        analysis = item["解析"]
        question_struct = llm_struct(question)
        question_struct = question_struct.strip("`")
        result.append({"question":question,"question_struct":question_struct,"analysis":analysis,"solver":item["sql案例"]})
    json.dump(result,open("question_solver_example.json","w",encoding="utf-8"),indent=4,ensure_ascii=False)
        # text_info = f"""
        # ==============================
        # {question}
        # ++++++++++++++++++++++++++++++
        # {sql}
        # """
        # logger.info(text_info)
    # question_list = json.load(open("question_classify_step.json",encoding="utf-8"))
    # _,sql = action_sql("""5月20日，征服者起吊到A架关机的时间点是多少？(征服者入水时段)""")
    # print(sql)

    # for item in question_list["盘点动作"]:
    #         q =  item["question"]
    #         sql_time = fuzzy_action_sql(q)
    #         text_info = f"""
    #         ===============================
    #         {q}
    #         +++++++++++++++++++++++++++++++
    #         {sql_time}
    #         """
    #         logger.info(text_info)
    # for item in question_list["动作数据查询"]:
    #         q =  item["question"]
    #         sql_time = action_sql(q)
    #         text_info = f"""
    #         ===============================
    #         {q}
    #         +++++++++++++++++++++++++++++++
    #         {sql_time}
    #         """
    #         logger.info(text_info)
    # for item in question_list["时长处理问题"]:
    #         q =  item["question"]
    #         sql_time = duration_sql(q)
    #         text_info = f"""
    #         ===============================
    #         {q}
    #         +++++++++++++++++++++++++++++++
    #         {sql_time}
    #         """
    #         logger.info(text_info)
    # for item in question_list["设备数据查询"]:
