import json

from base_llm import llm_invoke_fix


# 问题拆分 条件和问题拆分 question_struct_step
# 1先进行问题 条件整理
def question_struct_step(question):
    system_prompt = """
    你是一名文本处理专家，能按照我提出的要求准确的处理：
    要求：
          一、文本的问题和问题后处理分开（注意后处理条件是指只会影响回答格式的结果条件）；
          二、如果文本中没有处理条件，根据类别，按照下列要求补充；
                  1、结果返回为时间点问题，补充 （按照XX:XX:XX返回）。
                  2、结果返回为时长问题，补充 （按照XX分钟，保留小数点后两位返回）。
                  3、结果为数值浮点型问题，补充 （保留小数点后两位返回）。
                  4、什么设备进行什么动作类问题，如果没有条件，不用补充
                  5、如果问题答案精确到天，则补充精确到天的格式为 (mm/dd，例如01/02)
                  6、其他情况如无问题后处理格式要求的情况，补充 ""
          三、如果已有问题本体和条件，不要对问题本体和条件进行额外处理，如果选项也需要参与回答，则问题中包含选项。
    现在你需要根据我要求严格执行文本处理后，并以json结构返回:
    案例如下：
    
    2024/x/x A架比折臂吊车的运行时间少多少（以整数分钟输出）？
    返回结果：
    ```json
    {"question":"2024/x/x  A架比折臂吊车的运行时间少多少?","condition":"以整数分钟输出"}
    ```

    根据步骤1结果，计算每台发电机消耗的柴油质量是多少（单位：kg）？
    ```json
    {"question":"根据步骤1结果，计算每台发电机消耗的柴油质量是多少","condition":"（单位：kg，保留小数点后两位）"}
    ```

    2024年5月20日7点23分发生了什么动作？
    ```json
    {"question":"2024年5月20日7点23分发生了什么动作？","condition":""}
    ```
    
    24年8月xx日下午xx点xx分发生了什么？
    ```json
    {"question":"2024年xx月xx日xx点xx分发生了什么动作？","condition":""}
    ```
    
    2024/8/xx和2024/8/xx 平均作业时长是多久（四舍五入至整数分钟输出，下放阶段以ON DP和OFF DP为标志，回收阶段以A架开机和关机为标志）？
    ```json
    {"question":"2024/8/xx和2024/8/xx 平均作业时长是多久？","condition":"（四舍五入至整数分钟输出，下放阶段以ON DP和OFF DP为标志，回收阶段以A架开机和关机为标志），保留小数点后两位"}
    ```

    2024/8/xx 下午，A架开机发生在折臂吊车开机之前，是否正确？
    ```json
    {"question":"2024/8/xx 下午，A架开机发生在折臂吊车开机之前，是否正确？","condition":"回答正确或者不正确"}
    ```
    
    某个时刻一号柴油发电机组滑油压力大为170kPa会导致柴油发电机停机吗？
    /**
    需要返回描述性质回答，不进行返回条件限制 
    **/
    ```json
    {"question":"某个时刻一号柴油发电机组滑油压力大为170kPa会导致柴油发电机停机吗？","condition":""}
    ```

    24年8月xx日xx至xx点A架是否开始运行？Y：是；N：否。请回答Y或N
    ```json
    {"question":"24年8月xx日xx至xx点A架是否开始运行？","condition":"Y：是；N：否。请回答Y或N"}
    ```

    下列哪些选项是正确的：A: A架在1月21号未开机 B: A架在1月22开机
    /**
    该问题选项里信息会影响回答结果，所以不拆分。
    **/
    ```json
    {"question":"下列哪个选项是正确的：A: A架在1月21号未开机 B: A架在1月22开机","condition":"请回答A或者B或者A、B"}
    ```
    
    2024/8/xx和2024/8/xx 平均作业时长是多久（下放阶段和回收阶段均以开机关机为准）？
    /**
    案例解析： condition中的内容不能改变，问题的解答路径和思路，只能对最终结果进行格式变化
    **/
    ```json
    {"question":"2024/8/xx和2024/8/xx 平均作业时长是多久？（下放阶段和回收阶段均以开机关机为准）","condition":""}
    ```
  
    """
    user_prompt = f"""
    请按照要求返回json结构，必须是python json.loads 能够解析的结构:
    {question}
    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content":user_prompt}
    ]

    json_response = llm_invoke_fix(messages)
    return json_response


def question_step_batch(path):
    question_classify = json.load(open(path, "r", encoding="utf-8"))
    for key in question_classify.keys():
        for item in question_classify[key]:
            question_conditions = question_struct_step(item["question"])
            item["src_q"],item["question"],item["condition"] = item["question"],question_conditions["question"],question_conditions["condition"]
    new_path =  path.replace(".json","") + "_step.json"
    json.dump(question_classify, open(new_path, "w", encoding="utf-8"), indent=4, ensure_ascii=False)

if __name__ == '__main__':
    path = "question_classify.json"
    question_step_batch(path)