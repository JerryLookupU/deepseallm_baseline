from base_llm import llm_invoke
import json
from tqdm import tqdm
operations = [
    "A架开机", "A架摆出", "征服者起吊", "征服者入水", "征服者出水", "A架关机", "A架摆回",
    "折臂吊车开机", "折臂吊车关机", "小艇入水", "缆绳解除", "小艇落座", "缆绳挂妥","ON DP","OFF DP", "征服者", "小艇", "缆绳","揽绳","摆回时间","深海作业A"
]
entities = ["A架", "征服者", "折臂吊车", "小艇", "缆绳"]
actions = ["开机", "摆出", "起吊", "入水", "出水", "关机", "摆回", "解除", "落座", "挂妥","开启","关闭","结束"]
time_predicate = ["","什么时间","时间点","的时间","是几点","是什么时候","什么时候","是否开始运行？","是否正在进行","开始的时间","结束的时间","开启时间","关闭时间","的第一次开机时间","的最后一次关机时间","开机时间","关机时间","的比例","的占比","次数","摆次","开始运行"]
duration_word = ["间隔多久","长多久","相隔多久","时长","间隔","相隔","运行时长","需要多久","用了多少时间","花了多久","多少时间后","时长相比","时间是多久","时间是多长"]

extend_time_word = ["作业开始的时间","作业A开始的时间","作业A开始时间",
            "作业A开始的时间","作业结束的时间","作业A结束的时间",
            "作业A结束时间","作业A结束的时间","回收过程的开始时间",
            "回收过程的开始时间","回收的开始时间","回收阶段的开始时间",
           "下放过程的开始时间","下放的开始时间","下放阶段的开始时间",
            "回收过程的结束时间","回收的结束时间","回收阶段的结束时间",
           "下放过程的结束时间","下放的结束时间","下放阶段的结束时间",
]
time_predicate = time_predicate + extend_time_word
device_keyword = [
    "负载",
    "转速",
    "燃油消耗率",
    "蓄电池电压",
    "缸套水温度",
    "缸套水压力",
    "缸套水液位",
    "排气温度",
    "滑油压力",
    "滑油温度",
    "启动空气压力",
    "燃油压力",
    "应急停止",
    "安全系统故障",
    "控制系统故障",
    "发动机控制模块通用报警",
    "启动故障",
    "冷却液温度",
    "转速传感器故障",
    "缸套水温度传感器故障",
    "滑油压力传感器故障",
    "应急停止传感器故障",
    "超速停车",
    "缸套水高温停车",
    "滑油压力低停车",
    "海水压力低",
    "滑油滤器压差",
    "膨胀柜液位低",
    "电源故障",
    "冷风温度",
    "热风温度",
    "非驱动轴轴承温度",
    "驱动轴轴承温度",
    "绕组温度",
    "冷却水泄漏报警",
    "报警系统失效",
    "膨胀水柜低位报警",
    "电压测量",
    "电流测量",
    "有功功率测量",
    "频率测量",
    "励磁电流",
    "主开关闭合反馈",
    "主开关开启反馈",
    "主开关合闸命令",
    "主开关分闸命令",
    "主开关脱扣报警",
    "远程控制反馈",
    "远程控制",
    "就绪",
    "遥控启动",
    "遥控停止",
    "升速",
    "减速",
    "卸载",
    "功率分配模式转换",
    "额定转速运行",
    "公共停车",
    "起动失败",
    "负载分配线路故障",
    "负荷分配器公共报警",
    "AVR功率监控",
    "停泊/应急发电机组",
    "报警值",
    "警告",
    "应急发电机",
    "停泊发电",
    "请求",
    "指示",
    "一号外消防",
    "闭合反馈",
    "开启反馈",
    "开关电流",
    "主配电板联络开关",
    "变压器",
    "绝缘故障",
    "主配电板",
    "应急配电板",
    "联络开关",
    "备车完毕",
    "合闸命令",
    "分闸命令",
    "遥控起动",
    "分闸反馈",
    "报警系统失效",
    "电源故障",
    "应急模式",
    "断线报警",
    "开关",
    "变压器",
    "燃油",
    "压力的范围",
]
def format_type(info):
    result = []
    for class_key,class_value in info.items():
        str_data = "%s: %s"%(class_key,class_value)
        result.append(str_data)
    return "\n".join(result)
classes_info = {
        "动作数据查询":  """根据动作查询时间点，深海作业A的动作时刻查询问题，需要要明确动作或者明确设备，例如：xxxx/xx/xx A架第一次开机时间在什么时候？ xxxx/xx/xx 缆绳解除的时间点是什么时候？,动作包括："A架开机", "A架摆出", "征服者起吊", "征服者入水", "征服者出水", "A架关机", "A架摆回",
    "折臂吊车开机", "折臂吊车关机", "小艇入水", "缆绳解除", "小艇落座", "缆绳挂妥","ON DP","OFF DP" """,
        "设备数据查询":"具体设备在某个时刻或者个时间段内的数值情况，设备包括，推进器，侧推（艏推或者艏侧推），绞车，A架（一号二号门架），折臂吊车，发电机，舵桨 问题中一定会包含具体时间，主要是各种设备各个时刻数据指标状态，指标包括压力，电流电压，频率Hz，功率，相位，燃油。例如：在XXXX年XX月XX日XX时XX分，X号柴油发电机组燃油消耗率是多少？",
        "时长处理问题": "动作相隔时间判断问题，包括各类时长比较计算，例如：xxxx/xx/xx A架开机和关机时间间隔多久？, xxxx/xx/xx A架运行时长多长,xx/xx~xx/xx 平均时长, 包括深海作业A各类时长处理问题",
        "盘点动作": "什么设备执行了什么动作的问题。未知设备和未知动作，有具体日期或者时间时间段 例如：xxxx/xx/xx 什么设备进行了什么动作",
        "能耗问题": "计算各类设备的能耗数值问题, 例如：xxxx/xx/xx A架设备总能耗是多少？几月几日xx过程中xx设备能耗，xx日~xx日设备平均能耗 相关设备能耗等",
        "油耗问题": "计算发电机的油耗，例如：xxxx/xx/xx 上午 发电机油耗是多少L?",
        "资料查询问题": "查询设备的属性参数，查询设备的属性资料，包括给定设备指标查询数据表字段。例如：柴油发电机组滑油压力的范围是多少？一号柴油发电机组有功功率测量的范围是多少kW到多少kW？ 控制xxxx的字段名称是",
        "特殊条件时长问题": "A架，绞车，折臂吊车都存在待机情况，角度摆动存在持续存在，计算这些特殊情况的时常问题。例如：xxxx/xx/xx A架实际运行时长，xxxx/xx/xx 上午折臂吊车的待机时长是多少？ (注意实际运行时长和运行时长的区别)",
        "理论发电量计算": "计算发电机的理论发电量，包括发电效率，例如： xxxx/xx/xx ~ xxxx/xx/xx 时间 柴油发电机的理论发电量",
        "原始字段异常判断": "询问原始数据中的数据是否存在异常，例如： xxxx/xx/xx 上午 A架摆动数据是否异常",
        "未知分类": "不属于上述分类"
    }
format_classify_info = format_type(classes_info)
def get_system_prompt(format_classify_define):
    time_example = """
    xxxx/xx/xx 早上A架实际作业时长是多少？
    /**
    特殊时长仅有 A架实际作业时长、A架摆出某角度持续时长、这种情况
    **/
    ```
    特殊条件时长问题
    ```

    xxxx/xx/xx 早上A架运行时长是多久？
    /**
    单天 单一设备时长 可以归为动作时间间隔
    **/
    ```
    时长处理问题
    ```

    统计xxxx/xx/xx-xx/xx在7点前开始工作的比例
    /**
    不涉及到时长计算比较的情况, 比较 动作前后关系，为盘点动作
    **/
    ```
    盘点动作
    ```
    """

    fangwei = """
    一号柴油发电机组滑油压力的范围是多少？
    ```
    资料查询问题
    ```

    一号柴油发电机组燃油压力的最大值是多少？
    ```
    资料查询问题
    ```

    一号柴油发电机组燃油压力报警值是多少？
    ```
    资料查询问题
    ```

    某个时刻xxx设备xx值为300kPa，这个数值是正常的吗？
    ```
    资料查询问题
    ```

    发电机组转速为xxxxRPM会发生什么？
    ```
    资料查询问题
    ```
    """

    jiaodu = """
    xxxx/xx/xx A架角度数据是否异常?
    ```
    原始字段异常判断
    ```

    xxxx/xx/xx A架最大角度是多少？持续多久？
    ```
    特殊条件时长问题
    ```
    """

    shijian = """
    xxxx/xx/xx A架摆出时间点在什么时候？
    /**
    除了A架摆出 还有一系列 如： 缆绳解除、缆绳挂妥 ...
    **/
    ```
    动作数据查询
    ```
    """

    dongzuo = """
    xxxx/xx/xx xx:xx 发生了什么动作？
    /**
    尽量只考虑这种类型问题
    **/
    ```
    盘点动作
    ```

    xxxx/xx/xx xx:xx ~ xx:xx 发生了什么关键动作？
    ```
    盘点动作
    ```
    
    根据步骤1，2回答问题
    /**
    单独提问 无法回答该问题 问题主体不清晰无法回答 属于未知分类
    **/
    ```
    未知分类
    ```
    """

    system_prompt = f"""
    你是一名问题分类专家，能根据类别和描述对问题进行分类，如果这个问题不在给定的问题类别中，请返回 "未知分类"
    问题背景：
        深海作业船是进行远洋深海探测作业船舶，需要对于常见作业问题进行回答，现在需要对问题进行分类，其中作业有若干个动作，每个动作都有具体的执行设备，当然还会询问具体设备某一个时刻的数据或者询问某个设备的指标
    以下是分类标准：
        {format_classify_define}
    案例如下：
    """
    system_prompt = system_prompt + time_example + dongzuo + jiaodu + fangwei
    return system_prompt


def get_user_prompt(question, device_keyword):
    fuzz_word = ["什么设备执行了什么动作","发生了哪些关键动作", "执行什么动作", "什么动作在执行", "什么设备执行了什么关键动作", "什么关键动作在进行", "发生了哪些关键动作", "哪些关键动作","什么设备在进行什么动作"]
    extend_word = ""
    for word in fuzz_word:
        if (word in question) or ((("哪些" in question) or ("什么" in question)) and ("动作" in question)):
            return "盘点动作"
    for item in device_keyword:
        if item in question:
            extend_word = "其他信息：命中 设备资料关键词：" + item
            break
    user_prompt = f"""
    请帮我直接进行问题分类，不解释不说明
    {question}
    仅返回分类结果
    """
    user_prompt = extend_word + user_prompt
    return user_prompt


def question_classify(question, format_classify_define=format_classify_info, device_keyword=device_keyword, operations=operations, time_predicate=time_predicate, duration_word=duration_word):
    if "外摆的最大角度范围" in question:
        return "特殊条件时长问题"

    if "平均作业能耗" in question:
        return "能耗问题"

    if "总能耗" in question:
        return "能耗问题"
    if ("发电量" in question) or ("总发电" in question):
        return "能耗问题"
    if "总做功" in question:
        return "能耗问题"
    if ("燃油消耗量" in question):
        return "能耗问题"
    if "平均摆动次数" in question:
        return "动作数据查询"

    if "数据" in question and ("缺失" in question):
        return "原始字段异常判断"

    out_word = "什么原因导致"
    action_match = False
    predicate_match = False
    duration_match = False
    for operation in operations:
        if operation in question:
            action_match = True
            break

    for ipredicate in time_predicate:
        if ipredicate in question:
            predicate_match = True
            break

    for iduration in duration_word:
        if iduration in question:
            duration_match = True
            break

    if action_match and predicate_match and not duration_match and out_word not in question:
        return "动作数据查询"

    system_prompt = get_system_prompt(format_classify_define)
    example_q = """
    请帮我直接进行问题分类，不解释不说明
    在2024年8月28日，A架的关机时间是几点（请以XX:XX输出）
    仅返回分类结果
    """
    example_a = """动作数据查询"""
    user_prompt = get_user_prompt(question, device_keyword)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": example_q},
        {"role": "assistant", "content": example_a},
        {"role": "user", "content": user_prompt}
    ]
    response = llm_invoke(messages)
    return response.strip("`").strip("\"")


if __name__ == "__main__":
    res = []
    fp = open("../data/深远海初赛b榜题目.jsonl",encoding="utf-8")
    for item in tqdm(fp):
        node = json.loads(item)
        node["question_type"] = question_classify(node["question"])
        res.append(node)
    json.dump(res, open("plan_b_classify.json", "w", encoding="utf-8"),indent=4 ,ensure_ascii=False)
    # fp = open("plan_b_classify.json", "r", encoding="utf-8")
    # data = json.load(fp)
    # json.dump(data, open("plan_b_classify1.json", "w", encoding="utf-8"),indent=4 ,ensure_ascii=False)