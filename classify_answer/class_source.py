"""
class_source - 

Author: cavit
Date: 2025/2/18
"""



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
        "能耗问题": "计算各类设备的能耗数值问题, 例如：xxxx/xx/xx A架设备总能耗是多少？几月几日xx过程中xx设备能耗，xx日~xx日设备平均能耗等",
        "油耗问题": "计算发电机的油耗，例如：xxxx/xx/xx 上午 发电机油耗是多少L?",
        "资料查询问题": "查询设备的属性参数，查询设备的属性资料，包括给定设备指标查询数据表字段。例如：柴油发电机组滑油压力的范围是多少？一号柴油发电机组有功功率测量的范围是多少kW到多少kW？ 控制xxxx的字段名称是",
        "特殊条件时长问题": "A架，绞车，折臂吊车都存在待机情况，角度摆动存在持续存在，计算这些特殊情况的时常问题。例如：xxxx/xx/xx A架实际运行时长，xxxx/xx/xx 上午折臂吊车的待机时长是多少？ (注意实际运行时长和运行时长的区别)",
        "理论发电量计算": "计算发电机的理论发电量，包括发电效率，例如： xxxx/xx/xx ~ xxxx/xx/xx 时间 柴油发电机的理论发电量",
        "原始字段异常判断": "询问原始数据中的数据是否存在异常，例如： xxxx/xx/xx 上午 A架摆动数据是否异常",
        "未知分类": "不属于上述分类"
    }
format_classify_info = format_type(classes_info)

## 文本分类