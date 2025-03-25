"""
config - 

Author: cavit
Date: 2025/2/19
"""


import numpy as np
import pandas as pd
keys_tables = {
    # A架相关
    "Ajia_plc": ["Ajia_plc_1"],

    "A架": ["Ajia_plc_1", "task_action"],
    "A架角度": ["Ajia_plc_1"],
    "角度": ["Ajia_plc_1"],
    "A架右舷角度": ["Ajia_plc_1"],
    "A架左舷角度": ["Ajia_plc_1"],
    "A架开机": ["Ajia_plc_1", "task_action"],
    "A架关机": ["Ajia_plc_1", "task_action"],
    "A架摆出": ["Ajia_plc_1", "task_action"],
    "A架摆回": ["Ajia_plc_1", "task_action"],
    "A架运行时长": ["Ajia_plc_1", "task_action"],
    "A架摆动次数": ["Ajia_plc_1", "task_action"],
    "A架第一次开启": ["Ajia_plc_1", "task_action"],
    "A架最后一次关机": ["Ajia_plc_1", "task_action"],

    # 深海作业A相关
    "深海作业A": ["Ajia_plc_1", "task_action"],
    "深海作业A开始": ["Ajia_plc_1", "task_action"],
    "深海作业A结束": ["Ajia_plc_1", "task_action"],
    "作业A": ["Ajia_plc_1", "task_action"],
    "下放": ["Ajia_plc_1", "task_action"],
    "回收": ["Ajia_plc_1", "task_action"],

    # 征服者相关
    "征服者": ["task_action"],
    "征服者入水": ["task_action"],
    "征服者出水": ["task_action"],
    "征服者落座": ["task_action"],
    "征服者起吊": ["task_action"],
    "征服者入水平均时间": ["task_action"],
    "征服者出水平均时间": ["task_action"],
    # 绞车相关
    "Jiaoche_plc": ["Jiaoche_plc_1","device_1_15_meter_115", "task_action"],
    "绞车": ["Jiaoche_plc_1", "device_1_15_meter_115", "task_action"],
    "绞车A": ["Jiaoche_plc_1", "device_1_15_meter_115", "task_action"],
    "绞车B": ["Jiaoche_plc_1", "device_1_15_meter_115", "task_action"],
    "绞车C": ["Jiaoche_plc_1", "device_1_15_meter_115", "task_action"],
    "绞车变频器": ["device_1_15_meter_115"],
    "绞车张力": ["Jiaoche_plc_1"],

    # 折臂吊车相关
    "折臂吊车": ["device_13_11_meter_1311", "task_action"],
    "折臂吊车液压": ["device_13_11_meter_1311"],
    "折臂吊车能耗": ["device_13_11_meter_1311", "task_action"],

    # 门架主液压泵相关
    "一号门架主液压泵": ["device_1_5_meter_105"],
    "二号门架主液压泵": ["device_13_14_meter_1314"],

    # 舵桨相关
    "一号舵桨": ["device_1_2_meter_102", "device_1_3_meter_103", "Port3_ksbg_10", "task_action"],
    "二号舵桨": ["device_13_2_meter_1302", "device_13_3_meter_1303", "Port4_ksbg_9", "task_action"],
    "左舵桨": ["Port1_ksbg_5", "task_action"],
    "右舵桨": ["Port2_ksbg_4", "task_action"],
    "舵桨转舵A": ["device_1_2_meter_102", "device_13_2_meter_1302"],
    "舵桨转舵B": ["device_1_3_meter_103", "device_13_3_meter_1303"],

    # 发电机组相关
    "Port1_ksbg_1": ["Port1_ksbg_1"],
    "一号柴油发电机组": ["Port1_ksbg_1", "Port1_ksbg_3"],
    "二号柴油发电机组": ["Port1_ksbg_1", "Port1_ksbg_3"],
    "三号柴油发电机组": ["Port2_ksbg_1", "Port2_ksbg_2"],
    "四号柴油发电机组": ["Port2_ksbg_1", "Port2_ksbg_3"],
    "停泊/应急发电机组": ["Port1_ksbg_2"],
    "发电机组能耗": ["Port1_ksbg_1", "Port1_ksbg_3", "Port2_ksbg_1", "Port2_ksbg_2", "Port2_ksbg_3"],
    "发电机组燃油消耗": ["Port1_ksbg_1", "Port2_ksbg_1"],

    # 推进器相关
    "推进器": ["Port3_ksbg_8", "Port3_ksbg_9", "Port4_ksbg_7", "Port4_ksbg_8", "Port4_ksbg_9"],
    "主推进变频器": ["Port3_ksbg_8"],
    "可伸缩推": ["Port2_ksbg_4", "Port4_ksbg_8"],
    "艏推": ["Port1_ksbg_4", "Port3_ksbg_9"],
    "艏侧推": ["Port1_ksbg_4", "Port3_ksbg_9"],
    "侧推": ["Port1_ksbg_4", "Port3_ksbg_9"],

    # 其他设备和动作
    "缆绳解除": ["task_action"],
    "缆绳挂妥": ["task_action"],
    "小艇": ["task_action"],
    "小艇入水": ["task_action"],
    "小艇落座": ["task_action"],
    "DP过程": ["task_action"],
    "甲板机械设备": ["device_1_15_meter_115", "device_13_11_meter_1311", "task_action"],
    "设备能耗": ["device_1_15_meter_115", "device_13_11_meter_1311", "Port1_ksbg_1", "Port1_ksbg_3", "Port2_ksbg_1", "Port2_ksbg_2", "Port2_ksbg_3", "Port3_ksbg_8", "Port3_ksbg_9", "Port4_ksbg_7", "Port4_ksbg_8", "Port4_ksbg_9"],
    "设备运行时间": ["Ajia_plc_1", "device_1_15_meter_115", "device_13_11_meter_1311", "task_action"],

    # 时间相关
    "开机时间": ["Ajia_plc_1", "task_action"],
    "关机时间": ["Ajia_plc_1", "task_action"],
    "入水时间": ["task_action"],
    "出水时间": ["task_action"],
    "落座时间": ["task_action"],
    "动作时间": ["task_action"],

    # 其他
    "任务动作": ["task_action"],
    "设备名称": ["task_action"],
    "动作类型": ["task_action"],
    "能耗": ["device_1_15_meter_115", "device_13_11_meter_1311", "Port1_ksbg_1", "Port1_ksbg_3", "Port2_ksbg_1", "Port2_ksbg_2", "Port2_ksbg_3", "Port3_ksbg_8", "Port3_ksbg_9", "Port4_ksbg_7", "Port4_ksbg_8", "Port4_ksbg_9"],
    "运行时长": ["Ajia_plc_1", "device_1_15_meter_115", "device_13_11_meter_1311", "task_action"],
    "平均能耗": ["device_1_15_meter_115", "device_13_11_meter_1311", "Port1_ksbg_1", "Port1_ksbg_3", "Port2_ksbg_1", "Port2_ksbg_2", "Port2_ksbg_3", "Port3_ksbg_8", "Port3_ksbg_9", "Port4_ksbg_7", "Port4_ksbg_8", "Port4_ksbg_9"],
    "作业时长": ["task_action"],
    "作业能耗": ["device_1_15_meter_115", "device_13_11_meter_1311", "Port1_ksbg_1", "Port1_ksbg_3", "Port2_ksbg_1", "Port2_ksbg_2", "Port2_ksbg_3", "Port3_ksbg_8", "Port3_ksbg_9", "Port4_ksbg_7", "Port4_ksbg_8", "Port4_ksbg_9"],
    "数据异常": ["Ajia_plc_1", "device_1_15_meter_115", "device_13_11_meter_1311", "task_action"]
}

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
    "MAC报警板24V电源故障",
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
    "舵桨",
    "一号门架",
    "二号门架",
    "艏推",
    "开关电流",
    "主配电板联络开关",
    "变压器",
    "左舷",
    "右舷",
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
    "停泊/应急发电机开关",
    "左舵桨开关",
    "MCC左屏主开关",
    "ROV1主开关",
    "一号690V/230V主变压器",
    "一号应急变压器",
    "二号应急变压器",
    "燃油",
    "二号柴油发电机",
    "三号柴油发电机",
    "四号柴油发电机"
    "压力的范围",
]





# df1["报警值"]
def alarm_rename(x):
    if x is None:
        return x
    if type(x) == str:
        if "↑" in x:
            x = x.replace("↑", "")
            x = x.strip()
            x = "高于" + x
        if "↓" in x:
            x = x.replace("↓", "")
            x = x.strip()
            x = "低于" + x
    return x


def to_int(x):
    if x is None:
        return x
    if np.isnan(x):
        return x
    return int(x)

def get_device_info_table(path):
    df = pd.read_excel(path, sheet_name="Sheet1")
    df1 = df[['Channel_Text', 'Channel_Text_CN', 'Alarm_Information_Range_Low',
              'Alarm_Information_Range_High', 'Alarm_Information_Unit',
              'Parameter_Information_Alarm', 'Parameter_Information_Inhibit',
              'Parameter_Information_Delayed', 'Safety_Protection_Set_Value',
              'Remarks']]
    df1.columns = ["参数名", "参数中文名", "参数下限", "参数上限", "报警值单位", "报警值", "屏蔽值", "报警信号延迟值",
                   "安全保护设定值", "超过安全保护设定值之后动作"]
    df1["超过安全保护设定值之后动作"] = df1["超过安全保护设定值之后动作"].replace("Alarm state", "报警")
    df1["超过安全保护设定值之后动作"] = df1["超过安全保护设定值之后动作"].replace("Shutdown", "停机")
    df1["报警值"] = df1["报警值"].apply(lambda x: alarm_rename(x))
    df1["参数下限"] = df1["参数下限"].apply(lambda x: to_int(x))
    df1["参数上限"] = df1["参数上限"].apply(lambda x: to_int(x))
    df1["报警信号延迟值"] = df1["报警信号延迟值"].apply(lambda x: to_int(x))
    df1["安全保护设定值"] = df1["安全保护设定值"].apply(lambda x: alarm_rename(x))
    df1 = df1.fillna("")
    device_info_table = list(df1.T.to_dict().values())
    return device_info_table

device_info_table = get_device_info_table("F:/code_review/llmsystem/classify_answer/device_info_array.xlsx")
## 基础数据准备
table_info = []
with open("F:/code_review/llmsystem/classify_answer/table_info.txt",encoding="utf-8") as fp:
    data = fp.read()
    table_info = data.split("\n\n\n")


energy_table =[
    {
        "设备名": "一号门架",
        "字段": "`1-5-6_v` as v,csvTime",
        "表": "device_1_5_meter_105",
        "字段功能": "功",
        "归属": "A架、甲板设备",
    },
    {
        "设备名": "二号门架",
        "字段": "`13-14-6_v` as v,csvTime",
        "表": "device_13_14_meter_1314",
        "字段功能": "功",
        "归属": "A架、甲板设备",
    },
    {
        "设备名": "绞车",
        "字段": "`1-15-6_v` as v,csvTime",
        "表": "device_1_15_meter_115",
        "字段功能": "功",
        "归属": "甲板设备",
    },
    {
        "设备名": "折臂吊车",
        "字段": "`13-11-6_v` as v,csvTime",
        "表": "device_13_11_meter_1311",
        "字段功能": "功",
        "归属": "甲板设备",
    },
    {
        "设备名": "一号推进器",
        "字段": "P3_15 as v,csvTime",
        "表": "Port3_ksbg_8",
        "字段功能": "功",
        "归属": "推进设备",
    },
    {
        "设备名": "二号推进器",
        "字段": "P4_16 as v,csvTime",
        "表": "Port4_ksbg_7",
        "字段功能": "功",
        "归属": "推进设备",
    },
    {
        "设备名": "艏推",
        "字段": "P3_18 as v,csvTime",
        "表": "Port3_ksbg_9",
        "字段功能": "功",
        "归属": "推进设备",
    },
    {
        "设备名": "可伸缩推",
        "字段": "P4_21 as v,csvTime",
        "表": "Port4_ksbg_8",
        "字段功能": "功",
        "归属": "推进设备",
    },
    {
        "设备名": "一号柴油发电机",
        "字段": "P1_66 as v,csvTime",
        "表": "Port1_ksbg_3",
        "字段功能": "功",
        "归属": "发电设备",
    },
    {
        "设备名": "一号发电机",
        "字段": "P1_66 as v,csvTime",
        "表": "Port1_ksbg_3",
        "字段功能": "功",
        "归属": "发电设备",
    },
    {
        "设备名": "一号柴油发电机",
        "字段": "P1_3 as v,csvTime",
        "表": "Port1_ksbg_1",
        "字段功能": "油耗",
        "归属": "发电设备",
    },
    {
        "设备名": "二号柴油发电机",
        "字段": "P1_75 as v,csvTime",
        "表": "Port1_ksbg_3",
        "字段功能": "功",
        "归属": "发电设备",
    },
    {
        "设备名": "二号发电机",
        "字段": "P1_75 as v,csvTime",
        "表": "Port1_ksbg_3",
        "字段功能": "功",
        "归属": "发电设备",
    },
    {
        "设备名": "二号柴油发电机",
        "字段": "P1_25 as v,csvTime",
        "表": "Port1_ksbg_1",
        "字段功能": "油耗",
        "归属": "发电设备",
    },
    {
        "设备名": "三号柴油发电机",
        "字段": "P2_51 as v,csvTime",
        "表": "Port2_ksbg_2",
        "字段功能": "功",
        "归属": "发电设备",
    },
    {
        "设备名": "三号发电机",
        "字段": "P2_51 as v,csvTime",
        "表": "Port2_ksbg_2",
        "字段功能": "功",
        "归属": "发电设备",
    },
    {
        "设备名": "三号柴油发电机",
        "字段": "P2_3 as v,csvTime",
        "表": "Port2_ksbg_1",
        "字段功能": "油耗",
        "归属": "发电设备",
    },
    {
        "设备名": "四号柴油发电机",
        "字段": "P2_60 as v,csvTime",
        "表": "Port2_ksbg_3",
        "字段功能": "功",
        "归属": "发电设备",
    },
    {
        "设备名": "四号发电机",
        "字段": "P2_60 as v,csvTime",
        "表": "Port2_ksbg_3",
        "字段功能": "功",
        "归属": "发电设备",
    },
    {
        "设备名": "四号柴油发电机",
        "字段": "P2_25 as v,csvTime",
        "表": "Port2_ksbg_1",
        "字段功能": "油耗",
        "归属": "发电设备",
    },
    {
        "设备名": "停泊/应急发电机",
        "字段": "P1_47 as v,csvTime",
        "表": "Port1_ksbg_2",
        "字段功能": "油耗",
        "归属": "发电设备",
    },
    {
        "设备名": "一号舵桨转舵A",
        "字段": "`1-2-6_v` as v,csvTime",
        "表": "device_1_2_meter_102",
        "字段功能": "功",
        "归属": "舵桨",
    },
    {
        "设备名": "一号舵桨转舵B",
        "字段": "`1-3-6_v` as v,csvTime",
        "表": "device_1_3_meter_103",
        "字段功能": "功",
        "归属": "舵桨",
    },
    {
        "设备名": "二号舵桨转舵A",
        "字段": "`13-2-6_v` as v,csvTime",
        "表": "device_13_2_meter_1302",
        "字段功能": "功",
        "归属": "舵桨",
    },
    {
        "设备名": "二号舵桨转舵B",
        "字段": "`13-3-6_v` as v,csvTime",
        "表": "device_13_3_meter_1303",
        "字段功能": "功",
        "归属": "舵桨",
    }
]
business_logic = """
1.深海作业A是一种深海作业类型。包含下放和回收两个阶段。深海作业A中包含一系列关键动作，具体如下： \n 
OFF DP、折臂吊车关机、折臂吊车开机、ON DP、A架关机、A架开机、小艇检查完毕、小艇入水、征服者起吊、 \n 
征服者入水、缆绳解除、A架摆回、小艇落座、A架摆出、缆绳挂妥、征服者出水、征服者落座。
2.如果没有特殊指明，深海作业A的开始标志动作为'ON DP',深海作业A的结束标志动作为'OFF DP'。 \n
3.全部数据发生的时间都是2024年，如果问题没有说明年份，统一按照2024年计算。 \n
4.深海作业A的上午和下午都可能发生多次以上动作。比如上午就可能有多次A架开机和关机，一般开机和关机都是一一前后对应的。 \n
5.主要的三种设备和相关动作的关系如下： \n
-A架：A架开机、A架摆出、征服者起吊、征服者入水、征服者出水、A架关机、A架摆回。 \n
-折臂吊车：折臂吊车开机、折臂吊车关机、小艇入水、缆绳解除、小艇落座、缆绳挂妥。 \n
-DP:ON DP、OFF DP
"""

energy_table_dict = {}
for item in energy_table:
    key = item["设备名"] + "-" + item["字段功能"]
    energy_table_dict[key] = item
