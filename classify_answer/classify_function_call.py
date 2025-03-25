

from tabulate import tabulate
from datetime import datetime
import pandas as pd
import numpy as np
from typing import Dict,List
import json
from classify_answer.base_llm import client
from loguru import logger
from classify_answer.base_llm import  round

def format_duration(seconds: float) -> Dict:
    """将秒数格式化为两种格式：HH小时XX分钟 和 XX分钟（保留小数点后两位）"""
    minutes = seconds / 60
    hours = int(minutes // 60)
    remaining_minutes = minutes % 60
    round_minutes = round(minutes)
    round_remaining = round(remaining_minutes)
    return {
        "HH小时XX分钟": f"{hours:02d}小时{round_remaining:02d}分钟",
        "XX分钟": f"{round_minutes:02d}分钟"
    }

def format_duration_minutes(minutes: float) -> Dict:
    """将分钟数格式化为两种格式：HH小时XX分钟 和 XX分钟（保留小数点后两位）"""
    hours = int(minutes // 60)
    remaining_minutes = minutes % 60
    round_minutes = round(minutes)
    round_remaining = round(remaining_minutes)
    return {
        "HH小时XX分钟": f"{hours:02d}小时{round_remaining:02d}分钟",
        "XX分钟": f"{round_minutes:02d}分钟"
    }

def date_before_after(action1_name,action1_time, action2_name,action2_time):
    if action1_time < action2_time:
        return f"{action1_name}时间:{action1_time} 早于 {action2_name}时间:{action2_time}"
    else:
        return f"{action1_name}时间:{action1_time} 晚于 {action2_name}时间:{action2_time}"

def keyname_parser(c_dict, key_prefix):
    result = {}
    for k, v in c_dict.items():
        result[key_prefix + "(" + k + ")"] = v

    return result

def calculate_days(start_time, end_time):
    # 计算天数
    if len(start_time) == 10:
        if ":" in start_time:
            raise ValueError("输入字段不满足 年\-月\-日 格式要求")
        start = datetime.strptime(start_time, "%Y-%m-%d")
        end = datetime.strptime(end_time, "%Y-%m-%d")
        delta_hours = (end - start).total_seconds() / 3600 /24
        return round(delta_hours,0) + 1
    if len(start_time) == len("2024-01-01 01:01"):
        start = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
        end = datetime.strptime(end_time, "%Y-%m-%d %H:%M")
        delta_hours = (end - start).total_seconds() / 3600 /24
        return round(delta_hours,0) + 1
    if len(start_time) == len("2024-01-01 01:01:01"):
        start = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        delta_hours = (end - start).total_seconds() / 3600 /24
        return round(delta_hours,0) + 1
    return 0


def time_analysis_by_day(date_pairs: List[List[str]], start_time=None, end_time=None) -> str:
    daily_data = {}
    all_durations = []  # 存储所有时间间隔（秒）用于汇总
    duration_list = {}
    duration_list["开始时间"] = []
    duration_list["结束时间"] = []
    duration_list["时长（分钟）"] = []
    duration_list["时长（XX分钟）"] = []
    duration_list["时长（XX小时XX分钟）"] = []

    for start, end in date_pairs:
        start = start[:-3]
        end = end[:-3]
        start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M")
        end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M")
        duration_minute = (end_dt - start_dt).total_seconds()/60
        day = start_dt.strftime("%Y-%m-%d")

        all_durations.append(duration_minute)
        # "HH小时XX分钟": f"{hours:02d}小时{round_remaining:02d}分钟",
        # "XX分钟": f"{round_minutes:02d}分钟"
        duration_data = format_duration_minutes(duration_minute)

        duration_list["开始时间"].append(start)
        duration_list["结束时间"].append(end)
        duration_list["时长（分钟）"].append(duration_minute)
        duration_list["时长（XX分钟）"].append(duration_data["XX分钟"])
        duration_list["时长（XX小时XX分钟）"].append(duration_data["HH小时XX分钟"])

        if day not in daily_data:
            daily_data[day] = {
                "时长列表": [],
                "当天总时长(分钟)": 0.0,
                "当天最大时长(分钟)": -float("inf"),
                "当天最小时长(分钟)": float("inf"),
                "当天时段条数": 0
            }

        daily_data[day]["时长列表"].append(duration_minute)
        daily_data[day]["当天总时长(分钟)"] += duration_minute
        daily_data[day]["当天时段条数"] += 1

        if duration_minute > daily_data[day]["当天最大时长(分钟)"]:
            daily_data[day]["当天最大时长(分钟)"] = duration_minute
        if duration_minute < daily_data[day]["当天最小时长(分钟)"]:
            daily_data[day]["当天最小时长(分钟)"] = duration_minute

    # for day in daily_data:
    #     daily = daily_data[day]
    #     daily["当天每个时间段平均时长(分钟)"] = daily["当天总时长(分钟)"] / daily["当天时段条数"] if daily["当天时段条数"] else 0.0

    # 计算多天汇总（新增总天数）
    total_days = len(daily_data)
    current_days = len(daily_data)
    if start_time is not None and end_time is not None:
        current_days = calculate_days(start_time, end_time)

    daily_stats = {}
    summary = {}
    summary.update(keyname_parser(format_duration_minutes(sum(all_durations) + 0.0 / total_days), "每天平均时长"))
    summary.update(keyname_parser(format_duration_minutes(sum(all_durations)), "多天总时长"))
    summary.update({"每天平均次数": len(all_durations) / (total_days + 0.0)})
    if total_days != current_days:
        summary.update(keyname_parser(format_duration_minutes(sum(all_durations) + 0.0 / current_days),
                                      "每天平均时长【缺失数据不计算在内】"))
        summary.update({"每天平均次数【缺失数据不计算在内】": "{num}次".format(
            num=round(len(all_durations) / (current_days + 0.0)))})

    for day, data in daily_data.items():
        daily_stats[day] = {}
        daily_stats[day]["日期"] = day
        daily_stats[day].update(keyname_parser(format_duration_minutes(data["当天总时长(分钟)"]), "当天总时长"))
        daily_stats[day].update(keyname_parser(format_duration_minutes(data["当天最大时长(分钟)"]), "当天最大时长"))
        daily_stats[day].update(keyname_parser(format_duration_minutes(data["当天最小时长(分钟)"]), "当天最小时长"))
        # daily_stats[day].update(keyname_parser(format_duration_minutes(data["当天平均时长(分钟)"]), "当天平均时长"))
        daily_stats[day].update({"当天时段数": data["当天时段条数"]})

    df_all_dur = pd.DataFrame(duration_list)
    df_summary = pd.DataFrame([summary])
    df_daily = pd.DataFrame(daily_stats).T
    df_daily = df_daily.reset_index(drop=True)

    all_dur_str = tabulate(df_all_dur, headers='keys', tablefmt="grid", showindex=False, floatfmt=".4f")
    if len(df_all_dur) == 2:
        # 计算差值
        a = duration_list["时长（分钟）"][0]
        b = duration_list["时长（分钟）"][1]
        all_dur_str = all_dur_str + "\n" + "这两个时段的时长相差值为" + str(format_duration_minutes(abs(a - b)))

    daily_str = tabulate(df_daily, headers='keys', tablefmt="grid", showindex=False, floatfmt=".4f")
    summary_str = tabulate(df_summary, headers='keys', tablefmt="grid", showindex=False, floatfmt=".4f")
    result = f"""
以下是时段数据明细表：
{all_dur_str}

以下是天粒度汇总表：
{daily_str}

按照多天粒度进行计算：[注意每天平均时长，计算方法为：多天总时长/天数，如果1天数据则为一天总时长]，[多天总时长，计算方法为：sum(多天时长)]
{summary_str}
    """
    return result


def calculate_days_between_dates(start_date, end_date):
    """
    计算两个日期之间的天数
    :param date_range: 字符串，格式为 "YYYY-MM-DD ~ YYYY-MM-DD"
    :return: 两个日期之间的天数
    """
    try:
        # 分割字符串，提取两个日期
        date1 = start_date.strip()  # 去除空格
        date2 = end_date.strip()

        # 定义日期格式
        date_format = "%Y-%m-%d"

        # 将字符串转换为日期对象
        d1 = datetime.strptime(date1, date_format)
        d2 = datetime.strptime(date2, date_format)

        # 计算天数差
        delta = d2 - d1
        days_diff = abs(delta.days)

        return days_diff + 1
    except Exception as e:
        return f"日期格式错误或计算失败: {e}"

# 8 计算 某个时间段比某天早的天数比例
def more_than_day_ratio(x: List[str], judge_time: str, start_date: str, end_date: str):
    num = calculate_days_between_dates(start_date, end_date)
    i = 0
    for j in x:
        if j[-8:-3] > judge_time:
            i += 1

    return f"计算获取时间列表{start_date}~{end_date}中晚于{judge_time}的比例为：%.4f, 如果考虑缺失数据则晚于{judge_time}的比例为：%.4f" % (
    (i + 0.0) / num, (i + 0.0) / len(x))


def less_than_day_ratio(x: List[str], judge_time: str, start_date: str, end_date: str):
    num = calculate_days_between_dates(start_date, end_date)
    i = 0
    for j in x:
        if j[-8:-3] < judge_time:
            i += 1

    return f"计算获取时间列表{start_date}~{end_date}中早于{judge_time}的比例为：%.4f, 如果考虑缺失数据则早于{judge_time}的比例为：%.4f" % (
    (i + 0.0) / num, (i + 0.0) / len(x))


table_tools = [
    # {
    #     "type": "function",
    #     "function": {
    #         "name": "device_time_gb",
    #         "description": "计算动作之间的时间间隔,可以选择按照日期分组,例如：1、计算xxxx-xx-xx 早上A架开机时长(开机时长是由A架开机和A架关机计算)，2、计算xxxx-xx-xx ~ xxxx-xx-xx 平均每天的A架开机时长，3、计算xxxx-xx-xx A架开机时长(一天可能有多次开机关机)",
    #         "parameters": {
    #             "type": "object",
    #             "properties": {
    #                 "start_action": {
    #                     "type": "string",
    #                     "description": "开始配对的动作名称。"
    #                 },
    #                 "end_action": {
    #                     "type": "string",
    #                     "description": "结束配对的动作名称。"
    #                 }
    #
    #             },
    #             "required": ["start_action", "end_action"]
    #         }
    #     }
    # },
    {
        "type": "function",
        "function": {
            "name": "time_analysis_by_day",
            "description": "计算时间对的间隔时长，返回时间间隔统计分析结果（输入日期对列表）（输入日期对列表:每个数组为[开始时间，结束时间]）,分析日期对列表，按天统计时长信息，并返回汇总结果。",
            "parameters": {
                "type": "object",
                "properties": {
                    "date_pairs": {
                        "type": "array",
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "description": "日期时间对，格式为 ['开始时间', '结束时间']，例如 ['2023-10-01 10:00:00', '2023-10-01 12:00:00']"
                            }
                        },
                        "description": "日期时间对列表，例如 [['2023-10-01 10:00:00', '2023-10-01 12:00:00'], ['2023-10-01 14:00:00', '2023-10-01 15:00:00']]"
                    },
                    "start_time": {
                        "type": "string",
                        "description": "【可选项】开始时间，用于计算总天数，离散时间则设置为None,例如问题 20xx-aa-bb ~ 20xx-xx-xx 则 start_time为 20xx-aa-bb, 如果问题为 20xx-aa-bb 和 20xx-xx-xx 则为None，例如 '2023-10-01 00:00:00'"
                    },
                    "end_time": {
                        "type": "string",
                        "description": "【可选项】结束时间，用于计算总天数，离散时间则设置为None,例如问题 20xx-aa-bb ~ 20xx-xx-xx 则 end_time： 20xx-xx-xx, 如果问题为 20xx-aa-bb 和 20xx-xx-xx 则为None，例如 '2023-10-02 00:00:00'"
                    }
                },
                "required": ["date_pairs"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "action_row_list",
            "description": "将筛选满足条件的动作进行描述说明，例如 A架开机，返回 A架进行A架开机动作,用于询问动作问题（什么设备什么动作、关键动作问题，什么动作发生）",
            "parameters": {
                "type": "object",
                "properties": {
                    "x": {
                        "type": "array",
                        "items": {
                            "type": "str",
                            "description": "动作元素，仅有以下选项 A架开机、A架关机、折臂吊车开机、折臂吊车关机、A架摆出、A架摆回、征服者出水、征服者入水、征服者落座、小艇入水、小艇检查完毕、小艇落座、ON DP、OFF DP、缆绳挂妥、缆绳解除"
                        },
                        "description": "动作列表，只要满足条件的动作均写入"
                    }
                },
                "required": ["x"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "duplicated_table",
            "description": "对上文sql得到的基础表信息进行重复判断，根据字段进行重复判断，通常是判断动作同时发生，key=csvTimeMinute",
            "parameters": {
                "type": "object",
                "properties": {
                    "key": {
                        "type": "str",
                        "description": "通常key的值为 csvTimeMinute，筛选出同时发生的动作，如果key为actionName则表示返回所有相同动作的信息，如果key为deviceName则返回所有相同的设备信息"
                    },
                }
            },
            "required": ["key"]
        }
    },
    {
        "type": "function",
        "function": {
            "name": "more_than_day_ratio",
            "description": "计算时间列表中晚于指定时间的比例,或者说在某个时刻之后的比例。注意动作筛选，深海作业A，分成两个阶段，下放阶段 以 ON DP 开始，OFF DP 结束，回收过程以 A架开机开始，A架关机结束。深海作业A以ON DP 开始，在OFF DP后，以回收阶段中A架关机结束",
            "parameters": {
                "type": "object",
                "properties": {
                    "x": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "description": "时间字符串，格式如 '2023-10-01 10:00:00'"
                        },
                        "description": "时间列表"
                    },
                    "judge_time": {
                        "type": "string",
                        "description": "比较的时间，格式如 '10:00'"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "【可选】开始日期，格式如 '2023-10-01'"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "【可选】结束日期，格式如 '2023-10-02'"
                    }
                },
                "required": ["x", "judge_time"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "less_than_day_ratio",
            "description": "计算时间列表中早于指定时间的比例,或者说在某个时刻之前的比例。注意动作筛选，深海作业A，分成两个阶段，下放阶段 以 ON DP 开始，OFF DP 结束，回收过程以 A架开机开始，A架关机结束。深海作业A以ON DP 开始，在OFF DP后，以回收阶段中A架关机结束",
            "parameters": {
                "type": "object",
                "properties": {
                    "x": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "description": "时间字符串，格式如 '2023-10-01 10:00:00'"
                        },
                        "description": "时间列表"
                    },
                    "judge_time": {
                        "type": "string",
                        "description": "用来比较的时间点，格式如 '10:00'"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "【可选】开始日期，格式如 '2023-10-01'"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "【可选】结束日期，格式如 '2023-10-02'"
                    }
                },
                "required": ["x", "judge_time"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "date_before_after",
            "description": "比较 A和B 两个动作时间点的先后顺序。",
            "parameters": {
                "type": "object",
                "properties": {
                    "action1_name": {
                        "type": "string",
                        "description": "A动作名: 例如 A架开机，折臂吊车开机，征服者入水..."
                    },
                    "action1_time": {
                        "type": "string",
                        "description": "A动作发生时间点,格式如 '2023-10-01 10:00:00'"
                    },
                    "action2_name": {
                        "type": "string",
                        "description": "B动作名: 例如 A架开机，折臂吊车开机，征服者入水..."
                    },
                    "action2_time": {
                        "type": "string",
                        "description": "B动作发生时间点,格式如 '2023-10-01 10:00:00'"
                    },
                },
                "required": ["action1_time","action2_time"]
            }
        }
    },
]







def device_time_gb(start_action, end_action, df):
    # 确保 csvTime 是字符串格式
    if not isinstance(df['csvTime'].iloc[0], str):
        raise ValueError("csvTime 必须是字符串格式")

    # 按 csvTime 排序
    df = df.sort_values(by='csvTime')

    # 初始化变量
    total_duration = 0  # 分钟
    pairs = []  # 存储配对记录

    # 遍历 DataFrame
    start_time = None
    for index, row in df.iterrows():
        # 判断是否是开始配对的设备动作
        if row['actionName'] == start_action:
            start_time = row['csvTime']
        # 判断是否是结束配对的设备动作，并且已经有一个开始时间
        elif row['actionName'] == end_action and start_time is not None:
            end_time = row['csvTime']
            if len(start_time) == len("2024-01-01 00:00:00"):
                start_time = start_time[:-3]
            if len(end_time) == len("2024-01-01 00:00:00"):
                end_time = end_time[:-3]
            # 将字符串时间转换为 datetime 对象
            start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M")

            # 计算时间间隔（分钟）
            duration = (end_dt - start_dt).total_seconds()/60
            total_duration += duration

            # 记录配对信息
            pair_info = {
                "date": start_time[:10],
                'start_time': start_time,
                'end_time': end_time,
                "action_process": start_action + " ~ " + end_action,
                '时长（分钟）': duration
            }
            pairs.append(pair_info)
            # 重置开始时间
            start_time = None
    # 返回结果
    res = pd.DataFrame(pairs)
    gap_str = ""
    if len(res) == 2:
        gap_str = "上述数据中满足条件的两个时间段间隔时长为：" +  str(format_duration(abs(res.loc[0,"时长（分钟）"] - res.loc[1,"时长（分钟）"])))
    res1 = res.groupby(["date"]).agg({"时长（分钟）":sum}).reset_index()
    res1[["HH小时XX分钟","XX分钟"]] = res1["时长（分钟）"].apply(lambda x:pd.Series(format_duration_minutes(x)))
    res_text = tabulate(res1, headers='keys', tablefmt="grid", showindex=False,floatfmt=".4f")
    summary_text =  "多天的时间间隔总和：" + str(format_duration_minutes(res1["时长（分钟）"].sum())) + "\n" + "多天的时间间隔平均值：" + str(format_duration_minutes(res1["时长（分钟）"].mean()))
    res_text = start_action + " ~ " + end_action + " 天粒度时间间隔明细和统计结果\n" + res_text + "\n" +  summary_text + "\n" + gap_str
    res_text = res_text + "\n按照时间段统计结果：\n"  + tabulate(res, headers='keys', tablefmt="grid", showindex=False,floatfmt=".4f")
    summary_text =  "时间间隔总和：" + str(format_duration_minutes(res["时长（分钟）"].sum())) + "\n" + "时间间隔平均值：" + str(format_duration_minutes(res["时长（分钟）"].mean())) + "，有" + str(len(res)) + "个段数据"
    res_text = res_text + " 全部时间时长明细和统计结果\n"  +  summary_text
    return res_text

def action_row_list(x:List[str]):
    """将满足动作列表的动作进行输入并进行转化成文本描述"""
    action_names = {"折臂吊车":"折臂吊车","A架":"A架","DP":"DP","征服者":"A架","小艇":"折臂吊车","缆绳":"A架"}
    result = []
    for row in x:
        for entry_name in action_names.keys():
            if entry_name in row:
                text = action_names[entry_name] + "进行"+row+"动作"
                result.append(text)
                break
    return "，".join(result)

def duplicated_table(key,df):
    """返回df中某列的重复数据"""
    if df is None:
        return "上文数据中无对应数据表"
    dup_df = df[df[key].duplicated(keep=False)]
    if len(dup_df) == 0:
        return "无重复数据"
    return "重复数据如下：\n"+tabulate(dup_df, headers='keys', tablefmt='grid', showindex=False,floatfmt=".4f")


actual_dict = {
        "type": "function",
        "function": {
            "name": "actual_time",
            "description": "计算满足 Ajia-3_v > 0 且 Ajia-5_v > 0 的总时长，并根据时间范围筛选数据（如果提供）。",
            "parameters": {
                "type": "object",
                "properties": {
                    "s_time": {
                        "type": "str",
                        "description": "筛选的开始时间，格式为 'YYYY-MM-DD HH:MM:SS'。如果提供，将筛选从该时间开始的数据。"
                    },
                    "e_time": {
                        "type": "str",
                        "description": "筛选的结束时间，格式为 'YYYY-MM-DD HH:MM:SS'。如果提供，将筛选到该时间结束的数据。"
                    },
                },
            },
            "return": {
                "type": "str",
                "description": "返回总时长（以秒为单位）以及格式化的运行时间（小时、分钟）。如果提供了 s_time 和 e_time，则返回筛选范围内的运行时间；否则返回整个 DataFrame 的运行时间。"
            }
        }
    }

def actual_time(s_time, e_time, df):
    """
    计算 A架设备 实际运行时长的方法
    计算满足 Ajia-3_v > 0 且 Ajia-5_v > 0 的总时长
    :param df: 输入的 pandas DataFrame
    :return: 总时长（以秒为单位）
    """
    start_index = df.index[0]
    end_index = df.index[-1]
    df_start_time = df.iloc[start_index]["csvTime"]
    df_end_time = df.iloc[end_index]["csvTime"]

    total_duration = 0  # 初始化总时长
    start_time = None  # 初始化开始时间
    df_filter = df
    if (s_time is not None) and (e_time is not None):
        if len(s_time) == len("2021-01-01 00:00:00"):
            s_time = s_time[:-3] + ":00"
        if len(e_time) == len("2021-01-01 00:00:00"):
            e_time = e_time[:-3] + ":59"

        df_filter = df[(df["csvTime"] >= s_time) & (df["csvTime"] <= e_time)]

    # 遍历每一行数据
    for index, row in df_filter.iterrows():
        # 检查是否满足条件
        if row['Ajia-3_v'] > 0 and row['Ajia-5_v'] > 0:
            # 如果当前没有开始时间，则记录开始时间
            if start_time is None:
                start_time = row['csvTime'][:-3]
        else:
            # 如果不满足条件且已经有开始时间，则计算时长并累加
            if start_time is not None:
                start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M')
                end_time = datetime.strptime(row['csvTime'][:-3], '%Y-%m-%d %H:%M')
                duration = (end_time - start_time).total_seconds()/60
                total_duration += duration
                start_time = None  # 重置开始时间

    # 如果最后一行仍然满足条件，则需要单独处理
    if start_time is not None:
        end_time = datetime.strptime(df.iloc[-1]['csvTime'][:-3], '%Y-%m-%d %H:%M')
        duration = (end_time - start_time).total_seconds()/60
        total_duration += duration
    total_duration = int(total_duration)
    minutes = total_duration
    hours = int(minutes // 60)
    remaining_minutes = minutes % 60
    round_minutes = round(minutes)
    round_remaining = round(remaining_minutes)
    if (s_time is not None) and (e_time is not None):
        result = f"""开始时间：{s_time}，结束时间：{e_time} 准确计算： A架实际运行时间{round_minutes:02d}分钟（即{hours:02d}小时{round_remaining:02d}分钟）"""
        return result
    else:
        result = f"""开始时间：{df_start_time}，结束时间：{df_end_time} 准确计算： A架实际运行时间{round_minutes:02d}分钟（即{hours:02d}小时{round_remaining:02d}分钟）"""
        return result


def count_triggers(s):
    return (s & ~s.shift(1).fillna(False)).sum()

##
# 数据条件判断
def angle_condition_count(cond,value,df,keyname=['Ajia-0_v','Ajia-1_v']):
    res_num = 0
    if cond == ">":
        t = (df[keyname[0]] > float(value)) & (df[keyname[1]] > float(value))
        x  = count_triggers(t)
        res_num += x
    elif cond == "<":
        t = (df[keyname[0]] < float(value)) & (df[keyname[1]] < float(value))
        x = count_triggers(t)
        res_num += x
    elif cond == "=":
        t = (df[keyname[0]].astype(int) == int(value)) & (df[keyname[1]].astype(int) == int(value))
        x = count_triggers(t)
        res_num += x
    else:
        pass
    return res_num


condition_dict = {
        "type": "function",
        "function": {
            "name": "angle_condition_count",
            "description": "通过角度数据来判断触发情况，A架角度大于小于或者大约为某个数字时候，触发次数，注意A架摆出判断设置，\ncond为 '<'，value为负数 ,A架摆回 \ncond为 '>' ，value为正数\n; 询问大约情况例如摆出大约为43度 则cond为'=',value为 '-43',反之摆回 value为正即 '43'  ",
            "parameters": {
                "type": "object",
                "properties": {
                    "cond": {
                        "type": "str",
                        "description": "触发情况，仅有三个选项为: '>','<','=' "
                    },
                    "value": {
                        "type": "float",
                        "description": "触发值，如果A架摆出为负数，A架摆回为正值"
                    },
                },
            },
            "return": {
                "type": "int",
                "description": "返回角度触发条件次数"
            }
        }
    }

def table_function_call(question,df,question_type=""):
    tabstr = tabulate(df, headers='keys', tablefmt="grid", showindex=False)
    merge_text = f"""
    你是一名深海作业A专家，你能根据表格数据问题及相关的数据表获取准确的结果案例如下：
    1、设备动作分成 A架，折臂吊车，DP 注意不同设备动作发生在相同分钟的情况，都需要说明
    2、通常深海作业A 下放阶段结束以 ON DP 作为下放开始阶段 OFF DP 为下放结束， 回收阶段 以 A架开机为开始 A架关机为结束， 整个深海作业A 以 ON DP 为开始, A架关机为结束点
    以下是pandas 的 DataFrame 经过 tabulate 美化的表格
    {tabstr}
    以下是具体问题：
    {question}
    注意：
    - 不要编造数据，严格按照提供的数据调用函数
    - 查询时间点的问题不需要调用函数
    """
    if ("重复" in question) and ("数据" in question):
        merge_text = merge_text + """
        - 如果是 查询重复数据，需要调用函数 duplicated_table
        """
    if ("运行" in question) and ("A架" in question):
        merge_text = merge_text + """
        - 如果是 计算 某日A架运行时长，可以调用函数 time_analysis_by_day
        """
    if ("实际" in question) and ("A架" in question) and ("运行" in question or "开机" in question):
        merge_text = merge_text + """
        如果是 计算 某日A架实际运行时长，需要调用函数 actual_time
        """
    if (len(df) > 1) and (("后" in question) or ("前" in question)):
        extend_text = """
        获取的数据有多条，如果满足的条件有多条，请全部进行处理，不要遗漏
        """
        merge_text = merge_text + extend_text
    messages = [{"role":"user","content":merge_text}]
    if (len(df) > 1) and (("后" in question) or ("前" in question)):
        few_shot_question = """
        请根据数据和问题调用函数
        数据:
        +------------------+---------------------+--------------+--------------+--------------+
        | csvTimeMinute    | csvTime             | actionName   | deviceName   | actionType   |
        +==================+=====================+==============+==============+==============+
        | 2024-08-xx 08:51 | 2024-08-xx 08:51:07 | ON DP        | DP           | 下放         |
        +------------------+---------------------+--------------+--------------+--------------+
        | 2024-08-xx 09:12 | 2024-08-xx 09:12:09 | A架开机      | A架          | 下放         |
        +------------------+---------------------+--------------+--------------+--------------+
        | 2024-08-xx 11:00 | 2024-08-xx 11:00:09 | A架关机      | A架          | 下放         |
        +------------------+---------------------+--------------+--------------+--------------+
        | 2024-08-xx 11:00 | 2024-08-xx 11:00:07 | OFF DP       | DP           | 下放         |
        +------------------+---------------------+--------------+--------------+--------------+
        | 2024-08-xx 17:19 | 2024-08-xx 17:19:09 | A架开机      | A架          | 回收         |
        +------------------+---------------------+--------------+--------------+--------------+
        | 2024-08-xx 18:36 | 2024-08-xx 18:36:09 | A架关机      | A架          | 回收         |
        +------------------+---------------------+--------------+--------------+--------------+
        | 2024-08-xx 19:51 | 2024-08-xx 19:51:07 | ON DP        | DP           | 其他         |
        +------------------+---------------------+--------------+--------------+--------------+
        问题:
            8月xx日上午ON DP后，什么设备进行了什么动作？
        """
        few_shot_answer = {'content':None,
         'role': 'assistant',
         'tool_calls': [{'id': 'xxxxx',
           'function': {'arguments': '{"x":["A架开机","A架关机","OFF DP"]}',
            'name': 'action_row_list'},
           'type': 'function',
           'index': 0}]}
        temp_system_prompt = """
        你是一名表格分析专家能根据表格数据调用正确的函数
        """
        temp_message= [{"role":"system","content":temp_system_prompt},{"role":"user","content":few_shot_question},few_shot_answer]
        messages = temp_message + [messages[-1]]

    current_tools = table_tools
    if ("实际" in question) and ("A架" in question) and ("运行" in question or "开机" in question):
        current_tools.append(actual_dict)
        logger.info("loader function actual_dict")
    if question_type == "特殊条件时长问题":
        current_tools.append(condition_dict)
        logger.info("loader function condition_dict")
    completion = client.chat.completions.create(
        model="glm-4-plus",
        messages=messages,
        tools = current_tools,
        tool_choice="auto",  # 工具选择模式为auto,表示由LLM自行推理,觉得是生成普通消息还是进行工具调用
        temperature=0.
    )

    result_list = []
    if completion.choices[0].message.tool_calls:
        for func in completion.choices[0].message.tool_calls:
            try:
                args = func.function.arguments
                func_name = func.function.name
                if func_name == "time_analysis_by_day":
                    function_result = time_analysis_by_day(**json.loads(args))
                    result_list.append(function_result)
                elif func_name == "action_row_list":
                    function_result = action_row_list(**json.loads(args))
                    result_list.append(function_result)
                elif func_name == "duplicated_table":
                    function_result = duplicated_table(**json.loads(args),df=df)
                    result_list.append(function_result)
                elif func_name == "actual_time":
                    function_result = actual_time(**json.loads(args),df=df)
                    result_list.append(function_result)
                elif func_name == "less_than_day_ratio":
                    function_result = less_than_day_ratio(**json.loads(args))
                    result_list.append(function_result)
                elif func_name == "more_than_day_ratio":
                    function_result = more_than_day_ratio(**json.loads(args))
                    result_list.append(function_result)
                elif func_name == "date_before_after":
                    function_result = date_before_after(**json.loads(args))
                    result_list.append(function_result)
                elif func_name == "angle_condition_count":
                    function_result = angle_condition_count(**json.loads(args),df=df)
                    result_list.append(function_result)
                else:
                    pass
            except Exception as e:
                logger.error(f"调用函数 {func} 时出错: {e}")
    if len(result_list) == 0:
        return []
    return result_list


# 数学运算函数

def json_format_list(x: List[str]) -> Dict:
    kv_dict = {}
    for item in x:
        s = len("2021-01-01")
        key = item[:s]
        v = ""
        if len(item) == len("2021-01-01 00:00"):
            t = len("00:00")
            v = item[-t:]
        elif len(item) == len("2021-01-01 00:00:00"):
            t = len("00:00:00")
            v = item[-t:]
            v = v[:-3]
        if key not in kv_dict:
            kv_dict[key] = []
        kv_dict[key].append(v)
    result = {}
    for k, vv in kv_dict.items():
        k1 = k.replace("-", "")
        if len(vv) == 1:

            result[k1] = vv[0]
        else:
            for idx in range(len(vv)):
                vidx = idx + 1
                new_k = k1 + "_%d" % vidx
                result[new_k] = vv[idx]
    return json.dumps(result)

def date_sub(date1, date2):
    if len(date1) == len("2024-01-01 00:00:00"):
        fmt = "%Y-%m-%d %H:%M:%S"
    elif len(date1) == len("2024-01-01 00:00"):
        fmt = "%Y-%m-%d %H:%M"
    else:
        fmt = "%Y-%m-%d %H:%M:%S"
    dt1 = datetime.strptime(date1, fmt)
    dt2 = datetime.strptime(date2, fmt)
    diff = dt2 - dt1
    seconds = diff.total_seconds()
    formatted_duration = format_duration(seconds)
    return f"从 {date1} 到 {date2} 的时间差是 {formatted_duration['HH小时XX分钟']}（即 {formatted_duration['XX分钟']}）。"
# 数学运算函数
def math_add(nums: List[float], trans_time: bool = False) -> Dict:
    """计算两个数值之和"""
    a = sum(nums)
    result = "求和计算得到结果: " + str(a)
    if trans_time:
        sum_dict = format_duration_minutes(a)
        result += "\n将数据格式转化后得到：" + str(sum_dict)
    return {"result": result}


def math_sub(num1: float, num2: float, trans_time: bool = False) -> Dict:
    a = abs(num1 - num2)

    result = "求和计算得到结果: " + str(a)
    if trans_time:
        sum_dict = format_duration_minutes(a)
        result += "\n将数据格式转化四舍五入返回分钟后结果为：" + str(sum_dict)
    return {"result": result}


def math_avg(x: List[float], trans_time: bool = False):
    a = np.mean(x)

    result = "计算数据 %s 计算平均值结果: " % (str(x)) + str(a) + "秒"
    if trans_time:
        sum_dict = format_duration_minutes(a)
        result += "，将数据格式转化后得到：" + str(sum_dict)
    return {"result": result}


def math_mul(num1: float, num2: float) -> Dict:
    """计算两个数值之积"""
    return {"result": num1 * num2}


def math_div(num1: float, num2: float) -> Dict:
    """计算两个数值之商（num1 ÷ num2）"""
    if num2 == 0:
        return {"error": "除数不能为零"}
    return {"result": num1 / num2}


def math_percentage(a: float, b: float) -> Dict:
    """计算a占b的百分比（保留两位小数）"""
    if b == 0:
        return {"error": "基数不能为零"}
    return {"percentage": f"{round((a / b) * 100, 2)}%"}


# 格式化函数
def number_format(number: float, decimal: int = 2) -> Dict:
    """将数字格式化为指定小数位数（默认保留两位）"""
    return {"formatted": round(number, decimal)}


# 时长转换函数（已有）
def base_format_duration(seconds: float) -> Dict:
    """将秒数格式化为两种格式：HH小时XX分钟 和 XX分钟（保留小数点后两位）"""
    minutes = seconds / 60
    hours = int(minutes // 60)
    remaining_minutes = minutes % 60

    # 定义数字

    round_minutes = round(minutes)
    round_remaining = round(remaining_minutes)
    return {
        "HH小时XX分钟": f"{hours:02d}小时{round_remaining:02d}分钟",
        "XX分钟": f"{round_minutes:02d}分钟",
        "四舍五入整数分钟": f"{round_minutes:02d}分钟",
    }


def array_format_duration(x: List[float]) -> Dict:
    result = """"""

    for itx in x:
        block = base_format_duration(itx)
        result += "\n%f秒格式转化结果如下：" % x + str(block)
    return result


def data_count(x:List[str]):
    return len(x)

def data_summary(data_list):
    max_val = max(data_list)
    min_val = min(data_list)
    avg_val = sum(data_list) / len(data_list)
    total = sum(data_list)
    return f"输入数据分别为：{data_list}，最大值: {max_val}, 最小值: {min_val}, 平均值: {avg_val:.2f}, 总值: {total}"


tools_dict = [
    # 基础数学运算
    {
        "type": "function",
        "function": {
            "name": "math_add",
            "description": "计算数组之和,传入参数为数组数据数组(有可能数据为能耗，频率，次数)，返回数值求和结果，如果是计算分钟类数据，可以选择将其进行格式转化,转化成【XX分钟和XX小时XX分钟格式】，其他类型数据trans_time=false",
            "parameters": {
                "type": "object",
                "properties": {
                    "nums": {
                        "type": "array",
                        "items": {
                            "type": "number"
                        },
                        "description": "需要计算求和的数据列表，例如 [10.0, 20, 30]"
                    },
                    "trans_time": {"type": "boolean", "description": "是否转化成【XX分钟和XX小时XX分钟格式】，如果选项为true，则nums数据的单位为分钟"}
                },
                "required": ["nums","trans_time"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "math_sub",
            "description": "计算两个数值之差（num1 - num2），可以计算两个时段差值，例如A设备运行时长比B设备运行时长相差多少转化成【XX分钟和XX小时XX分钟格式】",
            "parameters": {
                "type": "object",
                "properties": {
                    "num1": {"type": "number"},
                    "num2": {"type": "number"},
                    "trans_time": {"type": "boolean", "description": "是否转化成【XX分钟和XX小时XX分钟格式】，如果选项为true，则nums数据的单位为分钟"},
                },
                "required": ["num1", "num2","trans_time"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "math_avg",
            "description": "计算一组数据的平均值",
            "parameters": {
                "type": "object",
                "properties": {
                    "x": {
                        "type": "array",
                        "items": {
                            "type": "number"
                        },
                        "description": "需要计算平均值的数据列表，例如 [10.0, 20, 30]"
                    },
                    "trans_time": {"type": "boolean", "description": "是否转化成【XX分钟和XX小时XX分钟格式】，如果选项为true，则nums数据的单位为分钟"},
                },
                "required": ["x","trans_time"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "date_sub",
            "description": "计算两个日期之间的时间差。例如：开机关机时间间隔，计算一个开机时长，注意动作筛选，深海作业A，分成两个阶段，下放阶段 以 ON DP 开始，OFF DP 结束，回收过程以 A架开机开始，A架关机结束。深海作业A以ON DP 开始，在OFF DP后，以回收阶段中A架关机结束",
            "parameters": {
                "type": "object",
                "properties": {
                    "date1": {
                        "type": "string",
                        "description": "开始日期时间，格式如 '2023-10-01 10:00:00'"
                    },
                    "date2": {
                        "type": "string",
                        "description": "结束日期时间，格式如 '2023-10-01 12:00:00'"
                    }
                },
                "required": ["date1", "date2"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "data_summary",
            "description": "返回数据组的最大值、最小值、平均值和总值。",
            "parameters": {
                "type": "object",
                "properties": {
                    "data_list": {
                        "type": "array",
                        "items": {
                            "type": "number"
                        },
                        "description": "数据列表，例如 [10, 20, 30]"
                    },
                },
                "required": ["data_list"]
            }
        }
    },
{
        "type": "function",
        "function": {
            "name": "data_count",
            "description": "计算数组的条数",
            "parameters": {
                "type": "object",
                "properties": {
                    "x": {
                        "type": "array",
                        "items": {
                            "type": "str"
                        },
                        "description": "字符串列表，用于计算数组的长度，可以用来 统计动作触发次数，例如 ['A架开机','A架开机'] 返回 2 "
                    },
                },
                "required": ["x"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "math_percentage",
            "description": "计算a占b的百分比比例",
            "parameters": {
                "type": "object",
                "properties": {
                    "a": {"type": "number", "description": "分子数值"},
                    "b": {"type": "number", "description": "分母数值"}
                },
                "required": ["a", "b"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "number_format",
            "description": "格式化数字到指定小数位数",
            "parameters": {
                "type": "object",
                "properties": {
                    "number": {"type": "number"},
                    "decimal": {"type": "integer", "description": "小数位数（默认2）"}
                },
                "required": ["number"]
            }
        }
    },
{
    "type": "function",
    "function": {
        "name": "json_format_list",
        "description": "[注意使用JSON格式输出时使用处理函数]将日期时间字符串列表转换为结构化JSON对象。根据日期时间字符串的格式提取日期作为键，时间作为值，若同一日期出现多次则添加序号（如'20210101_1'）。适用于json格式输出。",
        "parameters": {
            "type": "object",
            "properties": {
                "x": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "description": "日期时间字符串，支持以下格式：\n- 'YYYY-MM-DD'（如'2021-01-01'）\n- 'YYYY-MM-DD HH:MM'（如'2021-01-01 12:30'）\n- 'YYYY-MM-DD HH:MM:SS'（如'2021-01-01 12:30:45'）"
                    },
                    "description": "输入日期时间字符串列表，例如 ['2021-01-01 00:00', '2021-01-01 12:30:45']"
                }
            },
            "required": ["x"]
        },
        "returns": {
            "type": "string",
            "description": "JSON序列化字符串，结构示例：{'20210101_1': '00:00', '20210101_2': '12:30'}"
        }
    }
}
]

def agg_function_call(question,desc_ctx,final=False):
    system_prompt = """你需要根据用户问题调用函数：
    问题背景：
        深海作业A，分成两个阶段，
            1、下放阶段 以 ON DP 开始，OFF DP 结束，
            2、回收过程以 A架开机开始，A架关机结束
            3、作业A通常情况 以 ON DP 开始，以A架关机结束 
        （当计算深海作业A时长相关问题 需要分别计算两个时长间隔）
    1、计算进行函数调用,生成函数调用方法,可能涉及到时间点相机计算时长、能耗发电量求和求总量，油耗求和计算总量和油耗比较，请根据问题调用合适的函数，不要直接回答问题，
    案例：
    +--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| 折臂吊车 功 时间 (csvTime >= "2024-05-15 00:00:00") and (csvTime <= "2024-05-15 23:59:59")       | 0.0                                                                                     |
+--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| 折臂吊车 功 时间 (csvTime >= "2024-05-16 00:00:00") and (csvTime <= "2024-05-16 23:59:59")       | 16.817222222222224                                                                      |
+--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| A架-一号门架 功 时间 (csvTime >= "2024-05-15 00:00:00") and (csvTime <= "2024-05-15 23:59:59")   | 0.0                                                                                     |
+--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| A架-一号门架 功 时间 (csvTime >= "2024-05-16 00:00:00") and (csvTime <= "2024-05-16 23:59:59")   | 24.230833333333333                                                                      |
+--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| A架-二号门架 功 时间 (csvTime >= "2024-05-15 00:00:00") and (csvTime <= "2024-05-15 23:59:59")   | 0.0                                                                                     |
+--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| A架-二号门架 功 时间 (csvTime >= "2024-05-16 00:00:00") and (csvTime <= "2024-05-16 23:59:59")   | 35.471666666666664                                                                      |
+--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| 绞车 功 时间 (csvTime >= "2024-05-15 00:00:00") and (csvTime <= "2024-05-15 23:59:59")           | 0.0                                                                                     |
+--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| 绞车 功 时间 (csvTime >= "2024-05-16 00:00:00") and (csvTime <= "2024-05-16 23:59:59")           | 0.0                                                                                     |
+--------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------+
| 计算列表：[0.0, 16.817222222222224, 0.0, 24.230833333333333, 0.0, 35.471666666666664, 0.0, 0.0]  | 总量为:76.51972222222221最大值: 35.471666666666664 最小值:0.0 平均值:9.564965277777777  |
    上表为设备统计信息：
    问题：
    05-15到05-16 A架能耗是多少？ 记住能能耗类单位是kWh, 发电量是kWh,油耗单位是L
    /**
    A架包括一号门架和二号门架 调用函数 match_add 计算 [0.0，24.230833，0.0,35.471666] 值的和（无需时间转化）
    **/
    """
    user_prompt = f"""历史用户资料：
        {desc_ctx}
    子问题:
        {question}"""

    if final:
        user_prompt += """注意如果问题中未提到返回格式，则按照以下情况返回格式：
            1、如果是时长类问题，需要同时返回 XX秒，XX分钟， XX小时XX分钟格式
            2、还需要根据数据情况和问题进行汇总，调用函数解决，例如：17号 A架上午开机20分钟，下午开机30分钟，18号 A架上午开机30分钟，下午开机40分钟，需要总计所有时长 math_add <- [20,30,30,40] 并转化时间
                -  如果是普通情况例如能耗 例如：5月19日，A架-一号门架能耗为35.505 kWh，A架-二号门架能耗为42.55916666666667 kWh；5月20日，A架-一号门架能耗为16.605833333333333 kWh，A架-二号门架能耗为123.89055555555555 kWh。
                   则 math_add <- [35.505,42.55916666666667,16.605833333333333,123.89055555555555]， 不需要转化时间
            """

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]

    max_retries = 1
    retries = 0
    result_list = []
    error_messages = []

    while retries <= max_retries:
        # 调用API获取响应
        completion = client.chat.completions.create(
            model="glm-4-plus",
            messages=messages,
            tools=tools_dict,
            tool_choice="auto",
            temperature=0.
        )

        # 添加助理的响应到消息历史
        assistant_msg = completion.choices[0].message
        messages.append({
            "role": "assistant",
            "content": assistant_msg.content,
            "tool_calls": assistant_msg.tool_calls
        })

        current_errors = []
        current_results = []

        if assistant_msg.tool_calls:
            for func in assistant_msg.tool_calls:
                func_name = func.function.name
                args = func.function.arguments
                logger.info("调用函数: " + func_name + " 参数 " + args)
                try:
                    if func_name == "math_add":
                        function_result = math_add(**json.loads(args))
                    elif func_name == "math_sub":
                        function_result = math_sub(**json.loads(args))
                    elif func_name == "date_sub":
                        function_result = date_sub(**json.loads(args))
                    elif func_name == "data_summary":
                        function_result = data_summary(**json.loads(args))
                    elif func_name == "math_percentage":
                        function_result = math_percentage(**json.loads(args))
                    elif func_name == "number_format":
                        function_result = number_format(**json.loads(args))
                    elif func_name == "array_format_duration":
                        function_result = array_format_duration(**json.loads(args))
                    elif func_name == "math_avg":
                        function_result = math_avg(**json.loads(args))
                    elif func_name == "json_format_list":
                        function_result = json_format_list(**json.loads(args))
                    elif func_name == "data_count":
                        function_result = data_count(**json.loads(args))
                    else:
                        raise ValueError(f"未知函数 {func_name}")
                    current_results.append({
                        "function_name": func_name,
                        "function_result": function_result
                    })
                except Exception as e:
                    error_msg = f"Function {func_name} call failed: {str(e)}"
                    current_errors.append(error_msg)
                    logger.error(error_msg)

        # 处理错误和重试逻辑
        if current_errors:
            error_messages.extend(current_errors)
            if retries < max_retries:
                # 添加错误信息到消息历史并重试
                messages.append({
                    "role": "user",
                    "content": f"函数调用错误：{'; '.join(current_errors)}\n请修正参数后重试。"
                })
                retries += 1
                continue
        else:
            # 成功时收集结果
            result_list.extend(current_results)

        break  # 成功或达到最大重试次数时退出循环

    # 最终错误处理
    if error_messages:
        logger.error(f"最终函数调用失败，累计错误：{' | '.join(error_messages)}")
    return result_list if result_list else None







if __name__ == '__main__':
    date_pairs = [["2024-05-20 07:00:50", "2024-05-20 07:35:50"], ["2024-05-20 12:52:50", "2024-05-20 17:00:50"]]
    date_list = time_analysis_by_day(date_pairs)
    print(date_list)