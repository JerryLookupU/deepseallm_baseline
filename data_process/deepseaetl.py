import pandas as pd
import numpy as np
# ajia/征服者
from datetime import datetime,timedelta
import os
"data/v2/"
df_ajia_plc_1 = pd.read_csv("../data/v1/Ajia_plc_1_1.csv")
df_ajia_plc_2 = pd.read_csv("../data/v1/Ajia_plc_1_2.csv")

# dp
df_dp_plc_1 = pd.read_csv("../data/v1/Port3_ksbg_9_1.csv")
df_dp_plc_2 = pd.read_csv("../data/v1/Port3_ksbg_9_2.csv")

# 小艇 折臂吊车
df_xt_1 = pd.read_csv("../data/v1/device_13_11_meter_1311_1.csv")
df_xt_2 = pd.read_csv("../data/v1/device_13_11_meter_1311_2.csv")

# 相关数据字段
ajia_data_1 = df_ajia_plc_1[["Ajia-3_v", "Ajia-5_v", "csvTime"]]
ajia_data_2 = df_ajia_plc_2[["Ajia-3_v", "Ajia-5_v", "csvTime"]]

dp_plc_1 = df_dp_plc_1[["csvTime", "P3_33", "P3_18"]]
dp_plc_2 = df_dp_plc_2[["csvTime", "P3_33", "P3_18"]]

xt_1 = df_xt_1[["13-11-6_v", "csvTime"]]
xt_2 = df_xt_2[["13-11-6_v", "csvTime"]]

sz = len("2024-05-16 16:00")
ajia_data_1["csvTimeMinute"] = ajia_data_1["csvTime"].apply(lambda x: x[:sz])
ajia_data_2["csvTimeMinute"] = ajia_data_2["csvTime"].apply(lambda x: x[:sz])
dp_plc_1["csvTimeMinute"] = dp_plc_1["csvTime"].apply(lambda x: x[:sz])
dp_plc_2["csvTimeMinute"] = dp_plc_2["csvTime"].apply(lambda x: x[:sz])
xt_1["csvTimeMinute"] = xt_1["csvTime"].apply(lambda x: x[:sz])
xt_2["csvTimeMinute"] = xt_2["csvTime"].apply(lambda x: x[:sz])


def Ajia_zhuangtai(x):
    if x["Ajia-3_v"] != "error" and x["Ajia-5_v"] != "error":
        return 1
    else:
        return 0


def diaoche_zhuangtai(x):
    if x["13-11-6_v"] > 0:
        return 1
    else:
        return 0


def dp_status(x):
    if (x["P3_18_PRE"] == 0) and (x["P3_33_PRE"] == 0):
        if (x["P3_18"] > 0) or (x["P3_33"] > 0):
            return 1
        else:
            return 0
    if (x["P3_18"] > 0) or (x["P3_33"] > 0):
        return 1
    else:
        return 0


# 状态初始化
ajia_data_1["ajia_action"] = ""
ajia_data_2["ajia_action"] = ""
dp_plc_1["dp_action"] = ""
dp_plc_2["dp_action"] = ""
xt_1["xt_action"] = ""
xt_2["xt_action"] = ""

# 开机关机边界判断
ajia_status1 = ajia_data_1.apply(lambda x: Ajia_zhuangtai(x), axis=1).diff().fillna(0)
ajia_status2 = ajia_data_2.apply(lambda x: Ajia_zhuangtai(x), axis=1).diff().fillna(0)

ajia_data_1.loc[(ajia_status1 == 1), "ajia_action"] = "A架开机"
ajia_data_1.loc[(ajia_status1 == -1), "ajia_action"] = "A架关机"
ajia_data_2.loc[(ajia_status2 == 1), "ajia_action"] = "A架开机"
ajia_data_2.loc[(ajia_status2 == -1), "ajia_action"] = "A架关机"

# dp 开机关机判断
dp_plc_1["P3_33_PRE"] = dp_plc_1["P3_33"].shift(1)
dp_plc_1["P3_18_PRE"] = dp_plc_1["P3_18"].shift(1)
dp_plc_2["P3_33_PRE"] = dp_plc_2["P3_33"].shift(1)
dp_plc_2["P3_18_PRE"] = dp_plc_2["P3_18"].shift(1)

dp_status1 = dp_plc_1.apply(lambda x: dp_status(x), axis=1).diff().fillna(0)
dp_status2 = dp_plc_2.apply(lambda x: dp_status(x), axis=1).diff().fillna(0)

dp_plc_1.loc[(dp_status1 == 1), "dp_action"] = "ON DP"
dp_plc_1.loc[(dp_status1 == -1), "dp_action"] = "OFF DP"
dp_plc_2.loc[(dp_status2 == 1), "dp_action"] = "ON DP"
dp_plc_2.loc[(dp_status2 == -1), "dp_action"] = "OFF DP"

del dp_plc_1["P3_33_PRE"]
del dp_plc_1["P3_18_PRE"]
del dp_plc_2["P3_33_PRE"]
del dp_plc_2["P3_18_PRE"]

xt_status1 = xt_1.apply(lambda x: diaoche_zhuangtai(x), axis=1).diff().fillna(0)
xt_status2 = xt_2.apply(lambda x: diaoche_zhuangtai(x), axis=1).diff().fillna(0)
xt_1.loc[(xt_status1 == 1), "xt_action"] = "折臂吊车开机"
xt_1.loc[(xt_status1 == -1), "xt_action"] = "折臂吊车关机"
xt_2.loc[(xt_status2 == 1), "xt_action"] = "折臂吊车开机"
xt_2.loc[(xt_status2 == -1), "xt_action"] = "折臂吊车关机"


# 给定日期 判断 上午 还是 下午 上午为下放序列 下午为回收序列 如果当天仅有两组动作 则1为下放 2为回收

def find_peaks(df):
    peak_ids = []
    for i in range(1, len(df) - 1):  # 遍历除了第一个和最后一个的索引
        # 检查当前值是否比前后值都高
        if (float(df.loc[df.index[i], 'Ajia-3_v']) + float(df.loc[df.index[i], 'Ajia-5_v'])) > (
            float(df.loc[df.index[i - 1], 'Ajia-3_v']) + float(df.loc[df.index[i - 1], 'Ajia-5_v'])) and (
            float(df.loc[df.index[i], 'Ajia-3_v']) + float(df.loc[df.index[i], 'Ajia-5_v'])) > (
            float(df.loc[df.index[i + 1], 'Ajia-3_v']) + float(df.loc[df.index[i + 1], 'Ajia-5_v'])):
            # 检查峰值是否超过80
            if (float(df.loc[df.index[i], 'Ajia-3_v']) > 77 and
                float(df.loc[df.index[i], 'Ajia-5_v']) > 77):
                peak_id = df.index[i]
                peak_ids.append(peak_id)

    return peak_ids


ajia_data_1 = ajia_data_1.replace("error", "-1")
ajia_data_2 = ajia_data_2.replace("error", "-1")

ajia_data_1['Ajia-3_v'] = ajia_data_1['Ajia-3_v'].astype(float)
ajia_data_1['Ajia-5_v'] = ajia_data_1['Ajia-5_v'].astype(float)
ajia_data_2['Ajia-3_v'] = ajia_data_2['Ajia-3_v'].astype(float)
ajia_data_2['Ajia-5_v'] = ajia_data_2['Ajia-5_v'].astype(float)


# 找到 A架开机 关机的整个时间段
def search_up_down(df, index):
    cur_loc = df.index.get_loc(index)
    start_index = None
    end_index = None
    for i in range(cur_loc, -1, -1):
        t1 = float(df.loc[df.index[i], "Ajia-3_v"])
        t2 = float(df.loc[df.index[i], "Ajia-5_v"])
        if (t1 < 0) or (t1 < 0):
            break
        start_index = df.index[i]

    for i in range(cur_loc, len(df)):
        t1 = float(df.loc[df.index[i], "Ajia-3_v"])
        t2 = float(df.loc[df.index[i], "Ajia-5_v"])
        if (t1 < 0) or (t1 < 0):
            break
        end_index = df.index[i]
    if start_index is None:
        start_index = df.index[0]
    if end_index is None:
        end_index = df.index[len(df) - 1]
    return start_index, end_index


def peaks_split_data(df, peaks_indice):
    fields = set()
    for index in peaks_indice:
        start_index, end_index = search_up_down(df, index)
        fields.add((start_index, end_index))
    ret = list(fields)
    ret = sorted(ret, key=lambda x: x[0])
    return ret


# 通过索引切分
def data_fields_by_index(df, indices):
    result = []
    for start, end in indices:
        patch = df.loc[start:end]
        result.append(patch)
    return result


def TaskA_level(csvtime_minute):
    u_time = csvtime_minute[:10]
    if csvtime_minute < u_time + " 12:00":
        return "上午"
    else:
        return "下午"

def df_TaskA(df):
    start_time = df["csvTimeMinute"].tolist()[0]
    # print("日期",start_time[:10])
    return TaskA_level(start_time)


from datetime import datetime


def is_cross_am_pm(start_time, end_time, time_format="%Y-%m-%d %H:%M:%S"):
    # 定义时间格式（匹配输入的时间字符串格式）

    # 将字符串转换为 datetime 对象
    start = datetime.strptime(start_time, time_format)
    end = datetime.strptime(end_time, time_format)

    # 判断是否跨天
    if start.date() != end.date():
        return True, "时间跨天"

    # 定义中午时间
    noon = start.replace(hour=12, minute=0, second=0)

    # 判断是否跨越上午和下午
    if start < noon and end >= noon:
        return True, "时间跨越了上午和下午"
    else:
        return False, "时间没有跨越上午和下午"


def resub(task_fields):
    sub_df = []
    for item in task_fields:
        task_time = item["csvTime"].tolist()
        start_task = task_time[0]
        end_task = task_time[-1]
        ST = pd.to_datetime(start_task)
        ET = pd.to_datetime(end_task)
        max_hour = pd.to_datetime(item["csvTime"]).diff().dt.seconds.max()
        gap = (ET - ST).components.hours
        iscross, desc = is_cross_am_pm(start_task, end_task)
        if gap >= 2:
            item["groups"] = (pd.to_datetime(item["csvTime"]).diff() > pd.Timedelta(hours=1)).cumsum()
            gb = [i for _, i in item.groupby("groups")]
            for i in gb:
                del i["groups"]
            sub_df += gb
        else:
            sub_df += [item]
    return sub_df


# ajia_data_fields1
# 找到段落的开始时间是 下午还是早上

# 判断数据区段 小艇状态 如果无小艇状态则不考虑，

def get_item_start_end_time(item):
    timelist = item["csvTimeMinute"].tolist()
    return timelist[0], timelist[-1]


def xt_data_analysis(xt_item_data):
    if all(xt_item_data["13-11-6_v"] <= 7):
        # {"peaks_num":}
        return None, "折臂吊车无动作"
    row = xt_item_data["13-11-6_v"].tolist()
    data_sub = []
    peaks = []
    for idx in range(1, len(row) - 2):
        _curr = row[idx]
        _last = row[idx - 1]
        _next = row[idx + 1]

        if (_curr >= _last) and (_curr > _next) and (_curr >= 9):
            peaks.append(idx)

    return len(peaks), "时段内多个峰值"


def dp_data_analysis(dp_item_data):
    if len(dp_item_data[dp_item_data["P3_33"] > 0]) < 3:
        return None
    if len(dp_item_data[dp_item_data["P3_18"] > 0]) < 3:
        return None
    # 判断开机关机情况
    action_list = dp_item_data[dp_item_data["dp_action"] != ""]

    return len(action_list)


def ajia_time_gap(ajia_patch):
    """ajia 数据时间断层"""
    time_gap = pd.to_datetime(ajia_patch["csvTimeMinute"]).diff().dt.seconds.fillna(0).max()
    if time_gap > 3600:
        return None, "时间间隔大于1小时"

    return time_gap, "正常"


def chunk_analysis(ajia_data_fields, ajia_data, dp_data, xt_data):
    result = []
    status = []
    for i in ajia_data_fields:
        start_time, end_time = get_item_start_end_time(i)
        xt_patch = xt_data[(xt_data["csvTimeMinute"] >= start_time) & (xt_data["csvTimeMinute"] <= end_time)]
        xt_status, desc = xt_data_analysis(xt_patch)
        if xt_status is None:
            print(desc)
            continue
        dp_patch = dp_data[(dp_data["csvTimeMinute"] >= start_time) & (dp_data["csvTimeMinute"] <= end_time)]
        dp_status = dp_data_analysis(dp_patch)
        if dp_status is None:
            print("dp未启动")
            continue
        ajia_time_status, desc = ajia_time_gap(i)

        status.append(desc)
        result.append(i)
    return result, status


def info_desc_with_filter(ajia_data_fields, xt_data, dp_data):
    data_chunk = {}
    data_info = []
    for idx, item in enumerate(ajia_data_fields):
        df_time = item["csvTime"].tolist()
        start_time, end_time = get_item_start_end_time(item)
        date = start_time[:10]
        is_cross, desc1 = is_cross_am_pm(start_time, end_time, time_format="%Y-%m-%d %H:%M")
        xt_patch = xt_data[(xt_data["csvTimeMinute"] >= start_time) & (xt_data["csvTimeMinute"] <= end_time)]
        xt_status, desc2 = xt_data_analysis(xt_patch)
        dp_patch = dp_data[(dp_data["csvTimeMinute"] >= start_time) & (dp_data["csvTimeMinute"] <= end_time)]
        dp_status = dp_data_analysis(dp_patch)
        ajia_time_status, desc3 = ajia_time_gap(item)
        data_info.append({"id": idx, "date": date, "len": len(item), "start_time": start_time, "end_time": end_time,
                          "is_cross": is_cross, "xt_status": xt_status, "time_info": desc1, "xt_info": desc2,
                          "dp_info": dp_status, "time_gap": desc3})
        if xt_status is None:
            continue
        date_actions = data_chunk.get(date, [])
        date_actions.append(item)
        data_chunk[date] = date_actions

    return data_chunk, data_info


def filter_date_check_hight(ajia_data, date, data_items):
    ## 检查当天时段确实情况
    # 上午
    shangwu_start_time, shangwu_end_time = date + " 00:00:00", date + " 12:00:00"
    xiawu_start_time, xiawu_end_time = date + " 12:00:00", date + " 23:59:59"
    if len(data_items) == 2:
        return 1, "完整", data_items

    if len(data_items) == 1:
        # 如果日期在下午
        status = df_TaskA(data_items[0])
        # 如果早上数据有较大确实
        if status == "下午":
            x_patch = ajia_data[
                (ajia_data["csvTime"] >= shangwu_start_time) & (ajia_data["csvTime"] <= shangwu_end_time)]
            shangwu_status, desc = ajia_time_gap(x_patch)
            if shangwu_status is None:
                return 0, "回收", data_items
        if status == "上午":
            x_patch = ajia_data[(ajia_data["csvTime"] >= xiawu_start_time) & (ajia_data["csvTime"] <= xiawu_end_time)]
            xiawu_status, desc = ajia_time_gap(x_patch)
            if xiawu_status is None:
                return 0, "下放", data_items
        return -1, "未知", data_items
    return -1, "未知", data_items


def chunk_analysis(ajia_data, data_chunk):
    sucess_chunks = {}
    error_chunks = {}

    for date, items in data_chunk.items():
        status_code, status_name, items = filter_date_check_hight(ajia_data, date, items)
        if status_name == "完整":
            sucess_chunks[date + " 下放"] = items[0]
            sucess_chunks[date + " 回收"] = items[1]
            continue
        if status_name == "下放" or status_name == "回收":
            sucess_chunks[date + " " + status_name] = items[0]
            continue
        if status_name == "未知":
            error_chunks[date + " " + status_name] = items
            continue
    return sucess_chunks, error_chunks


def find_hight_low_hight(task_item, index):
    """ 找到数值回落50后 又重新达到峰值的点位  """
    curr_point = task_item.index.get_loc(index)

    find_fifty = None
    find_hight = None
    p = curr_point
    for idx in range(curr_point, -1, -1):
        """"""
        row = task_item.loc[task_item.index[idx]]
        v3_ = row["Ajia-3_v"]
        v5_ = row["Ajia-5_v"]
        if (v5_ < 60) and (v3_ < 60):
            find_fifty = True
            p = idx
            continue
        if (v5_ > 78) and (v3_ > 78):
            if find_fifty:
                find_hight = True
                break
    if p == curr_point:
        return None
    return task_item.index[p]


# 窗口序列判断
def find_best_peaks(df):
    peak_ids = []
    for i in range(1, len(df) - 1):  # 遍历除了第一个和最后一个的索引
        if (float(df.loc[df.index[i], 'Ajia-3_v']) + float(df.loc[df.index[i], 'Ajia-5_v'])) > (
            float(df.loc[df.index[i - 1], 'Ajia-3_v']) + float(df.loc[df.index[i - 1], 'Ajia-5_v'])) and (
            float(df.loc[df.index[i], 'Ajia-3_v']) + float(df.loc[df.index[i], 'Ajia-5_v'])) > (
            float(df.loc[df.index[i + 1], 'Ajia-3_v']) + float(df.loc[df.index[i + 1], 'Ajia-5_v'])):
            if (float(df.loc[df.index[i], 'Ajia-3_v']) > 78 and
                float(df.loc[df.index[i], 'Ajia-5_v']) > 78):
                peak_id = df.index[i]
                peak_value = float(df.loc[df.index[i], 'Ajia-3_v']) + float(df.loc[df.index[i], 'Ajia-5_v'])
                peak_ids.append((peak_id, peak_value))
    if len(peak_ids) == 0:
        return None
    if len(peak_ids) == 1:
        return peak_ids[0][0]
    peak_ids = sorted(peak_ids, key=lambda x: x[1], reverse=True)
    return peak_ids[0][0]


def dp_search(dpdf, seated_time):
    """找到dp 开启还是dp关闭点位 """
    seated_point = dpdf[dpdf["csvTimeMinute"] == seated_time].index[-1]
    t1 = float(dpdf.loc[seated_point, "P3_33"])
    t2 = float(dpdf.loc[seated_point, "P3_18"])

    # 如果 t1 t2 均 == 0 则向上查询
    # 如果 t1 t2 均 >0 则向上 向下查询
    start_dp_index = None
    end_dp_index = None
    if (t1 == 0) or (t2 == 0):
        off_dp = dpdf[dpdf["csvTimeMinute"] <= seated_time]
        off_dp = off_dp[::-1]

        for i in off_dp.index:
            t3 = float(off_dp.loc[i, "P3_33"])
            t4 = float(off_dp.loc[i, "P3_18"])
            if (t3 == 0) or (t4 == 0):
                end_dp_index = i
            elif (t3 > 0) and (t4 > 0):
                break

        # 基于 off dp 查找 on dp
        end_dp_time = dpdf.loc[end_dp_index]["csvTimeMinute"]

        on_dp = dpdf[dpdf["csvTimeMinute"] < end_dp_time]
        on_dp = on_dp[::-1]
        for i in on_dp.index:
            t3 = float(off_dp.loc[i, "P3_33"])
            t4 = float(off_dp.loc[i, "P3_18"])
            if (t3 > 0) or (t4 > 0):
                start_dp_index = i
            elif (t3 == 0) and (t4 == 0):
                break
        return start_dp_index, end_dp_index

    on_dp = dpdf[dpdf["csvTimeMinute"] <= seated_time]
    on_dp = on_dp[::-1]
    for i in on_dp.index:
        t3 = float(on_dp.loc[i, "P3_33"])
        t4 = float(on_dp.loc[i, "P3_18"])
        if (t3 == 0) and (t4 == 0):
            break
        start_dp_index = i

    off_dp = dpdf[dpdf["csvTimeMinute"] >= seated_time]
    end_dp_index = seated_point
    for i in off_dp.index:
        t3 = float(off_dp.loc[i, "P3_33"])
        t4 = float(off_dp.loc[i, "P3_18"])
        if (t3 == 0) or (t4 == 0):
            end_dp_index = i
            break

    return start_dp_index, end_dp_index


def action_check(action_list):
    # 序列时间段 不超过 2个小时
    action_df = pd.DataFrame(action_list)
    diff_act = pd.to_datetime(action_df["csvTime"]).diff().dt.seconds.fillna(0).sum()
    if diff_act > 7200:
        return False
    else:
        return True


def check_ajia_tv(task_item, start, end):
    """检测 A架该段数据时间段完整性，以及 数值状态"""
    desc = ""
    isok = True
    patch = task_item.loc[start:end]
    diff_time = pd.to_datetime(patch["csvTime"]).diff().dt.seconds.fillna(0)
    max_diff_time = diff_time.max()
    diff_time_sum = sum(diff_time)
    if max_diff_time > 3600:
        desc += "时间数据不完整，间隔超1小时\n"
        isok = False
    if diff_time_sum > 3600:
        desc += "作业时间间隔过久\n"
        isok = False
    v3_z = sum(patch["Ajia-3_v"] == 0)
    v5_z = sum(patch["Ajia-5_v"] == 0)
    desc += "时段内3_v待机数据存在： %s条;5_v 待机数据存在： %s 条" % (v3_z, v5_z)
    return isok, desc


def check_xt_tv(task_item):
    """检查小艇数据时段是否正常"""
    isok = True
    patch = task_item
    desc = ""
    diff_time = pd.to_datetime(patch["csvTime"]).diff().dt.seconds.fillna(0)
    max_diff_time = diff_time.max()
    diff_time_sum = sum(diff_time)
    if max_diff_time > 3600:
        desc += "时间数据不完整，间隔超1小时\n"
        isok = False
    if diff_time_sum > 3600:
        desc += "作业时间间隔过久\n"
        isok = False
    return isok, desc


def find_fifty_to_hight(task_item, end_index, check_hight=True):
    # 找到当前点 判断是否为高值
    current_index = None
    curr_point = task_item.index.get_loc(end_index)
    if check_hight:
        t1 = float(task_item.loc[end_index, 'Ajia-3_v'])
        t2 = float(task_item.loc[end_index, 'Ajia-5_v'])
        if (t1 < 65) and (t2 < 65):
            return None, "当前数值小于65"

    for idx in range(curr_point - 1, -1, -1):
        t1 = float(task_item.loc[task_item.index[idx], 'Ajia-3_v'])
        t2 = float(task_item.loc[task_item.index[idx], 'Ajia-5_v'])
        # 找到数值  按照 都增加情况考虑
        if (t1 >= 60) and (t2 >= 60):
            current_index = task_item.index[idx]
            continue
        if (t1 < 60) or (t2 < 60):
            return current_index, "通过"
    return current_index, "无法找到开始增加的点"


def find_peak_index(data, base_state=7):
    peak_index = -1
    peak_value = base_state
    in_peak = False
    wait_for_base = True  # 初始时需要等待回落到基础状态

    for i, value in data.items():
        if wait_for_base:
            if value <= base_state:
                wait_for_base = False  # 回落到基础状态，可以开始计算峰值
            continue  # 忽略初始非基础状态的值

        if value > base_state:
            if not in_peak:
                in_peak = True  # 开始一个新的峰值段
            if value > peak_value:
                peak_value = value
                peak_index = i
        elif in_peak and value <= base_state:
            break  # 回落到基础状态，结束当前峰值段
    return peak_index


def pre_last_index(data, base_state=7):
    pre_index = -1
    pre_value = base_state
    wait_for_base = False
    for i, value in data.items():
        if not wait_for_base:
            if value <= base_state:
                wait_for_base = True
        if (value > base_state) and wait_for_base:
            # 回落之后 重新达到峰值
            return i
    return pre_index


def xt_search_jiancha(xt_patch):
    """搜索到小艇检查时间，时间点从小艇入水往前计算"""
    find_low = False
    find_hight = False
    xt_patch_rev = xt_patch[::-1]
    peak_index = pre_last_index(xt_patch_rev["13-11-6_v"])
    if peak_index == -1:
        return None, "无法找到小艇检查完毕时间点\n"
    return peak_index, ""


# def xt_prehight(xt_patch):
#     """找到小艇入水时间，即从起吊往前倒推,找到最高值版本"""
#     xt_patch_rev = xt_patch[::-1]
#     find_hight = False
#     hight_value = None
#     hight_index = None
#     for idx in xt_patch_rev.index:
#         point = xt_patch.loc[idx,"13-11-6_v"]
#         if point > 7:
#             if find_hight is False:
#                 find_hight = True
#                 hight_value = point
#                 hight_index = idx
#                 continue
#             else:
#                 # 比较高值和当前值
#                 if hight_value >= point:
#                     # 表示回落
#                     return hight_index
#                 else:
#                     # 表示上升
#                     hight_index = idx
#                     hight_value = point
#         else:
#             if find_hight:
#                 return hight_index
#             else:
#                 continue
#     return None

def xt_prehight(n):
    """找到小艇入水时间，即从起吊往前倒推"""
    xt_patch_rev = n[::-1]
    for idx in xt_patch_rev.index:
        point = xt_patch_rev.loc[idx, "13-11-6_v"]
        if point > 7:
            return idx
    return None


def reduce_one_minute_second(time_str):
    # 将字符串转换为datetime对象
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

    # 减少一分钟
    dt = dt - timedelta(minutes=1)

    # 将datetime对象转换回字符串
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def after_one_hour(time_str):
    # 将字符串转换为datetime对象
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

    # 1小时后
    dt = dt + timedelta(hours=1)

    # 将datetime对象转换回字符串
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def reduce_one_minute(time_str):
    # 将字符串转换为datetime对象
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M")

    # 减少一分钟
    dt = dt - timedelta(minutes=1)

    # 将datetime对象转换回字符串
    return dt.strftime("%Y-%m-%d %H:%M")


def xt_first_hight(xt_patch):
    xt_patch_rev = xt_patch[::-1]
    curr_idx = None
    v = None
    for idx in xt_patch_rev.index:
        curr_value = xt_patch.loc[idx, "13-11-6_v"]
        if v is None:
            v = curr_value
            curr_idx = idx
            continue
        if curr_value > v:
            v = curr_value
            curr_idx = idx
        else:
            return curr_idx
    return None


def xt_guangji(xt_item):
    shutdown_start_time = None
    for i in range(len(xt_item) - 1):
        if xt_item.loc[xt_item.index[i], '13-11-6_v'] == 0:
            shutdown_start_time = xt_item.index[i]
            break  # 找到第一个符合条件的点后退出循环
    return shutdown_start_time


def get_time_one_hour_earlier(dt_str, time_format="%Y-%m-%d %H:%M:%S"):
    """
    获取给定时间字符串的 1 小时之前的时间。

    参数:
        dt_str (str): 时间字符串，例如 "2024-05-16 10:00:00"。
        time_format (str): 时间字符串的格式，默认为 "%Y-%m-%d %H:%M:%S"。

    返回:
        str: 1 小时之前的时间字符串，格式与输入相同。
    """
    # 将字符串转换为 datetime 对象
    dt = datetime.strptime(dt_str, time_format)

    # 计算 1 小时之前的时间
    one_hour_earlier = dt - timedelta(hours=1)

    # 将结果转换回字符串格式
    one_hour_earlier_str = one_hour_earlier.strftime(time_format)

    return one_hour_earlier_str


def before_one_half_hour(time_str):
    # 将字符串转换为datetime对象
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

    # 1小时后
    dt = dt - timedelta(hours=1, minutes=30)

    # 将datetime对象转换回字符串
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def after_half_hour(time_str):
    # 将字符串转换为datetime对象
    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

    # 1小时后
    dt = dt + timedelta(minutes=30)

    # 将datetime对象转换回字符串
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def search_xt_actions(xt_data, middle_time, aname="下放"):
    """middle_time 为缆绳解除 或缆绳挂妥时间点 csvTime"""
    xt_action_list = []
    desc = ""
    ### 根据middle_time 小艇相关时间
    # print("开始搜索小艇入水时间，搜索时间")
    # xt_start_time = before_one_half_hour(middle_time)
    # xt_end_time = after_half_hour(middle_time)
    # xt_patch = xt_data[(xt_data["csvTime"] >= xt_start_time) &(xt_data["csvTime"] < middle_time)]

    if len(middle_time) == len("2024-01-01 00:00"):
        middle_time = middle_time + ":59"

    xt_patch = xt_data[xt_data["csvTime"] < middle_time]

    xt_rushui_index = xt_prehight(xt_patch)
    if xt_rushui_index is None:
        return xt_action_list, "无法查询到小艇入水点位"
    print("==================")
    print("成功找到小艇入水位置点,", xt_rushui_index)
    print("==================")
    xt_rushui_csvTime = xt_patch.loc[xt_rushui_index]["csvTime"]
    xt_rushui_csvTime_minute = xt_patch.loc[xt_rushui_index]["csvTimeMinute"]
    xt_action_list.append(
        {"csvTimeMinute": xt_rushui_csvTime_minute, "actionName": "小艇入水", "deviceName": "折臂吊车",
         "actionType": aname, "csvTime": xt_rushui_csvTime})
    ## 搜索小艇检查完毕点位
    # 获取需要查询的小艇数据片段
    #######################################################################################################################################################################
    xt_patch_jiancha = xt_patch[xt_patch["csvTime"] < xt_rushui_csvTime]
    xt_patch_jiancha_index, xt_desc = xt_search_jiancha(xt_patch_jiancha)
    if xt_patch_jiancha_index is None:
        """有可能没有小艇检查时间"""
        isok, desc1 = check_xt_tv(xt_patch_jiancha)
        desc = desc + xt_desc + desc1
    else:
        xt_jiancha_csvTime = xt_patch.loc[xt_patch_jiancha_index]["csvTime"]
        xt_jiancha_csvTime_minute = xt_patch.loc[xt_patch_jiancha_index]["csvTimeMinute"]
        xt_action_list.append(
            {"csvTimeMinute": xt_jiancha_csvTime_minute, "actionName": "小艇检查完毕", "deviceName": "折臂吊车",
             "actionType": aname, "csvTime": xt_jiancha_csvTime})
        print("==================")
        print("成功找到小艇检查完毕位置点")
        print("==================")

    xt_patch_guanji = xt_data[xt_data["csvTime"] > middle_time]

    guanji_index = xt_guangji(xt_patch_guanji)
    if guanji_index is None:
        isok, desc3 = check_xt_tv(xt_patch_guanji)
        desc = desc + desc3 + "无法找到折臂吊车关机点"
    else:
        xt_guanji_csvTime = xt_data.loc[guanji_index]["csvTime"]
        xt_guanji_csvTime_minute = xt_data.loc[guanji_index]["csvTimeMinute"]
        xt_action_list.append(
            {"csvTimeMinute": xt_guanji_csvTime_minute, "actionName": "折臂吊车关机", "deviceName": "折臂吊车",
             "actionType": aname, "csvTime": xt_guanji_csvTime})

    if guanji_index is not None:
        xt_luozuo_patch = xt_data[(xt_data["csvTime"] >= middle_time) & (xt_data["csvTime"] <= xt_guanji_csvTime)]
        ## 找到落座时间点
        xt_luozuo_index = xt_prehight(xt_luozuo_patch)
        if xt_luozuo_index is None:
            print("A架电流峰值时间：", middle_time)
            print("关机时间：", xt_guanji_csvTime)
        xt_luozuo_csvTime = xt_data.loc[xt_luozuo_index]["csvTime"]
        xt_luozuo_csvTime_minute = xt_data.loc[xt_luozuo_index]["csvTimeMinute"]
        xt_action_list.append(
            {"csvTimeMinute": xt_luozuo_csvTime_minute, "actionName": "小艇落座", "deviceName": "折臂吊车",
             "actionType": aname, "csvTime": xt_luozuo_csvTime})
    else:
        desc += "无法找到小艇落座点"
    return xt_action_list, desc


def task_xiafang(ajia_data, dp_data, xt_data, task_item):
    action_list = []

    peaks_nums = find_peaks(task_item)
    if len(peaks_nums) <= 2:
        return action_list, "找不到峰值数据"

    clist = task_item["csvTime"].tolist()
    print("开始处理下放数据，日期为: ", clist[0][:10])
    desc = ""
    peak_id = find_best_peaks(task_item)
    if peak_id is None:
        return action_list, "找不到峰值数据"
    # 下放过程 峰值ID 为 A架摆回
    baihui_point_value = task_item.loc[peak_id]
    baihui_csvTime = task_item.loc[peak_id]["csvTime"]
    baihui_csvTime_minute = task_item.loc[peak_id]["csvTimeMinute"]
    # insert data
    action_list.append(
        {"csvTimeMinute": baihui_csvTime_minute, "actionName": "A架摆回", "deviceName": "A架", "actionType": "下放",
         "csvTime": baihui_csvTime})
    # 向上获取缆绳解除点
    print("==================")
    print("成功找到A架摆回位置点:", ajia_data.loc[peak_id])
    print("==================")
    lsjc_id = find_hight_low_hight(task_item, peak_id)
    if lsjc_id is None:
        xt_action_list, xt_desc = search_xt_actions(xt_data, baihui_csvTime)
        action_list += xt_action_list
        isok, desc = check_ajia_tv(task_item, task_item.index[0], peak_id)
        action_list = sorted(action_list, key=lambda x: x["csvTime"], reverse=False)
        return action_list, "找不到拦缆绳解除点位\n" + desc
    if lsjc_id == 0:
        return action_list, "缆绳解除后，找不到前序点位\n"

    lsjc_point_value = task_item.loc[lsjc_id]
    lsjc_csvTime = task_item.loc[lsjc_id]["csvTime"]
    lsjc_csvTime_minute = task_item.loc[lsjc_id]["csvTimeMinute"]
    # insert data
    print("==================")
    print("成功找到缆绳解除位置点:", ajia_data.loc[lsjc_id])
    print("==================")
    action_list.append(
        {"csvTimeMinute": lsjc_csvTime_minute, "actionName": "缆绳解除", "deviceName": "A架", "actionType": "下放",
         "csvTime": lsjc_csvTime})
    # 开机关机
    start_index = task_item.index[0]
    end_pre = task_item.index[-1]
    end_loc = ajia_data.index.get_loc(end_pre) + 1
    end_index = ajia_data.index[end_loc]

    # if start_index != -1:
    #     ajia_kaiji_csvTime = ajia_data.loc[start_index]["csvTime"]
    #     ajia_kaiji_csvTime_minute = ajia_data.loc[start_index]["csvTimeMinute"]
    #     action_list.append({"csvTimeMinute": ajia_kaiji_csvTime_minute, "actionName": "A架开机", "deviceName": "A架",
    #                         "actionType": "下放", "csvTime": ajia_kaiji_csvTime})
    # if end_index != -1:
    #     ajia_guanji_csvTime = ajia_data.loc[end_index]["csvTime"]
    #     ajia_guanji_csvTime_minute = ajia_data.loc[end_index]["csvTimeMinute"]
    #     action_list.append({"csvTimeMinute": ajia_guanji_csvTime_minute, "actionName": "A架关机", "deviceName": "A架",
    #                         "actionType": "下放", "csvTime": ajia_guanji_csvTime})
    # 开机关机

    zfz_fake_rushui_id = task_item.index[task_item.index.get_loc(lsjc_id) - 1]
    # 检查点位

    # zfz_rushui_csvTime = task_item.loc[zfz_rushui_id]["csvTime"]
    # zfz_rushui_csvTime_minute =  task_item.loc[zfz_rushui_id]["csvTimeMinute"]
    zfz_rushui_csvTime = reduce_one_minute_second(lsjc_csvTime)
    zfz_rushui_csvTime_minute = zfz_rushui_csvTime[:16]

    action_list.append({"csvTimeMinute": zfz_rushui_csvTime_minute, "actionName": "征服者入水", "deviceName": "A架",
                        "actionType": "下放", "csvTime": zfz_rushui_csvTime})

    # 查找 征服者起吊点位
    zfz_qidiao_id, desc_qdiao = find_fifty_to_hight(task_item, zfz_fake_rushui_id)
    if zfz_qidiao_id is None:

        start_dp_index, end_dp_index = dp_search(dp_data, lsjc_csvTime_minute)
        if (start_dp_index is None) or (end_dp_index is None):
            desc += "作业过程dp数据不完整"

        else:
            dp_start_csvTime = dp_data.loc[start_dp_index]["csvTime"]
            dp_start_csvTime_minute = dp_data.loc[start_dp_index]["csvTimeMinute"]
            dp_end_csvTime = dp_data.loc[end_dp_index]["csvTime"]
            dp_end_csvTime_minute = dp_data.loc[end_dp_index]["csvTimeMinute"]

            action_list.append({"csvTimeMinute": dp_start_csvTime_minute, "actionName": "ON DP", "deviceName": "DP",
                                "actionType": "下放", "csvTime": dp_start_csvTime})
            action_list.append({"csvTimeMinute": dp_end_csvTime_minute, "actionName": "OFF DP", "deviceName": "DP",
                                "actionType": "下放", "csvTime": dp_end_csvTime})

        xt_action_list, xt_desc = search_xt_actions(xt_data, lsjc_csvTime)
        action_list += xt_action_list
        action_list = sorted(action_list, key=lambda x: x["csvTime"], reverse=False)

        return action_list, "无法找到征服者起吊点位"

    isok, desc0 = check_ajia_tv(ajia_data, zfz_qidiao_id, zfz_fake_rushui_id)
    if not isok:
        return action_list, desc

    # 通过检测获取 起吊数据点位
    zfz_qidiao_csvTime = task_item.loc[zfz_qidiao_id]["csvTime"]
    zfz_qidiao_csvTime_minute = task_item.loc[zfz_qidiao_id]["csvTimeMinute"]
    action_list.append({"csvTimeMinute": zfz_qidiao_csvTime_minute, "actionName": "征服者起吊", "deviceName": "A架",
                        "actionType": "下放", "csvTime": zfz_qidiao_csvTime})
    print("==================")
    print("成功找到起吊位置点")
    print("==================")
    print("起吊位置 和 入水位置 之间时段检测 起吊：", zfz_qidiao_id, " 入水：", zfz_fake_rushui_id, "缆绳解除时间：",
          lsjc_csvTime)
    xt_action_list, xt_desc = search_xt_actions(xt_data, lsjc_csvTime)
    action_list += xt_action_list
    action_list = sorted(action_list, key=lambda x: x["csvTime"], reverse=False)
    isok = action_check(action_list)
    if isok:
        """继续dp数据"""
        start_dp_index, end_dp_index = dp_search(dp_data, zfz_qidiao_csvTime_minute)
        if (start_dp_index is None) or (end_dp_index is None):
            desc += "作业过程dp数据不完整"
            return None, desc
        else:
            dp_start_csvTime = dp_data.loc[start_dp_index]["csvTime"]
            dp_start_csvTime_minute = dp_data.loc[start_dp_index]["csvTimeMinute"]
            dp_end_csvTime = dp_data.loc[end_dp_index]["csvTime"]
            dp_end_csvTime_minute = dp_data.loc[end_dp_index]["csvTimeMinute"]

            action_list.append({"csvTimeMinute": dp_start_csvTime_minute, "actionName": "ON DP", "deviceName": "DP",
                                "actionType": "下放", "csvTime": dp_start_csvTime})
            action_list.append({"csvTimeMinute": dp_end_csvTime_minute, "actionName": "OFF DP", "deviceName": "DP",
                                "actionType": "下放", "csvTime": dp_end_csvTime})
            action_list = sorted(action_list, key=lambda x: x["csvTime"], reverse=False)
            return action_list, desc
    else:
        desc += "\n{动作时间跨度过大}"
        return None, desc
    #################################################################################################################################################################


# huishou_example = date_success_chunks1["2024-05-19 回收"]


def xt_action_gettime(xt_actions, key):
    for row in xt_actions:
        if row["actionName"] == key:
            return row
    return None


def find_baichu(task_item, search_time):
    #
    # find_zero = False
    ajia_patch = task_item[task_item["csvTime"] < search_time]
    peak_index = find_best_peaks(ajia_patch)
    return peak_index


def find_zero(task_item, search_time):
    before_search_time = before_one_half_hour(search_time)
    ajia_patch = task_item[(task_item["csvTime"] < search_time) & (task_item["csvTime"] > before_search_time)]
    ajia_patch_rev = ajia_patch[::-1]
    for i in ajia_patch_rev.index:
        t1 = float(task_item.loc[i, 'Ajia-3_v'])
        t2 = float(task_item.loc[i, 'Ajia-5_v'])
        if t1 == 0 and t2 == 0:
            return i
    return None


def find_fifty_huishou(task_item, search_time):
    ajia_patch = task_item[(task_item["csvTime"] >= search_time)]
    cur = None
    for i in ajia_patch.index:
        t1 = float(task_item.loc[i, 'Ajia-3_v'])
        t2 = float(task_item.loc[i, 'Ajia-5_v'])
        if t1 < 60 and t2 < 60:
            return cur
        cur = i
    return cur


def ajia_search_io(ajia_data, search_time):
    # 从这个时间点出发 向上查找开机 向下查找关机
    up_half = ajia_data[ajia_data["csvTime"] < search_time]
    up_half = up_half[::-1]
    down_half = ajia_data[ajia_data["csvTime"] > search_time]
    kaiji_index = -1
    guanji_index = -1
    for i in up_half.index:
        t1 = float(ajia_data.loc[i, 'Ajia-3_v'])
        t2 = float(ajia_data.loc[i, 'Ajia-5_v'])
        if t1 == -1 or t2 == -1:
            break
        kaiji_index = i
    for j in down_half.index:
        t1 = float(ajia_data.loc[j, 'Ajia-3_v'])
        t2 = float(ajia_data.loc[j, 'Ajia-5_v'])
        if t1 == -1 and t2 == -1:
            guanji_index = j
            break
    return kaiji_index, guanji_index


def task_huishou(ajia_data, dp_data, xt_data, task_item):
    """回收动作状态"""
    # print(df_TaskA(task_item))
    action_list = []

    peaks_nums = find_peaks(task_item)
    if len(peaks_nums) <= 2:
        return action_list, "找不到峰值数据"

    clist = task_item["csvTime"].tolist()
    desc = []
    zfz_chushui_index = find_best_peaks(task_item)
    if zfz_chushui_index is None:
        return None, "找不到峰值数据"

    zfz_chushui_csvTime = task_item.loc[zfz_chushui_index]["csvTime"]
    zfz_chushui_csvTime_minute = task_item.loc[zfz_chushui_index]["csvTimeMinute"]

    guotuo_fake_loc = ajia_data.index.get_loc(zfz_chushui_index) - 1
    guotuo_fake_index = ajia_data.index[guotuo_fake_loc]
    langshen_guatuo_csvTime = reduce_one_minute_second(zfz_chushui_csvTime)
    langshen_guatuo_csvTime_minute = langshen_guatuo_csvTime[:16]
    action_list.append({"csvTimeMinute": zfz_chushui_csvTime_minute, "actionName": "征服者出水", "deviceName": "A架",
                        "actionType": "回收", "csvTime": zfz_chushui_csvTime})
    action_list.append({"csvTimeMinute": langshen_guatuo_csvTime_minute, "actionName": "缆绳挂妥", "deviceName": "A架",
                        "actionType": "回收", "csvTime": langshen_guatuo_csvTime})

    xt_action_list, xt_desc = search_xt_actions(xt_data, zfz_chushui_csvTime, aname="回收")

    action_list += xt_action_list
    ### 找到 A架摆出位置
    desc.append(xt_desc)
    xt_rushui_row = xt_action_gettime(xt_action_list, key="小艇入水")
    ajia_baichu_index = None
    if xt_rushui_row is None:
        desc.append("无法找到小艇入水位置")
        # zero_index = find_zero(ajia_data,langshen_guatuo_csvTime)
        # if zero_index is not None:
        #     zero_csvtime = task_item.loc[zero_index]["csvTime"]
        #     ajia_baichu_index = find_baichu(task_item,zero_csvtime)

    else:
        xt_rushui_csvTime = xt_rushui_row["csvTime"]
        ajia_baichu_index = find_baichu(task_item, xt_rushui_csvTime)

    if ajia_baichu_index is None:
        desc.append("无法找到A架摆出位置")
    else:
        ajia_baichu_csvTime = task_item.loc[ajia_baichu_index]["csvTime"]
        ajia_baichu_csvTime_minute = task_item.loc[ajia_baichu_index]["csvTimeMinute"]
        action_list.append({"csvTimeMinute": ajia_baichu_csvTime_minute, "actionName": "A架摆出", "deviceName": "A架",
                            "actionType": "回收", "csvTime": ajia_baichu_csvTime})
    # 找到落座位置
    zfz_luozuo_index = find_fifty_huishou(task_item, zfz_chushui_csvTime)
    if zfz_luozuo_index is not None:
        zfz_luozuo_csvTime = task_item.loc[zfz_luozuo_index]["csvTime"]
        zfz_luozuo_csvTime_minute = task_item.loc[zfz_luozuo_index]["csvTimeMinute"]
        action_list.append({"csvTimeMinute": zfz_luozuo_csvTime_minute, "actionName": "征服者落座", "deviceName": "A架",
                            "actionType": "回收", "csvTime": zfz_luozuo_csvTime})
    else:
        desc.append("无法找到征服者落座点")
    # 补充作业 A架开机关机
    # start_index,end_index = ajia_search_io(ajia_data,zfz_chushui_csvTime)
    start_index = task_item.index[0]
    end_pre = task_item.index[-1]
    end_loc = ajia_data.index.get_loc(end_pre) + 1
    end_index = ajia_data.index[end_loc]
    # 开关机单独导入导出
    # if start_index  != -1:
    #     ajia_kaiji_csvTime = ajia_data.loc[start_index]["csvTime"]
    #     ajia_kaiji_csvTime_minute = ajia_data.loc[start_index]["csvTimeMinute"]
    #     action_list.append({"csvTimeMinute":ajia_kaiji_csvTime_minute,"actionName":"A架开机","deviceName":"A架","actionType":"回收","csvTime":ajia_kaiji_csvTime})
    # if end_index!= -1:
    #     ajia_guanji_csvTime = ajia_data.loc[end_index]["csvTime"]
    #     ajia_guanji_csvTime_minute = ajia_data.loc[end_index]["csvTimeMinute"]
    #     action_list.append({"csvTimeMinute":ajia_guanji_csvTime_minute,"actionName":"A架关机","deviceName":"A架","actionType":"回收","csvTime":ajia_guanji_csvTime})
    return action_list, ",".join(desc)


#
# 找到所有峰值段落，表示 有可能进行 深海作业A
peaks_indices1 = find_peaks(ajia_data_1)
peaks_indices2 = find_peaks(ajia_data_2)

## 根据峰值 分割数据
ajia_dataset1 = peaks_split_data(ajia_data_1,peaks_indices1)
ajia_dataset2 = peaks_split_data(ajia_data_2,peaks_indices2)

# 根据峰值 上下划分数据段
ajia_fields1 = data_fields_by_index(ajia_data_1,ajia_dataset1)
ajia_fields2 = data_fields_by_index(ajia_data_2,ajia_dataset2)

# 根据断层数据 对数据段进行再划分
ajia_data_fields1 = resub(ajia_fields1)
ajia_data_fields2 = resub(ajia_fields2)

# 过滤 无折臂吊车的动作段 排除 深海作业A
date_chunk1,date_info1 = info_desc_with_filter(ajia_data_fields1,xt_1,dp_plc_1)
date_chunk2,date_info2 = info_desc_with_filter(ajia_data_fields2,xt_2,dp_plc_2)
## 获取出能够进行 深海作业A的日期
print(date_chunk1["2024-05-16"])

#
# date_success_chunks1,date_error_chunks1 = chunk_analysis(ajia_data_1,date_chunk1)
# date_success_chunks2,date_error_chunks2 = chunk_analysis(ajia_data_2,date_chunk2)
#
# def process_chunks(date_chunks,ajia_data,dp_data,xt_data):
#     result = []
#     date_desc = {}
#     for k,v in date_chunks.items():
#         date = k[:10]
#         ac_name = k[-2:]
#         if ac_name == "下放":
#             action_list,desc = task_xiafang(ajia_data,dp_data,xt_data,v)
#             if action_list is not None and len(action_list) > 0:
#                 result.append(action_list)
#         if ac_name == "回收":
#             action_list,desc = task_huishou(ajia_data,dp_data,xt_data,v)
#             if action_list is not None and len(action_list) > 0:
#                 result.append(action_list)
#         date_desc[k] = desc
#     return result,date_desc
#
#
# data_output1,date_desc1 = process_chunks(date_success_chunks1,ajia_data_1,dp_plc_1,xt_1)
# data_output2,date_desc2 = process_chunks(date_success_chunks2,ajia_data_2,dp_plc_2,xt_2)
#
# result = []
# for i in data_output1:
#     for j in i:
#         result.append(j)
# for i in data_output2:
#     for j in i:
#         result.append(j)
#
# ajia_io1 = ajia_data_1[ajia_data_1["ajia_action"] != ""]
# ajia_io2 = ajia_data_2[ajia_data_2["ajia_action"] != ""]
# xt_io1 = xt_1[xt_1["xt_action"] != ""]
# xt_io2 = xt_2[xt_2["xt_action"] != ""]
#
# for idx,item in ajia_io1.iterrows():
#     itemv = {"csvTime":item["csvTime"],"csvTimeMinute":item["csvTimeMinute"],"actionName":item["ajia_action"],"deviceName":"A架","actionType":"其他"}
#     result.append(itemv)
#
# for idx,item in ajia_io2.iterrows():
#     itemv = {"csvTime":item["csvTime"],"csvTimeMinute":item["csvTimeMinute"],"actionName":item["ajia_action"],"deviceName":"A架","actionType":"其他"}
#     result.append(itemv)
#
#
# for idx,item in xt_io1.iterrows():
#     itemv = {"csvTime":item["csvTime"],"csvTimeMinute":item["csvTimeMinute"],"actionName":item["xt_action"],"deviceName":"折臂吊车","actionType":"其他"}
#     result.append(itemv)
#
# for idx,item in xt_io2.iterrows():
#     itemv = {"csvTime":item["csvTime"],"csvTimeMinute":item["csvTimeMinute"],"actionName":item["xt_action"],"deviceName":"折臂吊车","actionType":"其他"}
#     result.append(itemv)
#
# dfx = pd.DataFrame(result)
#
#
# debug_df = dfx[(dfx["csvTime"] > '2024-05-16 00:00:00') & (dfx["csvTime"] < '2024-05-16 23:59:59')]
#
# print(debug_df)
# # 数据表
# import glob
# import pandas as pd
# paths = glob.glob("../data/v1/[a-zA-Z]*.csv")
# import pymysql
# from sqlalchemy import create_engine
#
# # 后续需要自行处理字段 需要增加库表情况 设置 数据库 ship1
# engine = create_engine('mysql+pymysql://root:qweasd@10.5.101.152:3306/ship2')
#
# def dtype_col(df):
#     for i in df.columns:
#         if "Ajia" in i:
#             df[i] = df[i].astype(float)
#         if "PLC" in i:
#             df[i] = df[i].astype(float)
#
# for path in paths:
#     if "Ajia" in path:
#         file_name = os.path.basename(path)
#         file_name = file_name.split(".")[0]
#         name = file_name[:-2]
#         df = pd.read_csv(path)
#         del df["Unnamed: 0"]
#         df = df.replace("error",-1)
#         dtype_col(df)
#         df.to_sql(name,con=engine,if_exists="append",index=False)
#     else:
#         file_name = os.path.basename(path)
#         file_name = file_name.split(".")[0]
#         name = file_name[:-2]
#         df = pd.read_csv(path)
#         del df["Unnamed: 0"]
#         df = df.replace("error", -1)
#         dtype_col(df)
#         df.to_sql(name,con=engine,if_exists="append",index=False)
#
# dfx.to_sql("task_action",con=engine,if_exists="replace",index=False)