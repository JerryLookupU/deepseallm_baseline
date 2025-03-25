import pandas as pd
import numpy as np
# ajia/征服者
from datetime import datetime,timedelta
import os



def Ajia_zhuangtai(x):
    if x["Ajia-3_v"] != -1 and x["Ajia-5_v"] != -1:
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





# 给定日期 判断 上午 还是 下午 上午为下放序列 下午为回收序列 如果当天仅有两组动作 则1为下放 2为回收

# def find_peaks_old(df):
#     peak_ids = []
#     for i in range(1, len(df) - 1):  # 遍历除了第一个和最后一个的索引
#         # 检查当前值是否比前后值都高
#         if (float(df.loc[df.index[i], 'Ajia-3_v']) + float(df.loc[df.index[i], 'Ajia-5_v'])) > (
#             float(df.loc[df.index[i - 1], 'Ajia-3_v']) + float(df.loc[df.index[i - 1], 'Ajia-5_v'])) and (
#             float(df.loc[df.index[i], 'Ajia-3_v']) + float(df.loc[df.index[i], 'Ajia-5_v'])) > (
#             float(df.loc[df.index[i + 1], 'Ajia-3_v']) + float(df.loc[df.index[i + 1], 'Ajia-5_v'])):
#             # 检查峰值是否超过80
#             if (float(df.loc[df.index[i], 'Ajia-3_v']) > 90 and
#                 float(df.loc[df.index[i], 'Ajia-5_v']) > 90):
#                 peak_id = df.index[i]
#                 peak_ids.append(peak_id)
#
#     return peak_ids


def find_peaks(df):
    # 创建副本避免修改原数据框，并计算总和列
    df = df.copy()
    df['sum_v'] = df['Ajia-3_v'] + df['Ajia-5_v']

    peak_candidates = []
    # 遍历数据框，寻找候选峰值
    for i in range(1, len(df) - 1):
        current_sum = df.iloc[i]['sum_v']
        prev_sum = df.iloc[i - 1]['sum_v']
        next_sum = df.iloc[i + 1]['sum_v']
        current_3v = df.iloc[i]['Ajia-3_v']
        current_5v = df.iloc[i]['Ajia-5_v']

        # 检查是否满足峰值条件
        if current_sum > prev_sum and current_sum > next_sum:
            if current_3v > 80 and current_5v > 80:
                peak_candidates.append({
                    'position': i,
                    'index': df.index[i],
                    'sum_v': current_sum
                })

    if not peak_candidates:
        return []

    # 按位置排序候选峰值
    peak_candidates.sort(key=lambda x: x['position'])

    # 将相邻不超过3个数据点的峰值分组
    groups = []
    if peak_candidates:
        current_group = [peak_candidates[0]]
        for peak in peak_candidates[1:]:
            if peak['position'] - current_group[-1]['position'] <= 3:
                current_group.append(peak)
            else:
                groups.append(current_group)
                current_group = [peak]
        groups.append(current_group)

    # 在每个组中选择总和最大的峰值
    selected_peaks = []
    for group in groups:
        max_sum = -float('inf')
        selected_peak = None
        for peak in group:
            if peak['sum_v'] > max_sum:
                max_sum = peak['sum_v']
                selected_peak = peak
            elif peak['sum_v'] == max_sum:
                # 总和相同则选择位置靠后的峰值
                if peak['position'] > selected_peak['position']:
                    selected_peak = peak
        if selected_peak:
            selected_peaks.append(selected_peak['index'])

    return selected_peaks

# 找到 A架开机 关机的整个时间段
def search_up_down(df, index):
    cur_loc = df.index.get_loc(index)
    start_index = None
    end_index = None
    for i in range(cur_loc, -1, -1):
        t1 = float(df.loc[df.index[i], "Ajia-3_v"])
        t2 = float(df.loc[df.index[i], "Ajia-5_v"])
        if (t1 < 0) or (t2 < 0):
            break
        start_index = df.index[i]

    for i in range(cur_loc, len(df)):
        t1 = float(df.loc[df.index[i], "Ajia-3_v"])
        t2 = float(df.loc[df.index[i], "Ajia-5_v"])
        if (t1 < 0) or (t2 < 0):
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
        # 转换时间格式并确保排序
        item = item.sort_values('csvTime').reset_index(drop=True)

        # 核心修改：按自然日创建分组
        # --------------------------------------------------
        # 生成日期列
        item['_date'] = item['csvTime'].apply(lambda x: x[:len("2024-05-22")])

        # 创建分组标识（当日期变化时生成新组）
        item['_group'] = (item['_date'] != item['_date'].shift()).cumsum()

        # 按自然日拆分数据
        daily_groups = []
        for _, group in item.groupby('_group'):
            # 移除临时列
            clean_group = group.drop(columns=['_date', '_group'])
            daily_groups.append(clean_group)
        # --------------------------------------------------
        # 对每个自然日分组执行原有逻辑
        for daily_item in daily_groups:
            # 保留原始处理逻辑
            task_time = daily_item["csvTime"].tolist()
            start_task = task_time[0]
            end_task = task_time[-1]

            ST = pd.to_datetime(start_task)
            ET = pd.to_datetime(end_task)

            # 计算总时长（小时）
            gap = (ET - ST).total_seconds() / 3600

            # 原时间间隔逻辑改为处理日间数据（可选保留）
            if gap >= 2:
                # 日内二次分组逻辑（保持原有逻辑）
                daily_item["groups"] = (pd.to_datetime(daily_item["csvTime"]).diff() > pd.Timedelta(hours=2)).cumsum()
                sub_groups = [g.drop(columns=['groups']) for _, g in daily_item.groupby("groups")]
                sub_df += sub_groups
            else:
                sub_df.append(daily_item)

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



# 数据处理
def filter_unpeaks_data(ajia_data_fields):
    result = []
    for item in ajia_data_fields:
        peaks_nums = find_peaks(item)
        if len(peaks_nums) == 0:
            continue
        result.append(item)
    return result


def find_xt_peaks(xt_data):
    peaks_nums = []
    row = xt_data["13-11-6_v"].tolist()
    base_state = 8  # 设定基础状态阈值
    current_peak = None  # 跟踪当前连续峰值的最后位置
    
    # 遍历所有数据点（从第二个元素开始）
    for idx in range(1, len(row)):
        prev_val = row[idx-1]
        curr_val = row[idx]
        
        # 进入峰值区域（当前值超过阈值，前值未超过）
        if curr_val > base_state and prev_val <= base_state:
            current_peak = idx
        
        # 持续在峰值区域（更新最后位置）
        if curr_val > base_state and prev_val > base_state:
            current_peak = idx
        
        # 离开峰值区域（当前值回落，前值在峰值区）
        if curr_val <= base_state and prev_val > base_state:
            if current_peak is not None:
                peaks_nums.append(current_peak)
                current_peak = None
    
    # 处理末尾未结束的峰值段
    if current_peak is not None:
        peaks_nums.append(current_peak)
    
    xt_loc_index = []
    for i in peaks_nums:
        xt_loc_index.append(xt_data.index[i])
    return xt_loc_index


def filter_xt_unpeaks_data(ajia_data_fields,xt_data):
    result = []
    for item in ajia_data_fields:
        peaks_nums = find_peaks(item)
        if len(peaks_nums) == 0:
            continue
        result.append(item)
    output = []
    for item in result:
        start_time, end_time = get_item_start_end_time(item)
        xt_patch = xt_data[(xt_data["csvTimeMinute"] >= start_time) & (xt_data["csvTimeMinute"] <= end_time)]
        xt_peaks_nums = find_xt_peaks(xt_patch)
        if len(xt_peaks_nums) == 0:
            continue
        output.append(item)
    return output



# 根据日期进行划分
def date_split_data(ajia_data_fields):
    date_chunks = {}
    for item in ajia_data_fields:
        start_time, end_time = get_item_start_end_time(item)
        date = start_time[:10]
        if date not in date_chunks:
            date_chunks[date] = []
        date_chunks[date].append(item)
    return date_chunks


def min_time(x,y):
    if x < y:
        return x
    else:
        return y
    
def max_time(x,y):
    if x > y:
        return x
    else:
        return y

# 
# 判断每天的段落数量
# 每天的数据段数量

def get_date_chunks_num(date_chunks):
    result = []
    for k,v in date_chunks.items():
        result.append({"date":k,"num":len(v)})
    return result

#
def is_xiafang_or_huishou(ajia_item,xt_data):
    # 下放回收逻辑
    # 1、早上下放概率高
    # 2、下放数据特征为  小艇峰值1 小艇峰值2  A架峰值1 A架峰值2 小艇峰值
    # 3、回收下午晚上概率高
    # 4、回收数据特征为  A架峰值1  小艇峰值2   A架峰值2 小艇峰值
    ajia_item_peak_ids = find_peaks(ajia_item)
    ajia_peak_time = ajia_item.loc[ajia_item_peak_ids,"csvTimeMinute"].tolist()
    # 如果 ajia 两个峰数据间隔有点大 则认为是 回收
    if len(ajia_peak_time) == 1:
        # 如果是上午 则认为是 下放
        if TaskA_level(ajia_peak_time[0]) == "上午":
            return "下放"
        else:
            return "回收"
    series_action = []
    for idx,s in enumerate(ajia_peak_time):
        series_action.append({"action":"A架","csvTime":s})
    
    if len(ajia_peak_time) <= 1:
        # 根据早上还是下午来判断
        if TaskA_level(ajia_peak_time.iloc[0]) == "上午":
            return "下放"
        else:
            return "回收"
    else:
        xt_patch = xt_data[(xt_data["csvTimeMinute"] >= ajia_peak_time[0]) & (xt_data["csvTimeMinute"] <= ajia_peak_time[1])]
        xt_peak_ids = find_xt_peaks(xt_patch)
        if xt_peak_ids is not None:
            xt_peak_times = xt_patch.loc[xt_peak_ids,"csvTime"]
            for idx,item in enumerate(xt_peak_times):
                series_action.append({"action":"折臂吊车","csvTime":item})
        # 加权判断逻辑
    xiafang_score = 0
    huishou_score = 0
    # 检测 序列情况
    sorted_actions = sorted(series_action,key=lambda x:x["csvTime"])
        # 时间差计算辅助函数
    def get_time_diff(t1, t2):
        return (pd.to_datetime(t2) - pd.to_datetime(t1)).total_seconds() / 60
    # 检测 序列情
    # 特征检测
    a_peaks = [a for a in sorted_actions if a["action"] == "A架"]
    xt_peaks = [a for a in sorted_actions if a["action"] == "折臂吊车"]
    # 规则1: 遍历所有A架峰值组合，取最大间隔
    if len(a_peaks) >= 2:
        max_gap = 0
        # 计算所有相邻峰值的间隔
        for i in range(1, len(a_peaks)):
            prev_gap = get_time_diff(a_peaks[i-1]["csvTime"], a_peaks[i]["csvTime"])
            max_gap = max(max_gap, prev_gap)
        

        if max_gap > 30:
            huishou_score += 3  # 最大间隔超过20分钟→回收特征

    # 规则2: 小艇双峰间隔4分钟以上（回收特征）
    if len(a_peaks) >= 2:
        min_gap = 0
        # 计算所有相邻峰值的间隔
        for i in range(1, len(a_peaks)):
            prev_gap = get_time_diff(a_peaks[i-1]["csvTime"], a_peaks[i]["csvTime"])
            min_gap = min(min_gap, prev_gap)
            
        if min_gap < 15:
            xiafang_score += 3  # 最大间隔超过20分钟→回收特征

    # 规则3: 交替模式检测（A-X-A-X）
    continuous_a_count = 0
    has_continuous_a = False
    
    # 遍历动作序列检测连续A架
    for i in range(1, len(sorted_actions)):
        prev = sorted_actions[i-1]["action"]
        curr = sorted_actions[i]["action"]
        if prev == "A架" and curr == "A架":
            continuous_a_count += 1
            has_continuous_a = True
    
    # 应用连续A架评分
    if has_continuous_a:
        xiafang_score += continuous_a_count * 0.5  # 每个连续对+0.5分
    else:
        huishou_score += 0.5  # 无连续A架时回收+0.5分
        # 时间加权（上午/下午）
    first_peak_time = a_peaks[0]["csvTime"]
    if TaskA_level(first_peak_time) == "上午":
        xiafang_score += 1
    else:
        huishou_score += 1
        
 # 最终判断
    if xiafang_score > huishou_score:
        return "下放"
    elif huishou_score > xiafang_score:
        return "回收"
    else:
        # 平局时根据时间判断
        return "下放" if TaskA_level(first_peak_time) == "上午" else "回收"
    


def generate_alternate_pattern(n, first_type):
        # 生成标准的交替模式序列
    return [first_type if i%2 == 0 else ("回收" if first_type=="下放" else "下放") for i in range(n)]

def find_optimal_sequence(int_types):
        # 计算两种交替模式的修改成本
    cost_a = sum(1 for i,t in enumerate(int_types) if t != generate_alternate_pattern(len(int_types), "下放")[i])
    cost_b = sum(1 for i,t in enumerate(int_types) if t != generate_alternate_pattern(len(int_types), "回收")[i])
        
        # 选择成本更低的模式
    if cost_a <= cost_b:
        return generate_alternate_pattern(len(int_types), "下放")
    return generate_alternate_pattern(len(int_types), "回收")

def process_date_dict(ajia_data_dict,xt_data):
    # 判断数据段是下放还是回收
    result = []
    for k,v in ajia_data_dict.items():
        if len(v) == 1:
            item = v[0]
            type_name = is_xiafang_or_huishou(item,xt_data)
            result.append({"item":item,"date":k,"type":type_name})
        elif len(v) == 2:
            item1 = v[0]
            item2 = v[1]
            result.append({"item":item1,"date":k,"type":"下放"})
            result.append({"item":item2,"date":k,"type":"回收"})
        elif len(v) > 3:
            init_types = [
                is_xiafang_or_huishou(v[0], xt_data),
                is_xiafang_or_huishou(v[1], xt_data),
                is_xiafang_or_huishou(v[2], xt_data)
            ]
            if init_types[0] == init_types[1] == init_types[2]:
                if init_types[1] == "下放":
                    result.append({"item":v[0],"date":k,"type":"下放"})
                    result.append({"item":v[1],"date":k,"type":"回收"})
                    result.append({"item":v[2],"date":k,"type":"下放"})
                else:
                    result.append({"item":v[0],"date":k,"type":"回收"})
                    result.append({"item":v[1],"date":k,"type":"下放"})
                    result.append({"item":v[2],"date":k,"type":"回收"})
            elif init_types[0] == init_types[1]:
                if init_types[0] == "下放":
                    result.append({"item":v[0],"date":k,"type":"回收"})
                    result.append({"item":v[1],"date":k,"type":"下放"})
                    result.append({"item":v[2],"date":k,"type":"回收"})
                else:
                    result.append({"item":v[0],"date":k,"type":"下放"})
                    result.append({"item":v[1],"date":k,"type":"回收"})
                    result.append({"item":v[2],"date":k,"type":"下放"})
            elif init_types[1] == init_types[2]:
                if init_types[1] == "下放":
                    result.append({"item":v[0],"date":k,"type":"回收"})
                    result.append({"item":v[1],"date":k,"type":"下放"})
                    result.append({"item":v[2],"date":k,"type":"回收"})
                else:
                    result.append({"item":v[0],"date":k,"type":"下放"})
                    result.append({"item":v[1],"date":k,"type":"回收"})
                    result.append({"item":v[2],"date":k,"type":"下放"})
            else:
                result.append({"item":v[0],"date":k,"type":init_types[0]})
                result.append({"item":v[1],"date":k,"type":init_types[1]})
                result.append({"item":v[2],"date":k,"type":init_types[2]})
        else:
            init_types = []
            for i in range(len(v)):
                init_types.append(is_xiafang_or_huishou(v[i], xt_data))

            optimized = find_optimal_sequence(init_types)
            for i in range(len(v)):
                result.append({"item":v[i],"date":k,"type":optimized[i]})
    return result





def get_xt_startup_shutdown(xt_data):
    # 找到折臂吊车开机和关机数据
    xt_path_kg = xt_data[xt_data["xt_action"] != ""]
    startup_shutdown_pairs = []
    startup_time = None

    for _, row in xt_path_kg.iterrows():
        if row["xt_action"] == "折臂吊车开机":
            startup_time = row["csvTime"]
        elif row["xt_action"] == "折臂吊车关机" and startup_time is not None:
            startup_shutdown_pairs.append({"开机时间": startup_time, "关机时间": row["csvTime"], "类型": "折臂吊车"})
            startup_time = None

    return startup_shutdown_pairs
    


def xt_pair_start_time(xt_pair,ajia_start_time):
    
    for i_pair in xt_pair:
        # 情况如下  
        # 1、 A架开机 - 折臂吊车开机，如果该情况  找到所有 Ajia_start_time in [开机~关机]
        # 2、 折臂吊车开机 A架开机，如果该情
        xt_start_time = i_pair["开机时间"][:-3]
        xt_end_time = i_pair["关机时间"][:-3]
        if xt_start_time <= ajia_start_time <= xt_end_time:
            return xt_start_time
    
    # ajia_start_time 提前 30 分钟
    ajia_new_time = pd.to_datetime(ajia_start_time+":00") - pd.Timedelta(minutes=30)
    ajia_new_time = ajia_new_time.strftime("%Y-%m-%d %H:%M")
    last_xt_end_time = None
    for i_pair in xt_pair:
        # 情况如下
        # 2、 折臂吊车开机 A架开机，如果该情
        xt_start_time = i_pair["开机时间"][:-3]
        xt_end_time = i_pair["关机时间"][:-3]
        if xt_start_time <= ajia_start_time <= xt_end_time:
            return xt_start_time
        ## 如果
        if last_xt_end_time != None:
            if last_xt_end_time <= ajia_new_time <= xt_start_time:
                return xt_start_time
        last_xt_end_time = xt_end_time
    return None
    

def xt_pair_end_time(xt_pair, ajia_end_time):
    # 遍历折臂吊车的开机和关机时间对
    for i_pair in xt_pair:
        # 获取折臂吊车的开机时间和关机时间，并去掉秒信息
        xt_start_time = i_pair["开机时间"][:-3]
        xt_end_time = i_pair["关机时间"][:-3]
        # 如果A架结束时间在折臂吊车的开机和关机时间之间，返回折臂吊车的关机时间
        if xt_start_time <= ajia_end_time <= xt_end_time:
            return xt_end_time
    
    # ajia_end_time 推迟 30 分钟
    ajia_new_time = pd.to_datetime(ajia_end_time+":59") + pd.Timedelta(minutes=30)
    ajia_new_time = ajia_new_time.strftime("%Y-%m-%d %H:%M")
    
    # 遍历折臂吊车的开机和关机时间对
    for i in range(len(xt_pair) - 1):
        # 获取当前折臂吊车的关机时间，并去掉秒信息
        current_xt_end_time = xt_pair[i]["关机时间"][:-3]
        current_xt_start_time = xt_pair[i]["开机时间"][:-3]
        # 获取下一个折臂吊车的开机时间，并去掉秒信息
        if current_xt_start_time <= ajia_new_time <= current_xt_end_time:
            return current_xt_end_time
        next_xt_start_time = xt_pair[i + 1]["开机时间"][:-3]
        # 如果推迟后的A架结束时间在当前折臂吊车的关机时间和下一个折臂吊车的开机时间之间
        if current_xt_end_time <= ajia_new_time <= next_xt_start_time:
            return current_xt_end_time
    return None


def find_lsjc_index(lsjc_patch_data):
    lsjc_patch = lsjc_patch_data[::-1]
    switch_index = None
    for idx,rid in enumerate(lsjc_patch.index):
        # 电流先都回到稳定值（50多）
        row = lsjc_patch.loc[rid]
        if row["Ajia-3_v"] < 60 and row["Ajia-5_v"] < 60:
            # 找到 30 秒内 电流都在 50 以下的点
            # 找到 小于60的点
            if switch_index is None:
                switch_index = rid
        # 如果数据归0 则数据异常
        if int(row["Ajia-3_v"]) == -1 and int(row["Ajia-5_v"]) == -1:
            return None
        
        if int(row["Ajia-3_v"]) == 0 and int(row["Ajia-5_v"]) == 0:
            return None
        
        if switch_index is not None:
            # 如果 从当前点开始上升的点即缆绳解除 
            # 下一个点 两个电流均大于等于60，当前点至少一个电流小于60
            if idx > len(lsjc_patch.index)-1:
                return None
            next_row = lsjc_patch.loc[lsjc_patch.index[idx+1]]
            if next_row["Ajia-3_v"] >= 60 and next_row["Ajia-5_v"] >= 60:
                if row["Ajia-3_v"] < 60 or row["Ajia-5_v"] < 60:
                    return rid
    return None

def find_lsjc_half_index(lsjc_patch_data):
    lsjc_patch = lsjc_patch_data[::-1]
    switch_index = None
    for idx,rid in enumerate(lsjc_patch.index):
        # 电流先都回到稳定值（50多）
        row = lsjc_patch.loc[rid]

        if idx > len(lsjc_patch.index)-1:
                return None
        next_row = lsjc_patch.loc[lsjc_patch.index[idx+1]]
        if next_row["Ajia-3_v"] >= 60 and next_row["Ajia-5_v"] >= 60:
            if row["Ajia-3_v"] < 60 or row["Ajia-5_v"] < 60:
                return rid
    return None


def xiafang_single_peaks(ajia_data, dp_data, xt_data, xt_pair, ajia_data_item):
    result = []
    desc = []
    start_time, end_time = get_item_start_end_time(ajia_data_item)
    xt_start_time = xt_pair_start_time(xt_pair, start_time)
    if xt_start_time is None:
        xt_start_time = xt_data.loc[xt_data.index[0]]["csvTime"][:-3]
    xt_end_time = xt_pair_end_time(xt_pair, end_time)
    if xt_end_time is None:
        xt_end_time = xt_data.loc[xt_data.index[-1]]["csvTime"][:-3]
    # 比较一下时间
    bs_time = min_time(xt_start_time, start_time)
    es_time = max_time(xt_end_time, end_time)

    # 找到折臂吊车数据
    xt_patch = xt_data[(xt_data["csvTimeMinute"] >= bs_time) & (xt_data["csvTimeMinute"] <= es_time)]
    xt_peaks_ids = find_xt_peaks(xt_patch)

    if len(xt_peaks_ids) == 0:
        return result, "折臂吊车无动作/A架单峰值"
    # 找到折臂吊车的峰值
    if len(xt_peaks_ids) >= 2:
        # 找到折臂吊车的峰值
        # 仅可以判断 折臂吊车最后一个峰值是 小艇落座
        xt_peaks_idx = xt_peaks_ids[-1]
        xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
        result.append({"csvTimeMinute": xt_luozuo_csvTime[:-3], "csvTime": xt_luozuo_csvTime, "actionName": "小艇落座", "deviceName": "折臂吊车", "actionType": "下放"})
        return result, "A架单峰值/折臂吊车动作次数:%s次" % (len(xt_peaks_ids))
    else:
        return result, "折臂吊车无动作/A架单峰值"

    # 找到A架单峰值索引
    xx_peaks_ids = find_peaks(ajia_data_item)
    ajia_peak_idx = xx_peaks_ids[0]
    ajia_peak_time = ajia_data_item.loc[ajia_peak_idx]["csvTime"]

    # 判断单峰值是A架摆回点还是征服者起吊点
    peak_index = ajia_data_item.index.tolist().index(ajia_peak_idx)
    pre_peak_data = ajia_data_item.iloc[:peak_index]
    post_peak_data = ajia_data_item.iloc[peak_index + 1:]

    # 检查峰值前是否有异常情况
    pre_peak_abnormal = any(pre_peak_data[["Ajia-3_v", "Ajia-5_v"]].isin([-1]).any(axis=1))
    pre_peak_time_gap = pd.to_datetime(pre_peak_data["csvTimeMinute"]).diff().dt.seconds.max()
    if pre_peak_time_gap > 1800:  # 30分钟
        pre_peak_abnormal = True
    # 新增截断判断：pre_peak_data的第一条数据电流不为0
    if not pre_peak_data.empty:
        first_row = pre_peak_data.iloc[0]
        if first_row["Ajia-3_v"] != 0 and first_row["Ajia-5_v"] != 0:
            pre_peak_abnormal = True

    # 检查峰值后电流是否先到50多然后到0
    post_peak_normal_to_zero = False
    if len(post_peak_data) > 1:
        next_values = post_peak_data[["Ajia-3_v", "Ajia-5_v"]].values
        if (next_values[0] > 50).all() and (next_values[-1] == 0).all():
            post_peak_normal_to_zero = True
    # 新增截断判断：post_peak_data的最后一条电流不为0
    if not post_peak_data.empty:
        last_row = post_peak_data.iloc[-1]
        if last_row["Ajia-3_v"] != 0 and last_row["Ajia-5_v"] != 0:
            post_peak_normal_to_zero = False

    if pre_peak_abnormal and post_peak_normal_to_zero:
        action_name = "A架摆回"
    else:
        action_name = "征服者起吊峰值"
    
    if action_name == "A架摆回":
        
        result.append({"csvTimeMinute": ajia_peak_time[:-3], "csvTime": ajia_peak_time, "actionName": action_name, "deviceName": "A架", "actionType": "下放"})

        xtlz_patch = xt_data[(xt_data["csvTimeMinute"] >= xt_start_time) & (xt_data["csvTimeMinute"] <= xt_end_time)]
        xt_peaks_ids = find_xt_peaks(xtlz_patch)
        if len(xt_peaks_ids) == 0:
            pass
        else:
            xt_peaks_idx = xt_peaks_ids[-1]
            xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"下放"})
        return result,"缆绳解除点未找到"

    else:
        lsjc_patch_index = find_lsjc_half_index(ajia_data_item)
        lsjc_patch_time = ""
        if lsjc_patch_index is not None:
            lsjc_patch_time = ajia_data_item.loc[lsjc_patch_index]["csvTime"]
            result.append({"csvTimeMinute":lsjc_patch_time[:-3],"csvTime":lsjc_patch_time,"actionName":"缆绳解除","deviceName":"A架","actionType":"下放"})
              # 找到缆绳解除时间点 往前推一分钟 为征服者入水时间
            zfzrs_patch_time = pd.to_datetime(lsjc_patch_time) - pd.Timedelta(minutes=1)
            zfzrs_patch_time = zfzrs_patch_time.strftime("%Y-%m-%d %H:%M:%S")
            # 征服者入水时间
            if check_time(start_time,end_time,zfzrs_patch_time[:-3]):
                result.append({"csvTimeMinute":zfzrs_patch_time[:-3],"csvTime":zfzrs_patch_time,"actionName":"征服者入水","deviceName":"A架","actionType":"下放"})
        else:
            # 设置一个假的 缆绳解除的点
            lsjc_patch_index = ajia_data_item.index[-1]
            lsjc_patch_time  = ajia_data_item.loc[lsjc_patch_index]["csvTime"]
           # 确定缆绳解除时间点 能获取 小艇入水时间点
        xtrs_patch = xt_data[(xt_data["csvTimeMinute"] <= lsjc_patch_time[:-3])&(xt_data["csvTimeMinute"] > bs_time)]
        xtrs_peaks_ids = find_xt_peaks(xtrs_patch)
        if len(xtrs_peaks_ids) == 0:
            # 只能找到折臂吊车最后一个峰值(缆绳解除前)
            pass
        elif len(xtrs_peaks_ids) == 1:
            # 找到折臂吊车的峰值
            # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
            xt_peaks_idx = xtrs_peaks_ids[-1]
            xt_rushui_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":xt_rushui_csvTime[:-3],"csvTime":xt_rushui_csvTime,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"下放"})
        else:
            # 找到折臂吊车的峰值
            # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
            xt_peaks_idx = xtrs_peaks_ids[-1]
            xt_rushui_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":xt_rushui_csvTime[:-3],"csvTime":xt_rushui_csvTime,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"下放"})
            xt_peaks_idx1 = xtrs_peaks_ids[-2]
            xt_jcwanbi_csvTime1 = xt_data.loc[xt_peaks_idx1]["csvTime"]
            result.append({"csvTimeMinute":xt_jcwanbi_csvTime1[:-3],"csvTime":xt_jcwanbi_csvTime1,"actionName":"小艇检查完毕","deviceName":"折臂吊车","actionType":"下放"})
        zfzrs_patch = ajia_data[ajia_data["csvTimeMinute"] <= ajia_peak_time[:-3]]
        zfzrs_patch_index = find_zfzqd_index(zfzrs_patch)
        if zfzrs_patch_index is None:
            return result,"征服者起吊点未找到"
        else:
            # 找到征服者起吊点
            zfzrs_patch_time = zfzrs_patch.loc[zfzrs_patch_index]["csvTime"]
            if check_time(start_time,end_time,zfzrs_patch_time[:-3]):
                result.append({"csvTimeMinute":zfzrs_patch_time[:-3],"csvTime":zfzrs_patch_time,"actionName":"征服者起吊","deviceName":"A架","actionType":"下放"})
    return result,"结束"
            

    


def huishou_single_peaks(ajia_data,dp_data,xt_data,xt_pair,ajia_data_item):
    result = []
    desc = []
    start_time,end_time = get_item_start_end_time(ajia_data_item)
    xt_start_time = xt_pair_start_time(xt_pair, start_time)
    if xt_start_time is None:
        xt_start_time = xt_data.loc[xt_data.index[0]]["csvTime"][:-3]
    xt_end_time = xt_pair_end_time(xt_pair, end_time)
    if xt_end_time is None:
        xt_end_time = xt_data.loc[xt_data.index[-1]]["csvTime"][:-3]
    # 比较一下时间
    bs_time = min_time(xt_start_time, start_time)
    es_time = max_time(xt_end_time, end_time)
    # 找到折臂吊车数据
    xt_patch = xt_data[(xt_data["csvTimeMinute"] >= bs_time) & (xt_data["csvTimeMinute"] <= es_time)]
    xt_peaks_ids = find_xt_peaks(xt_patch)
    ajia_peaks_ids = find_peaks(ajia_data_item)
    ajia_peak_idx = ajia_peaks_ids[0]
    ajia_peak_time = ajia_data_item.loc[ajia_peak_idx]["csvTime"]
    # 以峰值时间为界限切人xt_data
    pre_xt_patch = xt_data[(xt_data["csvTimeMinute"] >= bs_time) & (xt_data["csvTimeMinute"] < ajia_peak_time[:-3])]
    post_xt_patch = xt_data[(xt_data["csvTimeMinute"] >= ajia_peak_time[:-3]) & (xt_data["csvTimeMinute"] <= es_time)]
    # 找到峰值
    pre_xt_peaks_ids = find_xt_peaks(pre_xt_patch)
    post_xt_peaks_ids = find_xt_peaks(post_xt_patch)
    if len(pre_xt_peaks_ids) == 0 and len(post_xt_peaks_ids) == 0:
        return result, "折臂吊车无动作/A架无动作"
    # 找到折臂吊车的峰值
    peaks_name = ""
    if len(xt_peaks_ids) >= 2:
        # 找到折臂吊车的峰值
        # 
        if len(pre_xt_peaks_ids)  == len(post_xt_peaks_ids):
            # 无法判断峰值是摆出还是征服者出水
            # 只能给出最后一个位置是小艇落座

            xt_peaks_idx = post_xt_peaks_ids[-1]
            xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"回收"})
            # 先考虑 一个入水 一个落座情况 后续再根据情况修改
            post_xt_peaks_idx = post_xt_peaks_ids[-1]
            post_xt_peaks_time = xt_data.loc[post_xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":post_xt_peaks_time[:-3],"csvTime":post_xt_peaks_time,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"回收"})
            
        elif len(pre_xt_peaks_ids) > len(post_xt_peaks_ids):
            # 则峰值判断未 征服者出水
            peaks_name = "征服者出水"
            if len(post_xt_peaks_ids) != 0:
                # 没有 小艇落座
                # 只能给出最后一个位置是小艇落座
                xt_peaks_idx = post_xt_peaks_ids[-1]
                xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
                result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"回收"})
            if len(pre_xt_peaks_ids) == 1:
                # 小艇峰值如果只有一个值 则为小艇入水
                xt_peaks_idx = pre_xt_peaks_ids[0]
                xt_rushui_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
                result.append({"csvTimeMinute":xt_rushui_csvTime[:-3],"csvTime":xt_rushui_csvTime,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"回收"})
            else:
                # 找到折臂吊车的峰值
                # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
                xt_peaks_idx = pre_xt_peaks_ids[-1]
                xt_rushui_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
                result.append({"csvTimeMinute":xt_rushui_csvTime[:-3],"csvTime":xt_rushui_csvTime,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"回收"})
                xt_peaks_idx1 = pre_xt_peaks_ids[-2]
                xt_jcwanbi_csvTime1 = xt_data.loc[xt_peaks_idx1]["csvTime"]
                result.append({"csvTimeMinute":xt_jcwanbi_csvTime1[:-3],"csvTime":xt_jcwanbi_csvTime1,"actionName":"小艇检查完毕","deviceName":"折臂吊车","actionType":"回收"})
        elif len(pre_xt_peaks_ids) < len(post_xt_peaks_ids):
            # 则峰值判断为 小艇落座
            peaks_name = "A架摆出"
            if len(post_xt_peaks_ids) >= 3:
                                # 找到折臂吊车的峰值
                # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
                xt_peaks_idx = post_xt_peaks_ids[-1]
                xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
                result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"回收"})
                
                xt_peaks_idx1 = post_xt_peaks_ids[-2]
                xt_rushui_csvTime1 = xt_data.loc[xt_peaks_idx1]["csvTime"]
                result.append({"csvTimeMinute":xt_rushui_csvTime1[:-3],"csvTime":xt_rushui_csvTime1,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"回收"})
                
                xt_peaks_idx2 = post_xt_peaks_ids[-3]
                xt_jcwanbi_csvTime1 = xt_data.loc[xt_peaks_idx2]["csvTime"]
                result.append({"csvTimeMinute":xt_jcwanbi_csvTime1[:-3],"csvTime":xt_jcwanbi_csvTime1,"actionName":"小艇检查完毕","deviceName":"折臂吊车","actionType":"回收"})
    if peaks_name == "A架摆出":
        # 找到A架摆回点
        # 以A架摆出点查找ajia 下一个点
        ajia_baichu_csvTime = ajia_data_item.loc[ajia_peak_idx]["csvTime"]
        if check_time(start_time,end_time,ajia_baichu_csvTime[:-3]):
            result.append({"csvTimeMinute":ajia_baichu_csvTime[:-3],"csvTime":ajia_baichu_csvTime,"actionName":"A架摆出","deviceName":"A架","actionType":"回收"})
        return result,"找不到A架以外摆出其他点" 
        
    if peaks_name == "征服者出水":
        ajia_zfzchushui_csvTime = ajia_data_item.loc[ajia_peak_idx]["csvTime"]
        if check_time(start_time,end_time,ajia_zfzchushui_csvTime[:-3]):
            result.append({"csvTimeMinute":ajia_zfzchushui_csvTime[:-3],"csvTime":ajia_zfzchushui_csvTime,"actionName":"征服者出水","deviceName":"A架","actionType":"回收"})
        # 征服者出水想想找到 征服者落座点
        ajia_lsgt_csvTime = pd.to_datetime(ajia_zfzchushui_csvTime) - pd.Timedelta(minutes=1)
        ajia_lsgt_csvTime = ajia_lsgt_csvTime.strftime("%Y-%m-%d %H:%M:%S")
        if check_time(start_time,end_time,ajia_lsgt_csvTime[:-3]):
            result.append({"csvTimeMinute":ajia_lsgt_csvTime[:-3],"csvTime":ajia_lsgt_csvTime,"actionName":"征服者落座","deviceName":"A架","actionType":"回收"})

        zfzcs_patch = ajia_data[(ajia_data["csvTimeMinute"] > ajia_zfzchushui_csvTime[:-3])&(ajia_data["csvTimeMinute"] < es_time)]
        zfzlz_index = find_zfzlz_index(zfzcs_patch)
        if zfzlz_index is None:
            return result,"找不到征服者落座点"
        else:
            zfzlz_csvTime = zfzcs_patch.loc[zfzlz_index]["csvTime"]
            if check_time(start_time,end_time,zfzlz_csvTime[:-3]):
                result.append({"csvTimeMinute":zfzlz_csvTime[:-3],"csvTime":zfzlz_csvTime,"actionName":"征服者落座","deviceName":"A架","actionType":"回收"})
        return result,"结束"
    return result,"结束"


def find_zfzqd_index(zfzrs_patch_data):
    zfzrs_patch = zfzrs_patch_data[::-1]
    switch_index = None
    for idx,rid in enumerate(zfzrs_patch.index[:-1]):
        # 直接找到 下一个电流均小于60的位置
        next_row = zfzrs_patch.loc[zfzrs_patch.index[idx+1]]
        row = zfzrs_patch.loc[rid]
        if int(row["Ajia-3_v"]) == -1 and int(row["Ajia-5_v"]) == -1:
            return None
        if int(row["Ajia-3_v"]) == 0 and int(row["Ajia-5_v"]) == 0:
            return None
        
        if next_row["Ajia-3_v"] < 60 and next_row["Ajia-5_v"] < 60:
            if row["Ajia-3_v"] >= 60 and row["Ajia-5_v"] >= 60:
                return rid
    return None


def check_time(ajia_start_time,ajia_end_time,judge_time):
    if ajia_start_time <= judge_time <= ajia_end_time:
        return True
    return False

def task_xiafang(ajia_data,dp_data,xt_data,xt_pair,ajia_data_item):
    # 处理下放数据
    result = []
    desc = []
    find_peaks_ids = find_peaks(ajia_data_item)
    start_time,end_time = get_item_start_end_time(ajia_data_item)
    xt_start_time = xt_pair_start_time(xt_pair, start_time)
    if xt_start_time is None:
        xt_start_time = xt_data.loc[xt_data.index[0]]["csvTime"][:-3]
    xt_end_time = xt_pair_end_time(xt_pair, end_time)
    if xt_end_time is None:
        xt_end_time = xt_data.loc[xt_data.index[-1]]["csvTime"][:-3]
        # 找到峰值数据
    bs_time = min_time(xt_start_time,start_time)
    es_time = max_time(xt_end_time,end_time)

        
    if len(find_peaks_ids) == 1:
        # 找到峰值数据
        return xiafang_single_peaks(ajia_data,dp_data,xt_data,xt_pair,ajia_data_item)
    
    if len(find_peaks_ids) < 1:
        # A架找不到动作
        # 找到折臂吊车数据
        # 找到折臂吊车向上开机和向下开机数据
        xt_patch = xt_data[(xt_data["csvTimeMinute"] >= bs_time) & (xt_data["csvTimeMinute"] <= es_time)]
        xt_peaks_ids = find_xt_peaks(xt_patch)
        if len(xt_peaks_ids) == 0:
            return result, "折臂吊车无动作/A架无动作"
        # 找到折臂吊车的峰值
        if len(xt_peaks_ids) >= 2:
            # 找到折臂吊车的峰值
            # 仅可以判断 折臂吊车最后一个峰值是 小艇落座
            xt_peaks_idx = xt_peaks_ids[-1]
            xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"下放"})
            return result,"A架无动作/折臂吊车动作次数:%s次"%(len(xt_peaks_ids))
        else:
            return result, "折臂吊车无动作/A架无动作"

    # 如果 A架两个峰值为正常情况：
    # 下放过程 最后一个峰值为 A架摆回
    ajia_baihui_peak_idx = find_peaks_ids[-1]
    ajia_baihui_csvTime = ajia_data_item.loc[ajia_baihui_peak_idx]["csvTime"]
    result.append({"csvTimeMinute":ajia_baihui_csvTime[:-3],"csvTime":ajia_baihui_csvTime,"actionName":"A架摆回","deviceName":"A架","actionType":"下放"})
    
    # 从A架摆回时间点 向上搜索 缆绳解除点
    lsjc_patch = ajia_data[ajia_data["csvTimeMinute"] <= ajia_baihui_csvTime]
    lsjc_patch_index = find_lsjc_index(lsjc_patch_data=lsjc_patch)
    # 
    if lsjc_patch_index is None:
        # 只能找到折臂吊车最后一个峰值
        xtlz_patch = xt_data[(xt_data["csvTimeMinute"] >= xt_start_time) & (xt_data["csvTimeMinute"] <= xt_end_time)]
        xt_peaks_ids = find_xt_peaks(xtlz_patch)
        if len(xt_peaks_ids) == 0:
            pass
        else:
            xt_peaks_idx = xt_peaks_ids[-1]
            xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"下放"})
        return result,"缆绳解除点未找到"
    else:
        # 找到缆绳解除点
        lsjc_patch_time = lsjc_patch.loc[lsjc_patch_index]["csvTime"]
        xtlz_patch = xt_data[(xt_data["csvTimeMinute"] > lsjc_patch_time[:-3]) & (xt_data["csvTimeMinute"] <= es_time)]
        xt_peaks_ids = find_xt_peaks(xtlz_patch)
        if len(xt_peaks_ids) == 0:
            pass
        else:
            xt_peaks_idx = xt_peaks_ids[-1]
            xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"下放"})      
    # 
    # 缆绳解除的时间点往前推一分钟
    lsjc_patch_time = lsjc_patch.loc[lsjc_patch_index]["csvTime"]
    result.append({"csvTimeMinute":lsjc_patch_time[:-3],"csvTime":lsjc_patch_time,"actionName":"缆绳解除","deviceName":"A架","actionType":"下放"})
    
    # 确定缆绳解除时间点 能获取 小艇入水时间点
    xtrs_patch = xt_data[(xt_data["csvTimeMinute"] <= lsjc_patch_time[:-3])&(xt_data["csvTimeMinute"] > bs_time)]
    xtrs_peaks_ids = find_xt_peaks(xtrs_patch)
    if len(xtrs_peaks_ids) == 0:
        # 只能找到折臂吊车最后一个峰值(缆绳解除前)
        pass
    elif len(xtrs_peaks_ids) == 1:
        # 找到折臂吊车的峰值
        # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
        xt_peaks_idx = xtrs_peaks_ids[-1]
        xt_rushui_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
        result.append({"csvTimeMinute":xt_rushui_csvTime[:-3],"csvTime":xt_rushui_csvTime,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"下放"})
    else:
        # 找到折臂吊车的峰值
        # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
        xt_peaks_idx = xtrs_peaks_ids[-1]
        xt_rushui_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
        result.append({"csvTimeMinute":xt_rushui_csvTime[:-3],"csvTime":xt_rushui_csvTime,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"下放"})
        xt_peaks_idx1 = xtrs_peaks_ids[-2]
        xt_jcwanbi_csvTime1 = xt_data.loc[xt_peaks_idx1]["csvTime"]
        result.append({"csvTimeMinute":xt_jcwanbi_csvTime1[:-3],"csvTime":xt_jcwanbi_csvTime1,"actionName":"小艇检查完毕","deviceName":"折臂吊车","actionType":"下放"})
    
    
    # 找到缆绳解除时间点 往前推一分钟 为征服者入水时间
    zfzrs_patch_time = pd.to_datetime(lsjc_patch_time) - pd.Timedelta(minutes=1)
    zfzrs_patch_time = zfzrs_patch_time.strftime("%Y-%m-%d %H:%M:%S")
    # 征服者入水时间
    if check_time(start_time,end_time,zfzrs_patch_time[:-3]):
        result.append({"csvTimeMinute":zfzrs_patch_time[:-3],"csvTime":zfzrs_patch_time,"actionName":"征服者入水","deviceName":"A架","actionType":"下放"})
    # 寻找征服者起吊点
    else:
        return result,"征服者入水时间点未找到"
    zfzrs_patch = ajia_data[ajia_data["csvTimeMinute"] <= zfzrs_patch_time]
    zfzrs_patch_index = find_zfzqd_index(zfzrs_patch)
    if zfzrs_patch_index is None:
        return result,"征服者起吊点未找到"
    else:
        # 找到征服者起吊点
        zfzrs_patch_time = zfzrs_patch.loc[zfzrs_patch_index]["csvTime"]
        if check_time(start_time,end_time,zfzrs_patch_time[:-3]):
            result.append({"csvTimeMinute":zfzrs_patch_time[:-3],"csvTime":zfzrs_patch_time,"actionName":"征服者起吊","deviceName":"A架","actionType":"下放"})
    return result,"结束"


def find_zfzlz_index(zfzcs_patch_data):
    # 征服者落座点
    for idx,rid in enumerate(zfzcs_patch_data.index):
        # 找到 30 秒内 电流都在 50 以下的点
        if idx > len(zfzcs_patch_data.index)-1:
            return None
        row = zfzcs_patch_data.loc[rid]
        if row["Ajia-3_v"]  == -1 and row["Ajia-5_v"] == -1:
            return None
        if row["Ajia-3_v"] == 0 and row["Ajia-5_v"] == 0:
            return None
        if row["Ajia-3_v"] < 60 or row["Ajia-5_v"] < 60:
            # 找到 小于60的点
            return rid
    return None


def find_ajia_baichu_index(ajia_zfzcs_patch):
    # 
    # 找到起吊点
    c_zfzcs_patch = ajia_zfzcs_patch[::-1]
    switch_time = None
    for idx,rid in enumerate(c_zfzcs_patch.index):
        row = c_zfzcs_patch.loc[rid]
        if row["Ajia-3_v"] == -1 and row["Ajia-5_v"] == -1:
            return None
        if switch_time is None:
            if row["Ajia-3_v"] == 0 and row["Ajia-5_v"] == 0:
                switch_time = c_zfzcs_patch.loc[rid]["csvTime"]
                break
    if switch_time is not None:
        local_patch = ajia_zfzcs_patch[ajia_zfzcs_patch["csvTimeMinute"] < switch_time[:-3]]
        peaks_nums = find_peaks(local_patch)
        if len(peaks_nums) > 0 :
            peaks_ids = peaks_nums[-1]
            return peaks_ids
    return None


    
def task_huishou(ajia_data,dp_data,xt_data,xt_pair,ajia_data_item):
    # 处理回收数据
    result = []
    desc = []
    find_peaks_ids = find_peaks(ajia_data_item)
    start_time,end_time = get_item_start_end_time(ajia_data_item)
    xt_start_time = xt_pair_start_time(xt_pair, start_time)
    if xt_start_time is None:
        xt_start_time = xt_data.loc[xt_data.index[0]]["csvTime"][:-3]
    xt_end_time = xt_pair_end_time(xt_pair, end_time)
    if xt_end_time is None:
        xt_end_time = xt_data.loc[xt_data.index[-1]]["csvTime"][:-3]
    # 比较一下时间
    bs_time = min_time(xt_start_time,start_time)
    es_time = max_time(xt_end_time,end_time)
    if len(find_peaks_ids) == 1:
        # 找到峰值数据
        return huishou_single_peaks(ajia_data,dp_data,xt_data,xt_pair,ajia_data_item)
    
    if len(find_peaks_ids) < 1:
        # A架找不到动作
        # 找到折臂吊车数据
        
        # 找到折臂吊车向上开机和向下开机数据
    
        xt_patch = xt_data[(xt_data["csvTimeMinute"] >= xt_start_time) & (xt_data["csvTimeMinute"] <= xt_end_time)]
        xt_peaks_ids = find_xt_peaks(xt_patch)
        if len(xt_peaks_ids) == 0:
            return result, "折臂吊车无动作/A架无动作"
        # 找到折臂吊车的峰值
        if len(xt_peaks_ids) >= 2:
            # 找到折臂吊车的峰值
            # 仅可以判断 折臂吊车最后一个峰值是 小艇落座
            xt_peaks_idx = xt_peaks_ids[-1]
            xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
            result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"回收"})
            return result,"A架无动作/折臂吊车动作次数:%s次"%(len(xt_peaks_ids))
        else:
            return result, "折臂吊车无动作/A架无动作"
    
    # 如果 A架两个峰值为正常情况：
    # 回收过程 最后一个峰值为 征服者出水
    zfzcs_peak_index = find_peaks_ids[-1]
    zfzcs_peak_time = ajia_data_item.loc[zfzcs_peak_index]["csvTime"]
    result.append({"csvTimeMinute":zfzcs_peak_time[:-3],"csvTime":zfzcs_peak_time,"actionName":"征服者出水","deviceName":"A架","actionType":"回收"})
    # 上一分钟为缆绳挂妥o
    lsgt_peak_time = pd.to_datetime(zfzcs_peak_time) - pd.Timedelta(minutes=1)
    lsgt_peak_time = lsgt_peak_time.strftime("%Y-%m-%d %H:%M:%S")
    result.append({"csvTimeMinute":lsgt_peak_time[:-3],"csvTime":lsgt_peak_time,"actionName":"缆绳挂妥","deviceName":"A架","actionType":"回收"})
    
    # 以缆绳挂妥为基础往上是 小艇入水和小艇检查完毕
    xt_patch = xt_data[(xt_data["csvTimeMinute"] < lsgt_peak_time) &(xt_data["csvTimeMinute"] > bs_time)]
    xt_peaks_ids = find_xt_peaks(xt_patch)
    if len(xt_peaks_ids) == 0:
        pass
    elif len(xt_peaks_ids) == 1:
        # 找到折臂吊车的峰值
        # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
        xt_peaks_idx = xt_peaks_ids[-1]
        xt_rushui_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
        result.append({"csvTimeMinute":xt_rushui_csvTime[:-3],"csvTime":xt_rushui_csvTime,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"回收"})
    elif len(xt_peaks_ids) == 2:
        # 找到折臂吊车的峰值
        # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
        xt_peaks_idx = xt_peaks_ids[-1]
        xt_rushui_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
        result.append({"csvTimeMinute":xt_rushui_csvTime[:-3],"csvTime":xt_rushui_csvTime,"actionName":"小艇入水","deviceName":"折臂吊车","actionType":"回收"})
        xt_peaks_idx1 = xt_peaks_ids[-2]
        xt_jcwanbi_csvTime1 = xt_data.loc[xt_peaks_idx1]["csvTime"]
        result.append({"csvTimeMinute":xt_jcwanbi_csvTime1[:-3],"csvTime":xt_jcwanbi_csvTime1,"actionName":"小艇检查完毕","deviceName":"折臂吊车","actionType":"回收"})

    # 往下是小艇落座
    xt_patch = xt_data[(xt_data["csvTimeMinute"] > lsgt_peak_time) & (xt_data["csvTimeMinute"] <= es_time) ]
    xt_peaks_ids = find_xt_peaks(xt_patch)
    if len(xt_peaks_ids) == 0:
        pass
    else:
        # 找到折臂吊车的峰值
        # 仅可以判断 折臂吊车最后一个峰值是 小艇入水
        xt_peaks_idx = xt_peaks_ids[-1]
        xt_luozuo_csvTime = xt_data.loc[xt_peaks_idx]["csvTime"]
        result.append({"csvTimeMinute":xt_luozuo_csvTime[:-3],"csvTime":xt_luozuo_csvTime,"actionName":"小艇落座","deviceName":"折臂吊车","actionType":"回收"})


    # 至少有一个小艇入水时间
    # 先计算征服者落座时间
    zfzlz_patch = ajia_data[(ajia_data["csvTimeMinute"]>zfzcs_peak_time[:-3]) & (ajia_data["csvTimeMinute"] <= end_time)]
    zfzlz_index =  find_zfzlz_index(zfzlz_patch)
    if zfzlz_index is not None:
        zfzlz_csvTime = ajia_data.loc[zfzlz_index]["csvTime"]
        result.append({"csvTimeMinute":zfzlz_csvTime[:-3],"csvTime":zfzlz_csvTime,"actionName":"征服者落座","deviceName":"A架","actionType":"回收"})
    # 
    # 征服者出水 前两小时 和 A架开机哪个早设置哪个值
    zfzcs_peak_time2hour = pd.to_datetime(zfzcs_peak_time) -  pd.Timedelta(hours=2)
    zfzcs_peak_time2hour = zfzcs_peak_time2hour.strftime("%Y-%m-%d %H:%M:%S")
    bbs_time = min_time(zfzcs_peak_time2hour[:-3],start_time)
    ajia_zfzcs_patch = ajia_data[(ajia_data["csvTimeMinute"] > bbs_time) & (ajia_data["csvTimeMinute"] < zfzcs_peak_time[:-3])]
    ajia_baichu_index = find_ajia_baichu_index(ajia_zfzcs_patch)
    if ajia_baichu_index is not None:
        ajia_baichu_csvTime = ajia_data.loc[ajia_baichu_index]["csvTime"]
        result.append({"csvTimeMinute":ajia_baichu_csvTime[:-3],"csvTime":ajia_baichu_csvTime,"actionName":"A架摆出","deviceName":"A架","actionType":"回收"})

    return result,"结束"





def get_dp_startup_shutdown(dp_data):
    # 找到DP开机和关机数据
    dp_path_kg = dp_data[dp_data["dp_action"] != ""]
    startup_shutdown_pairs = []
    startup_time = None
    for _, row in dp_path_kg.iterrows():
        if row["dp_action"] == "ON DP":
            startup_time = row["csvTime"]
        elif row["dp_action"] == "OFF DP" and startup_time is not None:
            startup_shutdown_pairs.append({"开机时间": startup_time, "关机时间": row["csvTime"], "类型": "DP"})
            startup_time = None
    return startup_shutdown_pairs

def get_ajia_startup_shutdown(ajia_data):
    # 找到A架开机和关机数据
    ajia_path_kg = ajia_data[ajia_data["ajia_action"] != ""]
    xx_startup_shutdown_pairs = []
    startup_time = None
    for _, row in ajia_path_kg.iterrows():
        if row["ajia_action"] == "A架开机":
            startup_time = row["csvTime"]
        elif row["ajia_action"] == "A架关机" and startup_time is not None:
            xx_startup_shutdown_pairs.append({"开机时间": startup_time, "关机时间": row["csvTime"], "类型": "A架"})
            startup_time = None
    return xx_startup_shutdown_pairs


def dp_pair_action(dp_pair, xiafang_pair,huishou_pair):
    dp_result = []
    for dp in dp_pair:
        dp_start = pd.to_datetime(dp["开机时间"])
        dp_end = pd.to_datetime(dp["关机时间"])
        has_xiafang_intersection = False
        has_huishou_intersection = False
        for xiafang in xiafang_pair:
            xiafang_start = pd.to_datetime(xiafang["开始时间"])
            xiafang_end = pd.to_datetime(xiafang["结束时间"])
            # 判断是否有交集，包含相互包含的情况
            if not (dp_end < xiafang_start or dp_start > xiafang_end):
                has_xiafang_intersection = True
                break
        for huishou in huishou_pair:
            huishou_start = pd.to_datetime(huishou["开始时间"])
            huishou_end = pd.to_datetime(huishou["结束时间"])
            # 判断是否有交集，包含相互包含的情况
            if not (dp_end < huishou_start or dp_start > huishou_end):
                has_huishou_intersection = True
                break

        if has_xiafang_intersection:
            action_type = "下放"
        elif has_huishou_intersection:
            action_type = "回收"
        else:
            action_type = "其他"

        dp_result.append({
                "csvTime": dp["开机时间"],
                "csvTimeMinute": dp["开机时间"][:-3],
                "actionName": "ON DP",
                "deviceName": "DP",
                "actionType": action_type
            })
        dp_result.append({
                "csvTime": dp["关机时间"],
                "csvTimeMinute": dp["关机时间"][:-3],
                "actionName": "OFF DP",
                "deviceName": "DP",
                "actionType": action_type
            })

    return dp_result

def xt_pair_action(xt_pair, xiafang_pair, huishou_pair):
    xt_result = []
    for xt in xt_pair:
        xt_start = pd.to_datetime(xt["开机时间"])
        xt_end = pd.to_datetime(xt["关机时间"])
        has_xiafang_intersection = False
        has_huishou_intersection = False

        # 检查是否与下放时间段有交集
        for xiafang in xiafang_pair:
            xiafang_start = pd.to_datetime(xiafang["开始时间"])
            xiafang_end = pd.to_datetime(xiafang["结束时间"])
            if not (xt_end < xiafang_start or xt_start > xiafang_end):
                has_xiafang_intersection = True
                break

        # 检查是否与回收时间段有交集
        for huishou in huishou_pair:
            huishou_start = pd.to_datetime(huishou["开始时间"])
            huishou_end = pd.to_datetime(huishou["结束时间"])
            if not (xt_end < huishou_start or xt_start > huishou_end):
                has_huishou_intersection = True
                break

        if has_xiafang_intersection:
            action_type = "下放"
        elif has_huishou_intersection:
            action_type = "回收"
        else:
            action_type = "其他"

        xt_result.append({
            "csvTime": xt["开机时间"],
            "csvTimeMinute": xt["开机时间"][:-3],
            "actionName": "折臂吊车开机",
            "deviceName": "折臂吊车",
            "actionType": action_type
        })
        xt_result.append({
            "csvTime": xt["关机时间"],
            "csvTimeMinute": xt["关机时间"][:-3],
            "actionName": "折臂吊车关机",
            "deviceName": "折臂吊车",
            "actionType": action_type
        })
    return xt_result


def ajia_pair_action(ajia_pair_input, xiafang_time_list, huishou_time_list):
    ajia_result = []
    for idx,ajia in enumerate(ajia_pair_input):
        ajia_start = pd.to_datetime(ajia["开机时间"])
        ajia_end = pd.to_datetime(ajia["关机时间"])

        # 判断开机时间是否在 下放 时间段内
        start_action_type = "其他"
        for xiafang in xiafang_time_list:
            xiafang_start = pd.to_datetime(xiafang["开始时间"]) - pd.Timedelta(minutes=1)
            xiafang_end = pd.to_datetime(xiafang["结束时间"]) + pd.Timedelta(minutes=1)
            if xiafang_start <= ajia_start <= xiafang_end:
                start_action_type = "下放"
                break
        for huishou in huishou_time_list:
            huishou_start = pd.to_datetime(huishou["开始时间"]) - pd.Timedelta(minutes=1)
            huishou_end = pd.to_datetime(huishou["结束时间"]) + pd.Timedelta(minutes=1)
            if (huishou_start <= ajia_start) and (ajia_start <= huishou_end):
                start_action_type = "回收"
                break

        # 判断关机时间是否在 下放 时间段内
        end_action_type = "其他"
        for xiafang in xiafang_time_list:
            xiafang_start = pd.to_datetime(xiafang["开始时间"]) - pd.Timedelta(minutes=1)
            xiafang_end = pd.to_datetime(xiafang["结束时间"]) + pd.Timedelta(minutes=1)
            if xiafang_start <= ajia_end <= xiafang_end:
                end_action_type = "下放"
                break
        for huishou in huishou_time_list:
            huishou_start = pd.to_datetime(huishou["开始时间"]) - pd.Timedelta(minutes=1)
            huishou_end = pd.to_datetime(huishou["结束时间"]) + pd.Timedelta(minutes=1)
            if huishou_start <= ajia_end <= huishou_end:
                end_action_type = "回收"
                break

        # 添加开机记录
        ajia_result.append({
            "csvTime": ajia["开机时间"],
            "csvTimeMinute": ajia["开机时间"][:-3],
            "actionName": "A架开机",
            "deviceName": "A架",
            "actionType": start_action_type
        })

        # 添加关机记录
        ajia_result.append({
            "csvTime": ajia["关机时间"],
            "csvTimeMinute": ajia["关机时间"][:-3],
            "actionName": "A架关机",
            "deviceName": "A架",
            "actionType": end_action_type
        })
    return ajia_result
# print(ajia_data_chunk1)
def process_chunks(ajia_data, dp_data, xt_data, xt_pair, ajia_chunk):
    ac_result = []
    # 获取 DP开始时间和结束时间对
    dp_startup_shutdown_pairs = get_dp_startup_shutdown(dp_data)
    ajia_startup_shutdown_pairs = get_ajia_startup_shutdown(ajia_data)
    xt_startup_shutdown_pairs = get_xt_startup_shutdown(xt_data)
    # 获取 A架开始时间和结束时间对
    xiafang_time_list = []
    huishou_time_list = []
    for item in ajia_chunk:
        action_type = item["type"]
        itr_data = item["item"]
        if action_type == "下放":
            action_list,desc = task_xiafang(ajia_data, dp_data, xt_data, xt_pair, itr_data)
            # 获取 itr_data的开始时间和结束时间
            itr_data_start_time = itr_data.loc[itr_data.index[0]]["csvTime"]
            itr_data_end_time = itr_data.loc[itr_data.index[-1]]["csvTime"]
            xiafang_time_list.append({"开始时间":itr_data_start_time,"结束时间":itr_data_end_time,"类型":"下放"})
            ac_result += action_list
        else:
            action_list,desc = task_huishou(ajia_data, dp_data, xt_data, xt_pair, itr_data)
            # 获取 itr_data的开始时间和结束时间
            itr_data_start_time = itr_data.loc[itr_data.index[0]]["csvTime"]
            itr_data_end_time = itr_data.loc[itr_data.index[-1]]["csvTime"]
            huishou_time_list.append({"开始时间":itr_data_start_time,"结束时间":itr_data_end_time,"类型":"回收"})
            ac_result += action_list
    xx_ajia_result = ajia_pair_action(ajia_startup_shutdown_pairs, xiafang_time_list, huishou_time_list)
    dp_result = dp_pair_action(dp_startup_shutdown_pairs,xiafang_time_list,huishou_time_list)
    xt_result = xt_pair_action(xt_startup_shutdown_pairs,xiafang_time_list,huishou_time_list)

    ac_result += dp_result
    ac_result += xt_result
    ac_result += xx_ajia_result
    return ac_result


path = "../data/v2/"

def preprecess(path,ajia_file_name, dp_file_name, xt_file_name):

    df_ajia_plc_1 = pd.read_csv(path + ajia_file_name)
    # df_ajia_plc_2 = pd.read_csv(path+"Ajia_plc_1_2.csv")

    # dp
    df_dp_plc_1 = pd.read_csv(path + dp_file_name)
    # df_dp_plc_2 = pd.read_csv(path + "Port3_ksbg_9_2.csv")

    # 小艇 折臂吊车
    df_xt_1 = pd.read_csv(path + xt_file_name)
    # df_xt_2 = pd.read_csv(path + "device_13_11_meter_1311_2.csv")

    # 相关数据字段
    ajia_data_1 = df_ajia_plc_1[["Ajia-3_v", "Ajia-5_v", "csvTime"]]
    # ajia_data_2 = df_ajia_plc_2[["Ajia-3_v", "Ajia-5_v", "csvTime"]]

    ajia_data_1 = ajia_data_1.replace("error", "-1")
    # ajia_data_2 = ajia_data_2.replace("error", "-1")

    ajia_data_1['Ajia-3_v'] = ajia_data_1['Ajia-3_v'].astype(float)
    ajia_data_1['Ajia-5_v'] = ajia_data_1['Ajia-5_v'].astype(float)
    # ajia_data_2['Ajia-3_v'] = ajia_data_2['Ajia-3_v'].astype(float)
    # ajia_data_2['Ajia-5_v'] = ajia_data_2['Ajia-5_v'].astype(float)



    dp_plc_1 = df_dp_plc_1[["csvTime", "P3_33", "P3_18"]]
    # dp_plc_2 = df_dp_plc_2[["csvTime", "P3_33", "P3_18"]]

    xt_1 = df_xt_1[["13-11-6_v", "csvTime"]]
    # xt_2 = df_xt_2[["13-11-6_v", "csvTime"]]

    sz = len("2024-05-16 16:00")
    ajia_data_1["csvTimeMinute"] = ajia_data_1["csvTime"].apply(lambda x: x[:sz])
    # ajia_data_2["csvTimeMinute"] = ajia_data_2["csvTime"].apply(lambda x: x[:sz])
    dp_plc_1["csvTimeMinute"] = dp_plc_1["csvTime"].apply(lambda x: x[:sz])
    # dp_plc_2["csvTimeMinute"] = dp_plc_2["csvTime"].apply(lambda x: x[:sz])
    xt_1["csvTimeMinute"] = xt_1["csvTime"].apply(lambda x: x[:sz])
    # xt_2["csvTimeMinute"] = xt_2["csvTime"].apply(lambda x: x[:sz])
    # 状态初始化
    ajia_data_1["ajia_action"] = ""
    # ajia_data_2["ajia_action"] = ""
    dp_plc_1["dp_action"] = ""
    # dp_plc_2["dp_action"] = ""
    xt_1["xt_action"] = ""
    # xt_2["xt_action"] = ""
    ajia_data_1 = ajia_data_1.sort_values(by="csvTimeMinute")
    xt_1 = xt_1.sort_values(by="csvTimeMinute")
    dp_plc_1 = dp_plc_1.sort_values(by="csvTimeMinute")

    # 开机关机边界判断
    ajia_status1 = ajia_data_1.apply(lambda x: Ajia_zhuangtai(x), axis=1).diff().fillna(0)
    # ajia_status2 = ajia_data_2.apply(lambda x: Ajia_zhuangtai(x), axis=1).diff().fillna(0)

    ajia_data_1.loc[(ajia_status1 == 1), "ajia_action"] = "A架开机"
    ajia_data_1.loc[(ajia_status1 == -1), "ajia_action"] = "A架关机"
    # ajia_data_2.loc[(ajia_status2 == 1), "ajia_action"] = "A架开机"
    # ajia_data_2.loc[(ajia_status2 == -1), "ajia_action"] = "A架关机"
    #
    # dp 开机关机判断
    dp_plc_1["P3_33_PRE"] = dp_plc_1["P3_33"].shift(1)
    dp_plc_1["P3_18_PRE"] = dp_plc_1["P3_18"].shift(1)
    # dp_plc_2["P3_33_PRE"] = dp_plc_2["P3_33"].shift(1)
    # dp_plc_2["P3_18_PRE"] = dp_plc_2["P3_18"].shift(1)

    dp_status1 = dp_plc_1.apply(lambda x: dp_status(x), axis=1).diff().fillna(0)
    # dp_status2 = dp_plc_2.apply(lambda x: dp_status(x), axis=1).diff().fillna(0)

    dp_plc_1.loc[(dp_status1 == 1), "dp_action"] = "ON DP"
    dp_plc_1.loc[(dp_status1 == -1), "dp_action"] = "OFF DP"
    # dp_plc_2.loc[(dp_status2 == 1), "dp_action"] = "ON DP"
    # dp_plc_2.loc[(dp_status2 == -1), "dp_action"] = "OFF DP"

    del dp_plc_1["P3_33_PRE"]
    del dp_plc_1["P3_18_PRE"]
    # del dp_plc_2["P3_33_PRE"]
    # del dp_plc_2["P3_18_PRE"]

    xt_status1 = xt_1.apply(lambda x: diaoche_zhuangtai(x), axis=1).diff().fillna(0)
    # xt_status2 = xt_2.apply(lambda x: diaoche_zhuangtai(x), axis=1).diff().fillna(0)
    xt_1.loc[(xt_status1 == 1), "xt_action"] = "折臂吊车开机"
    xt_1.loc[(xt_status1 == -1), "xt_action"] = "折臂吊车关机"
    # xt_2.loc[(xt_status2 == 1), "xt_action"] = "折臂吊车开机"
    # xt_2.loc[(xt_status2 == -1), "xt_action"] = "折臂吊车关机"




    #
    # 找到所有峰值段落，表示 有可能进行 深海作业A
    peaks_indices1 = find_peaks(ajia_data_1)
    # peaks_indices2 = find_peaks(ajia_data_2)

    ## 根据峰值 分割数据
    ajia_dataset1 = peaks_split_data(ajia_data_1,peaks_indices1)
    # ajia_dataset2 = peaks_split_data(ajia_data_2,peaks_indices2)

    # 根据峰值 上下划分数据段
    ajia_fields1 = data_fields_by_index(ajia_data_1,ajia_dataset1)
    # ajia_fields2 = data_fields_by_index(ajia_data_2,ajia_dataset2)

    # 根据断层数据 对数据段进行再划分
    ajia_data_fields1 = resub(ajia_fields1)
    # ajia_data_fields2 = resub(ajia_fields2)

    ajia_data_fields1 = filter_unpeaks_data(ajia_data_fields1)
    # ajia_data_fields2 = filter_unpeaks_data(ajia_data_fields2)

    ajia_data_fields1 = filter_xt_unpeaks_data(ajia_data_fields1,xt_1)
    # ajia_data_fields2 = filter_xt_unpeaks_data(ajia_data_fields2,xt_2)
    ajia_data_dict1 = date_split_data(ajia_data_fields1)
    # ajia_date_dict2 = date_split_data(ajia_data_fields2)
    ajia_date_num1 = get_date_chunks_num(ajia_data_dict1)
    # ajia_date_num2 = get_date_chunks_num(ajia_date_dict2)


    ajia_data_chunk1 = process_date_dict(ajia_data_dict1,xt_1)
    # ajia_data_chunk2 = process_date_dict(ajia_date_dict2,xt_2)
    xt_pair1 = get_xt_startup_shutdown(xt_1)
    # xt_pair2 = get_xt_startup_shutdown(xt_2)
    # xt_pair = xt_pair1 + xt_pair2
    xt_pair1 = sorted(xt_pair1,key=lambda x:x["开机时间"])

    # print(process)
    actions_output = process_chunks(ajia_data_1,dp_plc_1,xt_1,xt_pair1,ajia_data_chunk1)
    # actions_output2 = process_chunks(ajia_data_2,dp_plc_2,xt_2,xt_pair,ajia_data_chunk2)

    return actions_output

ajia_file_name = "Ajia_plc_1.csv"
dp_file_name = "Port3_ksbg_9.csv"
xt_file_name = "device_13_11_meter_1311.csv"
result = preprecess(path, ajia_file_name, dp_file_name, xt_file_name)
# print(result)
dfx = pd.DataFrame(result)
dfx = dfx.sort_values("csvTime")
dfx = dfx.reset_index(drop=True)

# # # # 数据表
import glob
import pandas as pd
paths = glob.glob("../data/v2/[a-zA-Z]*.csv")
import pymysql
from sqlalchemy import create_engine
# #
# # # 后续需要自行处理字段 需要增加库表情况 设置 数据库 ship1
engine = create_engine('mysql+pymysql://root:qweasd@10.5.101.152:3306/ship3')
#
# def dtype_col(df):
#     for i in df.columns:
#         if "Ajia" in i:
#             df[i] = df[i].astype(float)
#         if "PLC" in i:
#             df[i] = df[i].astype(float)
#
# # one file
# for path in paths:
#     if "Ajia" in path:
#         file_name = os.path.basename(path)
#         file_name = file_name.split(".")[0]
#         name = file_name
#         df = pd.read_csv(path)
#         df = df.sort_values(by="csvTime").reset_index(drop=True)
#         del df["Unnamed: 0"]
#         df = df.replace("error",-1)
#         dtype_col(df)
#         df.to_sql(name,con=engine,if_exists="replace",index=False)
#     else:
#         file_name = os.path.basename(path)
#         file_name = file_name.split(".")[0]
#         name = file_name
#         df = pd.read_csv(path)
#         df = df.sort_values(by="csvTime").reset_index(drop=True)
#         del df["Unnamed: 0"]
#         df = df.replace("error", -1)
#         dtype_col(df)
#         df.to_sql(name,con=engine,if_exists="replace",index=False)
# #
dfx.to_sql("task_action",con=engine,if_exists="replace",index=False)