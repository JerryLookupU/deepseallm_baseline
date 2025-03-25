import pandas as pd
# 获取渡航可能时间段：
import pandas as pd
# 已经确认 主推进器 时间段内

def get_long_process_time(data_path):
    # 渡航状态
    # 读取数据
    port3 = pd.read_csv(f'{data_path}/Port3_ksbg_8.csv')
    port4 = pd.read_csv(f'{data_path}/Port4_ksbg_7.csv')

    # 处理时间列（统一到分钟精度）
    port3['time'] = pd.to_datetime(port3['csvTime'].str[:16], format='%Y-%m-%d %H:%M')
    port4['time'] = pd.to_datetime(port4['csvTime'].str[:16], format='%Y-%m-%d %H:%M')

    # 合并数据（内连接）
    merged = pd.merge(
        port3[['time', 'P3_32', 'P3_15']],
        port4[['time', 'P4_15', 'P4_16']],
        on='time',
        how='inner'
    )

    # 生成是否有效标记列
    merged['is_valid'] = (
        (merged['P3_32'] > 1500) &
        (merged['P4_15'] > 1500) &
        (merged['P3_15'] > 100) &
        (merged['P4_16'] > 100)
    ).astype(int)

    # 计算状态变化点
    merged = merged.sort_values('time')
    merged['status_change'] = merged['is_valid'] - merged['is_valid'].shift(1, fill_value=0)

    # 筛选状态变化点（1或-1）
    changes = merged[merged['status_change'].isin([1, -1])][['time', 'status_change']]

    # 分离开始和结束标记
    starts = changes[changes['status_change'] == 1].copy()
    ends = changes[changes['status_change'] == -1].copy()

    # 修正点：重命名右表时间列并确保列存在
    if not starts.empty and not ends.empty:
        # 重命名ends的时间列为end_time
        ends = ends.rename(columns={'time': 'end_time'})

        # 使用merge_asof进行前向匹配
        pairs = pd.merge_asof(
            starts.sort_values('time'),
            ends.sort_values('end_time'),
            left_on='time',
            right_on='end_time',
            direction='forward'
        )

        # 处理可能的NaN并计算时长
        pairs = pairs.dropna(subset=['end_time'])
        pairs['duration'] = (pairs['end_time'] - pairs['time']).dt.total_seconds() / 60

        # 筛选持续时间≥240分钟的记录
        result = pairs[pairs['duration'] >= 1][['time', 'end_time', 'duration']]
        result.columns = ['start_time', 'end_time', 'duration']
    else:
        result = pd.DataFrame(columns=['start_time', 'end_time', 'duration'])

    return result


import pandas as pd
from pathlib import Path


def process_ksbg_data(data_path, min_duration=300, max_gap=120):
    # 停泊状态
    """
    处理港口数据并合并相邻时间段（优化版）

    参数：
    data_path   : 数据目录路径（包含 Port3_ksbg_8.csv 和 Port4_ksbg_7.csv）
    min_duration: 最小有效持续时间（分钟），默认300
    max_gap     : 最大允许间隔（分钟），默认180（3小时）

    返回：
    DataFrame  : 包含合并后的时间段 [start_time, end_time, duration]
    """
    # ========================= 数据加载与预处理 =========================
    # 使用Path处理路径
    data_path = Path(data_path)

    # 读取数据并标准化时间列
    def load_and_clean_port(port_file):
        df = pd.read_csv(port_file)
        df['time'] = pd.to_datetime(df['csvTime'].str[:16], format='%Y-%m-%d %H:%M')
        return df.drop(columns=['csvTime']).drop_duplicates('time')

    port3 = load_and_clean_port(data_path / 'Port3_ksbg_8.csv')
    port4 = load_and_clean_port(data_path / 'Port4_ksbg_7.csv')

    # 合并数据集
    merged = pd.merge(
        port3[['time', 'P3_32', 'P3_15']],
        port4[['time', 'P4_15', 'P4_16']],
        on='time',
        how='inner'
    ).sort_values('time')

    # ========================= 有效时段标记 =========================
    merged['is_valid'] = (
        (merged['P3_32'] <= 0) &
        (merged['P4_15'] <= 0) &
        (merged['P3_15'] <= 0) &
        (merged['P4_16'] <= 0)
    ).astype(int)

    # ========================= 状态变化检测 =========================
    # 计算状态变化点（向量化操作）
    merged['status_change'] = merged['is_valid'].diff().fillna(merged['is_valid'])
    changes = merged.query('status_change != 0')[['time', 'status_change']]

    # ========================= 时间配对优化 =========================
    # 分离开始/结束点
    starts = changes[changes['status_change'] == 1][['time']]
    ends = changes[changes['status_change'] == -1][['time']]

    # 使用merge_asof高效配对
    if not starts.empty and not ends.empty:
        ends = ends.rename(columns={'time': 'end_time'})
        pairs = pd.merge_asof(
            starts.sort_values('time'),
            ends.sort_values('end_time'),
            left_on='time',
            right_on='end_time',
            direction='forward'
        ).dropna()

        # 计算持续时间
        pairs['duration'] = (pairs['end_time'] - pairs['time']).dt.total_seconds() / 60
        valid_pairs = pairs[pairs['duration'] >= min_duration]
    else:
        valid_pairs = pd.DataFrame(columns=['time', 'end_time', 'duration'])

    # ========================= 时段合并优化 =========================
    def merge_intervals(df, max_gap):
        if df.empty:
            return df

        intervals = df.sort_values('end_time').to_records(index=False)
        merged = []
        current_start, current_end = intervals[0][0], intervals[0][1]

        for start, end, _ in intervals[1:]:
            if (pd.to_datetime(start) - pd.to_datetime(current_end)).total_seconds() / 60 <= max_gap:
                current_end = max(current_end, end)  # 处理包含关系
            else:
                merged.append((current_start, current_end))
                current_start, current_end = start, end
        merged.append((current_start, current_end))

        return pd.DataFrame(merged, columns=['start_time', 'end_time'])

    # 合并相邻时段
    result_df = merge_intervals(valid_pairs.rename(columns={'time': 'start_time'}), max_gap)

    # 计算最终持续时间
    if not result_df.empty:
        result_df['duration'] = (result_df['end_time'] - result_df['start_time']).dt.total_seconds() / 60

    return result_df.reset_index(drop=True)


import pandas as pd


def analyze_dp_periods(port3_9_path, port3_8_path, port4_7_path, start_time, end_time):
    # 读取CSV文件
    port3_9 = pd.read_csv(port3_9_path, parse_dates=['csvTime'])
    port3_8 = pd.read_csv(port3_8_path, parse_dates=['csvTime'])
    port4_7 = pd.read_csv(port4_7_path, parse_dates=['csvTime'])

    # 时间对齐处理（截断到分钟）
    for df in [port3_9, port3_8, port4_7]:
        df['time'] = df['csvTime'].dt.floor('min')

    # 合并数据（INNER JOIN）
    merged = port3_9.merge(port3_8, on='time', how='inner', suffixes=('', '_y')) \
        .merge(port4_7, on='time', how='inner', suffixes=('', '_z'))

    # 筛选指定时间段
    merged = merged[(merged['time'] >= start_time) & (merged['time'] < end_time)]

    # 重命名列（根据SQL中的列别名）
    merged = merged.rename(columns={
        'P3_33': 'dp_control',
        'P3_18': 'dp_power',
        'P3_32': 'prop1_allow',
        'P4_15': 'prop2_allow',
        'P3_15': 'prop1_power',
        'P4_16': 'prop2_power'
    })[['time', 'dp_control', 'dp_power', 'prop1_allow', 'prop2_allow', 'prop1_power', 'prop2_power']]

    # 步骤2: 标记有效时间段
    merged['is_dp_active'] = (
        (merged['dp_control'] > 0) &
        (merged['dp_power'] > 0) &
        (merged['prop1_allow'] > 0) &
        (merged['prop2_allow'] > 0) &
        (merged['prop1_allow'] < 1500) &
        (merged['prop2_allow'] < 1500)
    ).astype(int)

    # 步骤3: 检测状态变化点
    merged = merged.sort_values('time')
    merged['prev_status'] = merged['is_dp_active'].shift(1, fill_value=0)
    merged['status_change'] = merged['is_dp_active'] - merged['prev_status']

    # 步骤4: 筛选变化点
    filtered_changes = merged[merged['status_change'].isin([1, -1])][['time', 'status_change']]

    # 步骤5: 生成时间段
    time_pairs = []
    start_stack = []

    for _, row in filtered_changes.sort_values('time').iterrows():
        if row['status_change'] == 1:
            start_stack.append(row['time'])
        elif row['status_change'] == -1 and start_stack:
            start_time = start_stack.pop(0)
            end_time = row['time']
            duration = (end_time - start_time).total_seconds() / 60
            time_pairs.append({
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration
            })

    # 转换为DataFrame并过滤
    result_df = pd.DataFrame(time_pairs)
    result_df = result_df[result_df['duration'] >= 5]

    # 格式化持续时间
    result_df['duration_formatted'] = result_df['duration'].apply(
        lambda x: f"{int(x // 60)}小时{int(x % 60)}分钟"
    )

    # 整理列顺序
    return result_df[['start_time', 'end_time', 'duration', 'duration_formatted']]




res = pd.read_csv("result.csv")
# 示例调用
include_times = get_long_process_time('../data/v2/')
outline_times = process_ksbg_data('../data/v2/')
dir_path = '../data/v2/'
# 使用示例
result = analyze_dp_periods(
    port3_9_path=dir_path + 'Port3_ksbg_9.csv',
    port3_8_path=dir_path +  'Port3_ksbg_8.csv',
    port4_7_path=dir_path + 'Port4_ksbg_7.csv',
    start_time='2024-05-04 00:00:00',
    end_time='2024-08-04 23:59:59'
)
for idx,row in result.iterrows():
    ctime = row["start_time"]
    patch1 = include_times[(include_times["start_time"] >= ctime) & (include_times["end_time"] <= ctime)]
    if len(patch1) > 0:
        print("\n")
        print(patch1)
        print("====")
        print(row)
    ctime = row["end_time"]
    patch2 = include_times[(include_times["start_time"] >= ctime) & (include_times["end_time"] <= ctime)]
    if len(patch2) > 0:
        print("\n")
        print(patch2)
        print("====")
        print(row)