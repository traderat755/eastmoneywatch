import akshare as ak
import pandas as pd
import os
import logging
from utils import setup_static_directory, uplimit10jqka
from config import type_mapping
import time


# 获取数据
def prepareChanges(current_date:str):
    static_dir = setup_static_directory()
    all_df = []
    for code, type_value in type_mapping.items():
        logging.debug(f'[prepare] 正在请求类型: {type_value} (symbol={type_value})')
        df = ak.stock_changes_em(symbol=type_value)
        logging.debug(df.columns)
        df['类型'] = type_value  # 使用type_value而不是df['板块']
        df = df.drop(['板块'], axis=1)  # 添加axis=1参数
        df = df[~df['名称'].str.contains('ST')]
        all_df.append(df)
        logging.debug('[prepare] 等待5秒...')
        time.sleep(5)
    df = pd.concat(all_df, ignore_index=True)

    # 确保时间列是字符串类型
    df['时间'] = df['时间'].astype(str)
    logging.debug(f'[prepare] 转换后时间列数据类型: {df["时间"].dtype}')

    # 拆分相关信息
    info_df = df['相关信息'].str.split(',', expand=True)
    if info_df.shape[1] >= 3:
        df['涨跌幅'] = pd.to_numeric(info_df[0], errors='coerce')
    
    # 只对特定类型的涨停板使用固定涨跌幅，其他情况保留解析出的数据
    def calculate_change_rate(row):
        if row['类型'] == '封涨停板':
            if row['代码'].startswith('8'):
                return 0.3
            elif row['代码'].startswith('300') or row['代码'].startswith('688'):
                return 0.2
            else:
                return 0.1
        else:
            # 对于非涨停板，使用解析出的涨跌幅数据
            return row['涨跌幅'] if pd.notnull(row['涨跌幅']) else 0.0
    
    df['涨跌幅'] = df.apply(calculate_change_rate, axis=1)
    df = df[(df['涨跌幅'] < 0.31) & ((df['涨跌幅'] >= 0.05) | (df['涨跌幅'] <= -0.05))]
   
    # 构造输出DataFrame
    output_df = pd.DataFrame()
    output_df['股票代码'] = df['代码']  # 添加股票代码列
    output_df['时间'] = df['时间'].str[:5]  # 只保留HH:MM
    output_df['名称'] = df['名称']
    output_df['相关信息'] = df['涨跌幅'].apply(lambda x: f"%+.2f" % (x * 100) + "%" if pd.notnull(x) else 'NaN')

    output_df['类型'] = df['类型']
    logging.debug(f'[prepare] 类型分布: {output_df["类型"].value_counts().to_dict()}')

    output_df['四舍五入取整'] = df['涨跌幅'].apply(lambda x: int(round(x * 100)) if pd.notnull(x) else None)

    def am_pm_col(tm):
        try:
            hour = int(tm[:2])
            return '上午' if hour < 12 else '下午'
        except:
            return '未知'
    output_df['上下午'] = output_df['时间'].apply(am_pm_col)
    output_df['时间排序'] = output_df['时间'].apply(lambda tm: int(tm[:2])*60 + int(tm[3:5]) if isinstance(tm, str) and len(tm) == 5 else 0)

    # 去重，按名称+类型、名称+时间
    output_df = output_df.drop_duplicates(subset=['股票代码', '类型'], keep='last')
    output_df = output_df.drop_duplicates(subset=['股票代码', '时间'], keep='last')
    # 按时间排序
    # 保存到CSV，使用与queue一致的路径和命名规则
    uplimit_df = uplimit10jqka(current_date)

    # 创建股票代码到high_days的映射
    code_to_high_days_map = dict(zip(uplimit_df['code'].astype(str), uplimit_df['high_days']))

    # 为output_df添加high_days列
    output_df['标识'] = output_df['股票代码'].map(code_to_high_days_map)

    # 处理未找到high_days的股票，填充空值
    missing_high_days_count = output_df['标识'].isna().sum()
    if missing_high_days_count > 0:
        logging.debug(f'[prepare] 有{missing_high_days_count}只股票未找到对应high_days，填充空值')
        output_df['标识'] = output_df['标识'].fillna('').infer_objects(copy=False)
 
    save_path = os.path.join(static_dir, f"changes_{current_date}.csv")
    output_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    logging.debug(f'[prepare] 已保存到: {save_path}, 共{len(output_df)}条记录')


# prepareChanges(get_latest_trade_date())