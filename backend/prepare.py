import akshare as ak
import pandas as pd
import os
from datetime import datetime
from utils import setup_static_directory, uplimit10jqka,get_latest_trade_date, is_trading_time


# 获取数据
def prepareChanges(current_date:str):
    import time
    # 类型映射字典
    type_mapping = {
        # '8201': '火箭发射',
        # '8202': '快速反弹',
        # '8193': '大笔买入',
        '4': '封涨停板',
        # '32': '打开跌停板',
        # '64': '有大买盘',
        # '8207': '竞价上涨',
        # '8209': '高开5日线',
        # '8211': '向上缺口',
        # '8213': '60日新高',
        # '8215': '60日大幅上涨',
        # '8204': '加速下跌',
        # '8203': '高台跳水',
        # '8194': '大笔卖出',
        # '8': '封跌停板',
        # '16': '打开涨停板',
        # '128': '有大卖盘',
        # '8208': '竞价下跌',
        # '8210': '低开5日线',
        # '8212': '向下缺口',
        # '8214': '60日新低',
        # '8216': '60日大幅下跌'
    }
    all_df = []
    for code, type_value in type_mapping.items():
        print(f'[prepare] 正在请求类型: {type_value} (symbol={type_value})')
        df = ak.stock_changes_em(symbol=type_value)
        print(df.columns)
        df['类型'] = type_value  # 使用type_value而不是df['板块']
        df = df.drop(['板块'], axis=1)  # 添加axis=1参数
        df = df[~df['名称'].str.contains('ST')]
        all_df.append(df)
        print('[prepare] 等待5秒...')
        time.sleep(5)
    df = pd.concat(all_df, ignore_index=True)

    # 确保时间列是字符串类型
    df['时间'] = df['时间'].astype(str)
    print(f'[prepare] 转换后时间列数据类型: {df["时间"].dtype}')

    # 拆分相关信息
    info_df = df['相关信息'].str.split(',', expand=True)
    if info_df.shape[1] >= 3:
        df['涨跌幅'] = pd.to_numeric(info_df[0], errors='coerce')
    df['涨跌幅'] = df.apply(lambda x: 0.3 if x['类型'] == '封涨停板' and x['代码'].startswith('8') else 
                             0.2 if x['类型'] == '封涨停板' and (x['代码'].startswith('300') or x['代码'].startswith('688')) else 
                             0.1, axis=1)
    df = df[(df['涨跌幅'] < 0.31) & ((df['涨跌幅'] >= 0.05) | (df['涨跌幅'] <= -0.05))]
    # 读取板块概念数据
    static_dir = setup_static_directory()
    concepts_path = os.path.join(static_dir, "concepts.csv")
    print(f'[prepare] 读取板块概念文件: {concepts_path}')

    # 初始化板块名称列
    df['板块名称'] = '未知板块'

    if os.path.exists(concepts_path):
        concepts_df = pd.read_csv(concepts_path)
        print(f'[prepare] 板块概念数据列: {concepts_df.columns.tolist()}')
        print(f'[prepare] 板块概念数据样例:\n{concepts_df.head()}')

        # 创建股票名称到板块的映射
        stock_to_sector_map = dict(zip(concepts_df['股票名称'], concepts_df['板块名称']))

        # 为df添加板块信息
        df['板块名称'] = df['名称'].map(stock_to_sector_map)

        # 处理未找到板块的股票
        missing_sector_count = df['板块名称'].isna().sum()
        if missing_sector_count > 0:
            print(f'[prepare] 有{missing_sector_count}只股票未找到对应板块，使用默认板块')
            df['板块名称'] = df['板块名称'].fillna('未知板块')

        print(f'[prepare] 板块分配完成，前5个股票的板块: {df[["名称", "板块名称"]].head().to_dict("records")}')
    else:
        print(f'[prepare] 板块概念文件不存在: {concepts_path}')


    # 构造输出DataFrame
    output_df = pd.DataFrame()
    output_df['股票代码'] = df['代码']  # 添加股票代码列
    output_df['时间'] = df['时间'].str[:5]  # 只保留HH:MM
    output_df['名称'] = df['名称']
    output_df['相关信息'] = df['涨跌幅'].apply(lambda x: f"%+.2f" % (x * 100) + "%" if pd.notnull(x) else 'NaN')

    output_df['类型'] = df['类型']
    output_df['板块名称'] = df['板块名称']  # 添加板块名称列
    print(f'[prepare] 类型分布: {output_df["类型"].value_counts().to_dict()}')

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
    output_df = output_df.sort_values(['上下午', '板块名称', '时间排序'])

    # 保存到CSV，使用与queue一致的路径和命名规则
    uplimit_df = uplimit10jqka(current_date)
    
    # 创建股票代码到high_days的映射
    code_to_high_days_map = dict(zip(uplimit_df['code'].astype(str), uplimit_df['high_days']))
    
    # 为output_df添加high_days列
    output_df['标识'] = output_df['股票代码'].map(code_to_high_days_map)
    
    # 处理未找到high_days的股票，填充空值
    missing_high_days_count = output_df['标识'].isna().sum()
    if missing_high_days_count > 0:
        print(f'[prepare] 有{missing_high_days_count}只股票未找到对应high_days，填充空值')
        output_df['标识'] = output_df['标识'].fillna('')

    save_path = os.path.join(static_dir, f"changes_{current_date}.csv")
    output_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f'[prepare] 已保存到: {save_path}, 共{len(output_df)}条记录')


# prepareChanges(get_latest_trade_date())