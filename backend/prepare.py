import akshare as ak
import pandas as pd
import os
from datetime import datetime
from utils import setup_static_directory

# 获取数据
def prepareChanges():
    print('[prepare] 开始获取大笔买入数据')
    df = ak.stock_changes_em(symbol="大笔买入")
    print(f'[prepare] 原始数据列: {df.columns.tolist()}')
    print(f'[prepare] 原始数据样例:\n{df.head()}')
    print(f'[prepare] 时间列数据类型: {df["时间"].dtype}')
    print(f'[prepare] 时间列前5个值: {df["时间"].head().tolist()}')

    # 确保时间列是字符串类型
    df['时间'] = df['时间'].astype(str)
    print(f'[prepare] 转换后时间列数据类型: {df["时间"].dtype}')

    # 拆分相关信息
    info_df = df['相关信息'].str.split(',', expand=True)
    if info_df.shape[1] >= 3:
        df['成交量'] = pd.to_numeric(info_df[0], errors='coerce')
        df['价格'] = pd.to_numeric(info_df[1], errors='coerce')
        df['涨跌幅'] = pd.to_numeric(info_df[2], errors='coerce')
    else:
        print('[prepare] 相关信息列格式异常')
        df['涨跌幅'] = None

    # 读取板块概念数据
    static_dir = setup_static_directory()
    concepts_path = os.path.join(static_dir, "concepts.csv")
    print(f'[prepare] 读取板块概念文件: {concepts_path}')
    
    if os.path.exists(concepts_path):
        concepts_df = pd.read_csv(concepts_path, encoding='utf-8-sig')
        print(f'[prepare] 板块概念数据列: {concepts_df.columns.tolist()}')
        print(f'[prepare] 板块概念数据样例:\n{concepts_df.head()}')
        
        # 创建股票名称到板块名称的映射
        stock_to_sector = {}
        for _, row in concepts_df.iterrows():
            stock_name = row['股票名称'].strip()
            sector_name = row['板块名称'].strip()
            if stock_name not in stock_to_sector:
                stock_to_sector[stock_name] = []
            stock_to_sector[stock_name].append(sector_name)
        
        print(f'[prepare] 创建了{len(stock_to_sector)}个股票的板块映射')
        
        # 为每个股票分配板块
        def get_sector(stock_name):
            if stock_name in stock_to_sector:
                sectors = stock_to_sector[stock_name]
                # 如果有多个板块，返回第一个
                return sectors[0] if sectors else '大笔买入'
            return '大笔买入'
        
        df['板块'] = df['名称'].apply(get_sector)
        print(f'[prepare] 板块分配完成，前5个股票的板块: {df[["名称", "板块"]].head().to_dict("records")}')
    else:
        print('[prepare] 板块概念文件不存在，使用默认板块')
        df['板块'] = '大笔买入'

    # 构造输出DataFrame
    output_df = pd.DataFrame()
    output_df['板块名称'] = df['板块']
    output_df['时间'] = df['时间'].str[:5]  # 只保留HH:MM
    output_df['名称'] = df['名称']
    output_df['相关信息'] = df['涨跌幅'].apply(lambda x: f"%+.2f" % (x * 100) + "%" if pd.notnull(x) else 'NaN')
    output_df['类型'] = df['板块']  # 使用从concepts.csv获取的板块
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
    output_df = output_df.drop_duplicates(subset=['名称', '类型'], keep='last')
    output_df = output_df.drop_duplicates(subset=['名称', '时间'], keep='last')

    # 按时间排序
    output_df = output_df.sort_values(['上下午', '板块名称', '时间排序'])

    # 保存到CSV，使用与queue一致的路径和命名规则
    current_date = datetime.now().strftime("%Y%m%d")
    save_path = os.path.join(static_dir, f"changes_{current_date}.csv")
    output_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f'[prepare] 已保存到: {save_path}, 共{len(output_df)}条记录')

