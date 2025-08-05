import pandas as pd
import time
import os
import akshare as ak
from fluctuation import getChanges
from utils import setup_static_directory, uplimit10jqka,get_latest_trade_date, is_trading_time
from datetime import datetime

def apply_concept_df_sorting(df, concept_df):
    """应用与concept_df相同的板块排序逻辑，并添加完整的时间排序"""
    if df.empty:
        return df
    
    # 如果df中没有板块代码字段，但有名称字段，尝试从concept_df中获取板块代码
    if '板块代码' not in df.columns and '名称' in df.columns and '板块代码' in concept_df.columns:
        print("[get_changes_worker_queue] 现有数据缺少板块代码字段，尝试从concept_df中获取")
        # 创建股票名称到板块代码的映射
        stock_to_sector_map = dict(zip(concept_df['股票名称'], concept_df['板块代码']))
        df['板块代码'] = df['名称'].map(stock_to_sector_map)
        print(f"[get_changes_worker_queue] 已为{len(df[df['板块代码'].notna()])}条记录添加板块代码")
    
    # 先按时间排序
    df = df.sort_values('时间')
    
    # 添加上下午列
    def am_pm_col(tm):
        hour = int(tm[:2])
        return '上午' if hour < 12 else '下午'
    df['上下午'] = df['时间'].apply(am_pm_col)
    
    # 新增一列用于排序，转为分钟数
    df['时间排序'] = df['时间'].apply(lambda tm: int(tm[:2])*60 + int(tm[3:5]))
    
    # 如果有板块代码字段，应用板块排序
    if '板块代码' in df.columns and '板块代码' in concept_df.columns:
        # 获取concept_df中板块代码的顺序
        concept_sector_order = concept_df['板块代码'].unique().tolist()
        
        # 创建排序映射：concept_df中的板块顺序作为排序权重
        sector_order_map = {sector: idx for idx, sector in enumerate(concept_sector_order)}
        
        # 为df添加板块排序字段
        df['_sector_order'] = df['板块代码'].map(lambda x: sector_order_map.get(x, len(concept_sector_order)))
        
        # 按上下午、板块顺序、板块名称、时间排序
        df_sorted = df.sort_values(['上下午', '_sector_order', '板块名称', '时间排序']).drop('_sector_order', axis=1)
    else:
        # 如果没有板块代码，只按上下午、板块名称、时间排序
        df_sorted = df.sort_values(['上下午', '板块名称', '时间排序'])
    
    # 删除临时的排序字段，但保留上下午字段
    df_sorted = df_sorted.drop(['时间排序'], axis=1)
    
    return df_sorted


def worker(concept_df, queue, interval=3, batch_interval=300):
    print("[get_changes_worker_queue] 启动，推送到主进程Queue并定时批量写入磁盘")

    # 设置静态目录
    static_dir = setup_static_directory()

    # 获取当前交易日
    current_date = get_latest_trade_date()
    changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
    print(f"[get_changes_worker_queue] 当日changes文件路径: {changes_path}")
    print(f"[get_changes_worker_queue] 检查changes文件是否存在: {changes_path} -> {os.path.exists(changes_path)}")

    # 启动时先读取当日的changes.csv文件
    if os.path.exists(changes_path):
        print(f"[get_changes_worker_queue] changes文件存在，准备读取: {changes_path}")
        try:
            master_df = pd.read_csv(changes_path)
            print(f"[get_changes_worker_queue] 读取到当日已存在的changes数据，记录数: {len(master_df)}")
            
            # 应用与concept_df相同的排序
            if not master_df.empty:
                master_df = apply_concept_df_sorting(master_df, concept_df)
                print(f"[get_changes_worker_queue] 已对现有数据应用concept_df排序")
        except Exception as e:
            print(f"[get_changes_worker_queue] 读取当日changes.csv失败: {e}")
            master_df = pd.DataFrame()
    else:
        print(f"[get_changes_worker_queue] changes文件不存在: {changes_path}")
        master_df = pd.DataFrame()

    # 启动时，将现有数据推送到队列
    if not master_df.empty:
        initial_data = master_df.to_dict(orient="records")
        queue.put(initial_data)
        print(f"[get_changes_worker_queue] 已将当日现有数据推送到队列")
    else:
        print(f"[get_changes_worker_queue] 现有数据为空，不推送到队列")


    last_write = time.time()
    last_date = current_date
    # 缓存涨停板数据，避免频繁请求
    uplimit_cache = None
    last_uplimit_update = 0
    uplimit_cache_interval = 300  # 5分钟更新一次涨停板数据
    
    # 交易日检测相关变量
    last_trade_date_check = time.time()
    morning_check_time = None  # 记录早上9点检查的时间

    while True:
        try:
            now = time.time()
            current_hour = datetime.now().hour
            current_minute = datetime.now().minute
            
            # 交易日检测策略：
            # 1. 启动时已检测
            # 2. 每天早上9点前后5分钟内检查一次（开盘前确保交易日正确）
            should_check_trade_date = False
            
            # 每天早上9点前后5分钟内检查一次
            if (current_hour == 9 and current_minute <= 5) or (current_hour == 8 and current_minute >= 55):
                if morning_check_time is None or (now - morning_check_time) >= 3600:  # 确保1小时内只检查一次
                    should_check_trade_date = True
                    morning_check_time = now
                    print(f"[get_changes_worker_queue] 早上9点前后检查交易日变化")
            
            # 执行交易日检测
            if should_check_trade_date:
                current_date = get_latest_trade_date()
                if current_date != last_date:
                    # 交易日变化，更新文件路径
                    changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
                    print(f"[get_changes_worker_queue] 交易日变化: {last_date} -> {current_date}")
                    print(f"[get_changes_worker_queue] 新的changes文件路径: {changes_path}")
                    last_date = current_date
                    # 清空主DataFrame，开始新的交易日
                    master_df = pd.DataFrame()
                    last_write = time.time()
                    # 清空涨停板缓存
                    uplimit_cache = None
                    last_uplimit_update = 0
                    print(f"[get_changes_worker_queue] 已清空数据缓存，准备收集新交易日数据")
                else:
                    print(f"[get_changes_worker_queue] 交易日未变化: {current_date}")
            else:
                # 使用缓存的交易日
                current_date = last_date

            # 定期更新涨停板数据
            if uplimit_cache is None or (now - last_uplimit_update) >= uplimit_cache_interval:
                try:
                    print(f"[get_changes_worker_queue] 开始获取涨停板数据，交易日: {current_date}")
                    uplimit_df = uplimit10jqka(current_date)
                    print(f"[get_changes_worker_queue] 获取到涨停板数据，记录数: {len(uplimit_df)}")
                    # 创建股票名称到high_days的映射
                    uplimit_cache = dict(zip(uplimit_df['name'].astype(str), uplimit_df['high_days']))
                    last_uplimit_update = now
                    print(f"[get_changes_worker_queue] 涨停板数据缓存更新完成，映射数量: {len(uplimit_cache)}")
                except Exception as e:
                    print(f"[get_changes_worker_queue] 获取涨停板数据失败: {e}")
                    if uplimit_cache is None:
                        uplimit_cache = {}

            # 检查是否为交易日且在交易时间内
            if is_trading_time():
                print("[get_changes_worker_queue] 当前为交易日且在交易时间内，开始获取数据")
                df = getChanges(concept_df)
            else:
                print("[get_changes_worker_queue] 当前非交易日或不在交易时间内，跳过数据获取")
                df = pd.DataFrame()

            # 无论是否在交易时间内，都要推送数据到前端
            if not df.empty:
                # 添加sign列
                if uplimit_cache:
                    df['sign'] = df['名称'].map(lambda x: uplimit_cache.get(x, ''))
                    print(f"[get_changes_worker_queue] 已添加sign列，有涨停板信息的记录数: {len(df[df['sign'] != ''])}")

                # 将新数据追加到主DataFrame
                master_df = pd.concat([master_df, df], ignore_index=True)

                # 为确保每个板块中的每只股票只保留最新的一条记录，
                master_df.drop_duplicates(subset=['时间', '名称'], keep='last', inplace=True, ignore_index=True)

                # 应用与concept_df相同的排序
                master_df = apply_concept_df_sorting(master_df, concept_df)

                # 调试：检查排序后的数据结构
                print(f"[get_changes_worker_queue] 排序后数据列名: {list(master_df.columns)}")
                if not master_df.empty:
                    print(f"[get_changes_worker_queue] 排序后数据示例: {master_df.iloc[0].to_dict()}")

                # 将完整的、更新后的数据推送到队列
                full_data = master_df.where(pd.notnull(master_df), None).to_dict(orient="records")
                queue.put(full_data)
                print(f"[get_changes_worker_queue] 已将更新后的完整数据({len(master_df)}条)推送到队列")
            elif not master_df.empty:
                # 即使没有新数据，也要推送现有的历史数据到前端
                # 应用与concept_df相同的排序
                master_df = apply_concept_df_sorting(master_df, concept_df)
                
                # 将现有的历史数据推送到队列
                full_data = master_df.where(pd.notnull(master_df), None).to_dict(orient="records")
                queue.put(full_data)
                print(f"[get_changes_worker_queue] 已推送现有历史数据({len(master_df)}条)到队列")

            now = time.time()
            if (now - last_write) >= batch_interval:
                if not master_df.empty:
                    try:
                        master_df.to_csv(changes_path, index=False)
                        print(f"[get_changes_worker_queue] 批量写入 {len(master_df)} 条总记录到 {changes_path}")
                        last_write = now
                    except Exception as e:
                        print(f"[get_changes_worker_queue] 批量写入失败: {e}")
                        
        except Exception as e:
            print(f"[get_changes_worker_queue] getChanges错误: {e}")
        time.sleep(interval)
