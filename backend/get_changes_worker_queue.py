import pandas as pd
import time
import os
from datetime import datetime
from fluctuation import getChanges
from utils import setup_static_directory, uplimit10jqka


def worker(concept_df, queue, interval=3, batch_interval=300):
    print("[get_changes_worker_queue] 启动，推送到主进程Queue并定时批量写入磁盘")

    # 设置静态目录
    static_dir = setup_static_directory()

    # 获取当前日期
    current_date = datetime.now().strftime("%Y%m%d")
    changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
    print(f"[get_changes_worker_queue] 当日changes文件路径: {changes_path}")
    print(f"[get_changes_worker_queue] 检查changes文件是否存在: {changes_path} -> {os.path.exists(changes_path)}")

    # 启动时先读取当日的changes.csv文件
    if os.path.exists(changes_path):
        print(f"[get_changes_worker_queue] changes文件存在，准备读取: {changes_path}")
        try:
            master_df = pd.read_csv(changes_path)
            print(f"[get_changes_worker_queue] 读取到当日已存在的changes数据，记录数: {len(master_df)}")
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

    while True:
        try:
            # 检查日期是否变化
            current_date = datetime.now().strftime("%Y%m%d")
            if current_date != last_date:
                # 日期变化，更新文件路径
                changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
                print(f"[get_changes_worker_queue] 日期变化，新的changes文件路径: {changes_path}")
                last_date = current_date
                # 清空主DataFrame，开始新的一天
                master_df = pd.DataFrame()
                last_write = time.time()
                # 清空涨停板缓存
                uplimit_cache = None
                last_uplimit_update = 0

            # 定期更新涨停板数据
            now = time.time()
            if uplimit_cache is None or (now - last_uplimit_update) >= uplimit_cache_interval:
                try:
                    print("[get_changes_worker_queue] 开始获取涨停板数据")
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

            # print(f"[get_changes_worker_queue] 开始获取变更数据，concept_df大小: {len(concept_df) if concept_df is not None else 0}")
            df = getChanges(concept_df)

            if not df.empty:
                # 添加sign列
                if uplimit_cache:
                    df['sign'] = df['名称'].map(lambda x: uplimit_cache.get(x, ''))
                    print(f"[get_changes_worker_queue] 已添加sign列，有涨停板信息的记录数: {len(df[df['sign'] != ''])}")

                # 将新数据追加到主DataFrame
                master_df = pd.concat([master_df, df], ignore_index=True)

                # 为确保每个板块中的每只股票只保留最新的一条记录，
                master_df.drop_duplicates(subset=['时间', '名称'], keep='last', inplace=True, ignore_index=True)

                # 将完整的、更新后的数据推送到队列
                full_data = master_df.where(pd.notnull(master_df), None).to_dict(orient="records")
                queue.put(full_data)
                print(f"[get_changes_worker_queue] 已将更新后的完整数据({len(master_df)}条)推送到队列")

        except Exception as e:
            print(f"[get_changes_worker_queue] getChanges错误: {e}")

        now = time.time()
        if (now - last_write) >= batch_interval:
            if not master_df.empty:
                try:
                    master_df.to_csv(changes_path, index=False)
                    print(f"[get_changes_worker_queue] 批量写入 {len(master_df)} 条总记录到 {changes_path}")
                    last_write = now
                except Exception as e:
                    print(f"[get_changes_worker_queue] 批量写入失败: {e}")
        time.sleep(interval)
