import pandas as pd
import time
import os
from fluctuation import getChanges
from utils import setup_static_directory, uplimit10jqka, get_latest_trade_date, is_trading_time
from datetime import datetime
from cache_manager import update_concept_df_cache, update_picked_df_cache
from data_processor import apply_sorting


def worker(queue, interval=3, initial_concept_df=None, shared_picked_data=None, initial_changes_df=None, batch_interval=300):
    print("[worker_queue] 启动，推送到主进程Queue并定时批量写入磁盘")

    # 设置静态目录
    static_dir = setup_static_directory()

    # 初始化概念数据缓存，使用传递的初始数据
    if initial_concept_df is not None and not initial_concept_df.empty:
        update_concept_df_cache(initial_concept_df)
        print(f"[worker] 使用传递的concept_df初始化缓存，记录数: {len(initial_concept_df)}")
    else:
        print("[worker] 未传递concept_df，缓存可能为空")

    # 初始化picked数据，优先使用共享内存
    if shared_picked_data is not None:
        # 从共享内存构建 DataFrame
        try:
            if 'records' in shared_picked_data and shared_picked_data['records']:
                records = list(shared_picked_data['records'])
                initial_picked_df = pd.DataFrame(records)
                # 确保数据类型正确
                initial_picked_df['股票代码'] = initial_picked_df['股票代码'].astype(str)
                initial_picked_df['板块代码'] = initial_picked_df['板块代码'].astype(str)
                initial_picked_df = initial_picked_df.fillna('')
                update_picked_df_cache(initial_picked_df)
                print(f"[worker] 使用共享内存初始化picked数据，记录数: {len(initial_picked_df)}")
            else:
                print("[worker] 共享内存中picked数据为空")
        except Exception as e:
            print(f"[worker] 从共享内存构建picked数据失败: {e}")
    else:
        print("[worker] 未传递共享内存数据")

    # 获取当前交易日
    current_date = get_latest_trade_date()
    changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
    print(f"[worker_queue] 当日changes文件路径: {changes_path}")

    # 初始化master_df，使用从backend_service传递的初始数据
    if initial_changes_df is not None and not initial_changes_df.empty:
        master_df = initial_changes_df.copy()
        print(f"[worker_queue] 使用传递的changes数据初始化master_df，记录数: {len(master_df)}")

        # 应用排序
        if not master_df.empty:
            master_df = apply_sorting(master_df, for_frontend=False)
            print(f"[worker_queue] 已对传递的数据应用排序")
    else:
        print("[worker_queue] 未传递changes数据，初始化为空DataFrame")
        master_df = pd.DataFrame()

    # 启动时，将现有数据推送到队列
    if not master_df.empty:
        # 为前端格式化数据
        frontend_df = apply_sorting(master_df, for_frontend=True)
        # Replace NaN values with 0 before converting to dict
        frontend_df = frontend_df.fillna('')  # Replace NaN with 0 for numeric fields
        initial_data = {
            "columns": list(frontend_df.columns),
            "values": frontend_df.values.tolist()
        }
        queue.put(initial_data)
        print(f"[worker_queue] 已将初始数据推送到队列")
    else:
        print(f"[worker_queue] 初始数据为空，不推送到队列")


    last_write = time.time()
    last_date = current_date
    # 缓存涨停板数据，避免频繁请求
    uplimit_cache = {}

    # 交易日检测相关变量
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
                    print(f"[worker_queue] 早上9点前后检查交易日变化")

            # 执行交易日检测
            if should_check_trade_date:
                current_date = get_latest_trade_date()
                if current_date != last_date:
                    # 交易日变化，更新文件路径
                    changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
                    print(f"[worker_queue] 交易日变化: {last_date} -> {current_date}")
                    print(f"[worker_queue] 新的changes文件路径: {changes_path}")
                    last_date = current_date
                    # 清空主DataFrame，开始新的交易日
                    master_df = pd.DataFrame()
                    last_write = time.time()
                    # 清空涨停板缓存
                    uplimit_cache = {}
                    print(f"[worker_queue] 已清空数据缓存，准备收集新交易日数据")
                else:
                    print(f"[worker_queue] 交易日未变化: {current_date}")
            else:
                # 使用缓存的交易日
                current_date = last_date


            # 定期同步共享内存中的picked数据到本地缓存（每次循环都检查）
            if shared_picked_data is not None:
                try:
                    if 'records' in shared_picked_data and shared_picked_data['records']:
                        records = list(shared_picked_data['records'])
                        updated_picked_df = pd.DataFrame(records)
                        # 确保数据类型正确
                        updated_picked_df['股票代码'] = updated_picked_df['股票代码'].astype(str)
                        updated_picked_df['板块代码'] = updated_picked_df['板块代码'].astype(str)
                        updated_picked_df = updated_picked_df.fillna('')
                        update_picked_df_cache(updated_picked_df)
                        # print(f"[worker] 已同步共享内存中的picked数据到缓存，记录数: {len(updated_picked_df)}")
                    else:
                        # 如果共享内存为空，清空本地缓存
                        update_picked_df_cache(pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称']))
                        # print("[worker] 共享内存中picked数据为空，已清空本地缓存")
                except Exception as e:
                    print(f"[worker] 同步共享内存picked数据失败: {e}")

            # 检查是否为交易日且在交易时间内
            if is_trading_time():
                print("[worker_queue] 当前为交易日且在交易时间内，开始获取数据")

                # 获取基础股票异动数据（不包含概念板块信息）
                df = getChanges()
                print(f"[worker_queue] getChanges返回数据列名: {list(df.columns) if not df.empty else '空DataFrame'}")
                if not df.empty:
                    print(f"[worker_queue] getChanges返回数据类型值: {df['类型'].unique().tolist()}")
            else:
                print("[worker_queue] 当前非交易日或不在交易时间内，跳过数据获取")
                df = pd.DataFrame()

            # 无论是否在交易时间内，都要推送数据到前端
            # 修改逻辑：如果有新数据则处理新数据，否则推送现有数据
            if not df.empty:
                # 检查是否有新的封涨停板，如果有则更新涨停板数据
                has_new_limit_up = False
                if '类型' in df.columns:
                    new_limit_up_stocks = df[df['类型'] == '封涨停板']['名称'].tolist()
                    if new_limit_up_stocks:
                        # 检查这些封涨停板股票是否在master_df中已存在
                        if not master_df.empty and '类型' in master_df.columns:
                            existing_limit_up_stocks = master_df[master_df['类型'] == '封涨停板']['名称'].tolist()
                            new_stocks = set(new_limit_up_stocks) - set(existing_limit_up_stocks)
                            has_new_limit_up = len(new_stocks) > 0
                            if has_new_limit_up:
                                print(f"[worker_queue] 发现新的封涨停板股票: {list(new_stocks)}")
                        else:
                            # master_df为空或无类型列，说明都是新的
                            has_new_limit_up = True
                            print(f"[worker_queue] 发现封涨停板股票: {new_limit_up_stocks}")

                # 只有发现新的封涨停板时才更新涨停板数据
                if has_new_limit_up:
                    try:
                        print(f"[worker_queue] 发现新的封涨停板，开始获取涨停板数据，交易日: {current_date}")
                        uplimit_df = uplimit10jqka(current_date)
                        print(f"[worker_queue] 获取到涨停板数据，记录数: {len(uplimit_df)}")
                        # 创建股票名称到high_days的映射
                        uplimit_cache = dict(zip(uplimit_df['name'].astype(str), uplimit_df['high_days']))
                        print(f"[worker_queue] 涨停板数据缓存更新完成，映射数量: {len(uplimit_cache)}")
                    except Exception as e:
                        print(f"[worker_queue] 获取涨停板数据失败: {e}")

                master_df = master_df[master_df['四舍五入取整'] != 0]
                # 将新数据追加到主DataFrame
                master_df = pd.concat([master_df, df], ignore_index=True)

                # 为确保每个板块中的每只股票只保留最新的一条记录，
                master_df.drop_duplicates(subset=['类型', '股票代码'], keep='last', inplace=True, ignore_index=True)

                # 应用统一排序，保留完整数据用于存储
                master_df = apply_sorting(master_df, uplimit_cache, for_frontend=False)

                # 调试：检查排序后的数据结构
                print(f"[worker_queue] 排序后完整数据列名: {list(master_df.columns)}")
                if not master_df.empty:
                    print(f"[worker_queue] 排序后数据示例: {master_df.iloc[0].to_dict()}")

                # 为前端准备格式化数据
                frontend_df = apply_sorting(master_df, uplimit_cache, for_frontend=True)
                # Replace NaN values with 0 or null before converting to dict
                frontend_df = frontend_df.fillna('')  # Replace NaN with 0 for numeric fields
                full_data = {
                    "columns": list(frontend_df.columns),
                    "values": frontend_df.values.tolist()
                }
                queue.put(full_data)
                print(f"[worker_queue] 已将格式化数据({len(frontend_df)}条)推送到队列")
            elif not master_df.empty:
                # 非交易时间但有历史数据时，也要推送现有数据到前端
                print(f"[worker_queue] 非交易时间，推送现有数据({len(master_df)}条)到前端")
                # 为前端准备格式化数据
                frontend_df = apply_sorting(master_df, uplimit_cache, for_frontend=True)
                # Replace NaN values with 0 or null before converting to dict
                frontend_df = frontend_df.fillna('')  # Replace NaN with 0 for numeric fields
                full_data = {
                    "columns": list(frontend_df.columns),
                    "values": frontend_df.values.tolist()
                }
                queue.put(full_data)
                print(f"[worker_queue] 已将现有数据({len(frontend_df)}条)推送到队列")

            now = time.time()
            if (now - last_write) >= batch_interval:
                if not master_df.empty:
                    try:
                        master_df.to_csv(changes_path, index=False)
                        print(f"[worker_queue] 批量写入 {len(master_df)} 条总记录到 {changes_path}")
                        last_write = now
                    except Exception as e:
                        print(f"[worker_queue] 批量写入失败: {e}")

        except Exception as e:
            print(f"[worker_queue] getChanges错误: {e}")
        time.sleep(interval)