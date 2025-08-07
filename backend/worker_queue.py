import pandas as pd
import time
import os
from fluctuation import getChanges
from utils import setup_static_directory, uplimit10jqka, get_latest_trade_date, is_trading_time
from datetime import datetime
from data_processor import apply_sorting
from services.pick_service import set_shared_picked_data



def worker(queue, interval=3, initial_concept_df=None, initial_changes_df=None, batch_interval=300, shared_picked_data=None):
    print("[worker_queue] 启动，推送到主进程Queue并定时批量写入磁盘")
    if shared_picked_data is not None:
        set_shared_picked_data(shared_picked_data)
        print(f"[worker_queue] set_shared_picked_data: id={id(shared_picked_data)}, keys={list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)

    # 设置静态目录
    static_dir = setup_static_directory()

    # 获取当前交易日
    current_date = get_latest_trade_date()
    changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
    print(f"[worker_queue] 当日changes文件路径: {changes_path}")

    # 定义标准字段
    standard_columns = [
        '股票代码', '时间', '名称', '相关信息', '类型', '板块名称', '四舍五入取整', '上下午', '时间排序', '标识'
    ]

    # 初始化master_df，使用从backend_service传递的初始数据
    print(f"[worker_queue] initial_changes_df类型: {type(initial_changes_df)}")
    if initial_changes_df is not None:
        # 如果是DataFrame
        if hasattr(initial_changes_df, 'empty'):
            is_empty = initial_changes_df.empty
            # 字段校验
            if not is_empty:
                df_columns = list(initial_changes_df.columns)
                if not all(col in df_columns for col in standard_columns):
                    print(f"[worker_queue] initial_changes_df字段不匹配，收到字段: {df_columns}")
                    print(f"[worker_queue] 内容预览: {initial_changes_df.head(2).to_dict('records') if hasattr(initial_changes_df, 'head') else str(initial_changes_df)[:200]}")
                    initial_changes_df = pd.DataFrame(columns=standard_columns)
                    is_empty = True
        elif isinstance(initial_changes_df, dict) or 'DictProxy' in str(type(initial_changes_df)):
            try:
                dict_data = dict(initial_changes_df)
                print(f"[worker_queue] initial_changes_df转dict内容预览: {str(dict_data)[:200]}")
                if 'records' in dict_data and dict_data['records']:
                    records = list(dict_data['records'])
                    print(f"[worker_queue] records长度: {len(records)}，前1条: {records[:1]}")
                    initial_changes_df = pd.DataFrame(records)
                    # 字段校验
                    df_columns = list(initial_changes_df.columns)
                    if not all(col in df_columns for col in standard_columns):
                        print(f"[worker_queue] initial_changes_df字段不匹配，收到字段: {df_columns}")
                        print(f"[worker_queue] 内容预览: {initial_changes_df.head(2).to_dict('records') if hasattr(initial_changes_df, 'head') else str(initial_changes_df)[:200]}")
                        initial_changes_df = pd.DataFrame(columns=standard_columns)
                        is_empty = True
                    else:
                        is_empty = initial_changes_df.empty
                else:
                    print(f"[worker_queue] dict_data无'records'或records为空，初始化为空DataFrame")
                    initial_changes_df = pd.DataFrame(columns=standard_columns)
                    is_empty = True
            except Exception as e:
                print(f"[worker_queue] initial_changes_df无法转为DataFrame: {e}")
                initial_changes_df = pd.DataFrame(columns=standard_columns)
                is_empty = True
        else:
            print(f"[worker_queue] 未知类型，初始化为空DataFrame")
            initial_changes_df = pd.DataFrame(columns=standard_columns)
            is_empty = True
        if not is_empty:
            master_df = initial_changes_df.copy()
            print(f"[worker_queue] 使用传递的changes数据初始化master_df，记录数: {len(master_df)}")
            # 应用排序
            if not master_df.empty:
                master_df = apply_sorting(master_df,initial_concept_df, for_frontend=False)
                print(f"[worker_queue] 已对传递的数据应用排序")
        else:
            print("[worker_queue] 传递的changes数据为空或字段不匹配，初始化为空DataFrame")
            master_df = pd.DataFrame(columns=standard_columns)
    else:
        print("[worker_queue] 未传递changes数据，初始化为空DataFrame")
        master_df = pd.DataFrame(columns=standard_columns)

    # 启动时，将现有数据推送到队列
    if not master_df.empty:
        print(f"[worker_queue] master_df.dtypes: {master_df.dtypes}")
        # 为前端格式化数据
        frontend_df = apply_sorting(master_df,initial_concept_df, for_frontend=True)
        print(f"[worker_queue] frontend_df.dtypes: {frontend_df.dtypes}")
        # 分别处理字符串和数值列
        for col in frontend_df.columns:
            if frontend_df[col].dtype == 'O':
                frontend_df[col] = frontend_df[col].fillna('')
            else:
                frontend_df[col] = frontend_df[col].fillna(0)
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

        # 检查是否为交易日且在交易时间内
        if is_trading_time():
            print("[worker_queue] 当前为交易日且在交易时间内，开始获取数据")

            # 获取基础股票异动数据（不包含概念板块信息）
            df = getChanges()
            print(f"[worker_queue] getChanges返回数据列名: {list(df.columns) if not df.empty else '空DataFrame'}")
            if not df.empty:
                print(f"[worker_queue] getChanges返回数据类型值: {df['类型'].unique().tolist()}")
                print(f"[worker_queue] getChanges返回数据dtypes: {df.dtypes}")
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
            master_df = apply_sorting(master_df, initial_concept_df, uplimit_cache,for_frontend=False)

            # 调试：检查排序后的数据结构
            print(f"[worker_queue] 排序后完整数据列名: {list(master_df.columns)}")
            if not master_df.empty:
                print(f"[worker_queue] 排序后数据示例: {master_df.iloc[0].to_dict()}")

            # 为前端准备格式化数据
            frontend_df = apply_sorting(master_df, initial_concept_df, uplimit_cache, for_frontend=True)
            print(f"[worker_queue] frontend_df.dtypes: {frontend_df.dtypes}")
            # 分别处理字符串和数值列
            for col in frontend_df.columns:
                if frontend_df[col].dtype == 'O':
                    frontend_df[col] = frontend_df[col].fillna('')
                else:
                    frontend_df[col] = frontend_df[col].fillna(0)
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
            frontend_df = apply_sorting(master_df, initial_concept_df, uplimit_cache, for_frontend=True)
            # 分别处理字符串和数值列
            for col in frontend_df.columns:
                if frontend_df[col].dtype == 'O':
                    frontend_df[col] = frontend_df[col].fillna('')
                else:
                    frontend_df[col] = frontend_df[col].fillna(0)
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


        time.sleep(interval)