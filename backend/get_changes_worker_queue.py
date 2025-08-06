import pandas as pd
import time
import os
import akshare as ak
from fluctuation import getChanges, getRisingConcepts
from utils import setup_static_directory, uplimit10jqka,get_latest_trade_date, is_trading_time
from datetime import datetime


# 缓存concept_df和picked_df，避免频繁读取
_concept_df_cache = None
_picked_df_cache = None

def get_current_concept_df():
    """获取当前缓存中的concept_df"""
    global _concept_df_cache
    return _concept_df_cache

def update_concept_df_cache(concept_df):
    """更新concept_df缓存"""
    global _concept_df_cache
    _concept_df_cache = concept_df
    print(f"[get_changes_worker_queue] 更新concept_df缓存，记录数: {len(concept_df) if concept_df is not None else 0}")

def get_current_picked_df():
    """获取当前缓存中的picked_df"""
    global _picked_df_cache
    return _picked_df_cache

def update_picked_df_cache(picked_df):
    """更新picked_df缓存"""
    global _picked_df_cache
    _picked_df_cache = picked_df
    print(f"[get_changes_worker_queue] 更新picked_df缓存，记录数: {len(picked_df) if picked_df is not None else 0}")


def apply_sorting(df, uplimit_cache=None, for_frontend=True):
    """统一的排序函数：添加概念板块信息并按板块排序（picked_df最前，rising concepts次之，其他concept_df最后）
    
    Args:
        df: 输入数据
        uplimit_cache: 涨停板缓存
        for_frontend: 是否为前端格式化（只返回指定列），False时返回完整数据用于存储
    """
    if df.empty:
        return df

    try:
        # 获取concept_df和picked_df
        concept_df = get_current_concept_df()
        picked_df = get_current_picked_df()
        
        if concept_df is None or concept_df.empty:
            print("[apply_sorting] concept_df缓存为空，这不应该发生")
            return df
            
        # 1. 为股票异动数据添加概念板块信息
        df = add_concept_data_to_changes(df, concept_df)
        
        # 2. 构建板块排序顺序：picked_df -> rising concepts -> 其他concept_df
        board_order = build_board_order(concept_df, picked_df)
        
        # 3. 应用板块排序
        df = sort_by_board_order(df, board_order)
        
        # 4. 添加上下午字段和uplimit信息
        if '时间' in df.columns:
            df['上下午'] = df['时间'].apply(lambda tm: '上午' if int(tm[:2]) < 12 else '下午')
            
        if uplimit_cache:
            df['sign'] = df['名称'].map(lambda x: uplimit_cache.get(x, ''))
        else:
            df['sign'] = ''
            
        # 5. 返回数据
        if for_frontend:
            # 为前端返回指定格式的DataFrame，处理可能的重复列名
            sector_code_col = None
            sector_name_col = None
            
            # 寻找正确的板块代码和名称列
            for col in ['板块代码', '板块代码_x', '板块代码_y']:
                if col in df.columns:
                    sector_code_col = col
                    break
            
            for col in ['板块名称', '板块名称_x', '板块名称_y']:
                if col in df.columns:
                    sector_name_col = col
                    break
            
            # 构建最终列名列表，去掉板块代码列以减少数据传输量
            final_columns = ['股票代码', '时间', '名称', '相关信息', '类型', '四舍五入取整', '上下午', 'sign']
            # 只添加板块名称，不添加板块代码
            if sector_name_col:
                final_columns.insert(1, sector_name_col)
            
            available_columns = [col for col in final_columns if col in df.columns]
            result_df = df[available_columns].copy()
            
            # 如果使用了带后缀的列名，重命名为标准名称
            if sector_name_col and sector_name_col != '板块名称':
                result_df = result_df.rename(columns={sector_name_col: '板块名称'})
            
            print(f"[apply_sorting] 处理完成，返回{len(result_df)}条记录给前端，列: {list(result_df.columns)}")
        else:
            # 为存储返回完整数据，确保包含股票代码
            result_df = df.copy()
            # 验证股票代码列是否存在
            if '股票代码' not in result_df.columns:
                print(f"[apply_sorting] 警告：存储数据中缺少股票代码列，当前列: {list(result_df.columns)}")
            else:
                print(f"[apply_sorting] 股票代码列验证通过，非空数量: {result_df['股票代码'].notna().sum()}/{len(result_df)}")
            print(f"[apply_sorting] 处理完成，返回{len(result_df)}条完整记录用于存储，列: {list(result_df.columns)}")
        
        return result_df

    except Exception as e:
        print(f"[apply_sorting] 排序时出错: {e}")
        return df


def add_concept_data_to_changes(df, concept_df):
    """为股票异动数据添加概念板块信息"""
    try:
        # 检查必要的列是否存在
        if '股票代码' not in df.columns:
            print("[add_concept_data_to_changes] df中缺少'股票代码'列，跳过概念数据添加")
            return df
            
        if concept_df is None or concept_df.empty:
            print("[add_concept_data_to_changes] concept_df为空，跳过概念数据添加")
            return df
            
        if '股票代码' not in concept_df.columns:
            print("[add_concept_data_to_changes] concept_df中缺少'股票代码'列，跳过概念数据添加")
            return df
            
        # 获取上涨概念板块代码并排序concept_df
        risingConceptsCodes = getRisingConcepts()
        
        # 排序concept_df：rising concepts在前
        try:
            if '板块代码' in concept_df.columns:
                matching_codes = [code for code in risingConceptsCodes if code in concept_df['板块代码'].values]
                if matching_codes:
                    ordered_df = concept_df[concept_df['板块代码'].isin(matching_codes)].set_index('板块代码').loc[matching_codes].reset_index()
                else:
                    ordered_df = pd.DataFrame()
                    
                rest_df = concept_df[~concept_df['板块代码'].isin(risingConceptsCodes)]
                changedConcepts_df = pd.concat([ordered_df, rest_df], ignore_index=True)
            else:
                print("[add_concept_data_to_changes] concept_df中缺少'板块代码'列，使用原始顺序")
                changedConcepts_df = concept_df
        except Exception as e:
            print(f"[add_concept_data_to_changes] 排序concept_df时出错，使用原始顺序: {e}")
            changedConcepts_df = concept_df

        # 检查合并所需的列
        available_columns = ['股票代码'] + [col for col in ['板块名称', '板块代码'] if col in changedConcepts_df.columns]
        
        if len(available_columns) < 2:  # 至少需要股票代码和一个板块信息列
            print(f"[add_concept_data_to_changes] concept_df缺少必要的列，可用列: {available_columns}")
            return df

        # 合并概念信息到股票异动数据
        first_concept_df = changedConcepts_df.drop_duplicates(subset=['股票代码'], keep='first').copy()
        df['股票代码'] = df['股票代码'].astype(str)
        first_concept_df['股票代码'] = first_concept_df['股票代码'].astype(str)

        # 保存原始数据，确保股票代码不丢失
        original_stock_codes = df['股票代码'].copy()
        
        # 只合并概念相关的列，排除股票代码避免重复
        merge_columns = [col for col in available_columns if col != '股票代码']
        if merge_columns:
            # 检查是否有重复列名，如果有则先删除df中的同名列
            overlapping_cols = [col for col in merge_columns if col in df.columns]
            if overlapping_cols:
                print(f"[add_concept_data_to_changes] 发现重复列，将删除df中的: {overlapping_cols}")
                df = df.drop(columns=overlapping_cols)
            
            df = pd.merge(df, first_concept_df[['股票代码'] + merge_columns], on='股票代码', how='left')
        
        # 确保股票代码列始终存在且不为空
        if '股票代码' not in df.columns or df['股票代码'].isna().any():
            if '股票代码' in df.columns:
                missing_codes_mask = df['股票代码'].isna()
                df.loc[missing_codes_mask, '股票代码'] = original_stock_codes[missing_codes_mask]
            else:
                df['股票代码'] = original_stock_codes
                
        print(f"[add_concept_data_to_changes] 合并完成，最终列: {list(df.columns)}")
        print(f"[add_concept_data_to_changes] 股票代码列非空数量: {df['股票代码'].notna().sum()}/{len(df)}")
            
        return df
        
    except Exception as e:
        print(f"[add_concept_data_to_changes] 添加概念板块信息时出错: {e}")
        import traceback
        print(f"[add_concept_data_to_changes] 详细错误信息: {traceback.format_exc()}")
        return df


def build_board_order(concept_df, picked_df):
    """构建板块排序顺序：picked_df最前，rising concepts次之，其他concept_df最后"""
    board_order = []
    
    try:
        # 1. picked_df的板块在最前面
        if picked_df is not None and not picked_df.empty:
            picked_sector_codes = picked_df['板块代码'].unique().tolist()
            board_order.extend(picked_sector_codes)
            print(f"[build_board_order] 添加picked板块: {picked_sector_codes}")
        
        # 2. rising concepts在中间
        risingConceptsCodes = getRisingConcepts()
        rising_codes = [code for code in risingConceptsCodes if code not in board_order]
        board_order.extend(rising_codes)
        print(f"[build_board_order] 添加rising concepts: {rising_codes[:5]}...")  # 只打印前5个
        
        # 3. 其他concept_df的板块在最后
        all_concept_codes = concept_df['板块代码'].unique().tolist()
        remaining_codes = [code for code in all_concept_codes if code not in board_order]
        board_order.extend(remaining_codes)
        print(f"[build_board_order] 添加剩余板块: {len(remaining_codes)}个")
        
    except Exception as e:
        print(f"[build_board_order] 构建板块顺序时出错: {e}")
        # 出错时使用concept_df的原始顺序
        board_order = concept_df['板块代码'].unique().tolist()
    
    return board_order


def sort_by_board_order(df, board_order):
    """按照板块顺序对DataFrame进行排序"""
    try:
        # 寻找板块代码列，支持多种可能的列名
        sector_col = None
        possible_cols = ['板块代码', '板块代码_x', '板块代码_y']
        
        for col in possible_cols:
            if col in df.columns:
                sector_col = col
                break
        
        if sector_col is None:
            print(f"[sort_by_board_order] 缺少板块代码列，无法排序。可用列: {list(df.columns)}")
            return df
            
        print(f"[sort_by_board_order] 使用板块代码列: {sector_col}")
        
        # 创建板块排序映射
        sector_order_map = {sector: idx for idx, sector in enumerate(board_order)}
        df['_board_order'] = df[sector_col].map(lambda x: sector_order_map.get(x, len(board_order)))
        
        # 按板块顺序排序
        df = df.sort_values('_board_order').drop('_board_order', axis=1)
        
        return df
        
    except Exception as e:
        print(f"[sort_by_board_order] 按板块排序时出错: {e}")
        return df


def worker(queue, interval=3, initial_concept_df=None, initial_picked_df=None, batch_interval=300):
    print("[get_changes_worker_queue] 启动，推送到主进程Queue并定时批量写入磁盘")

    # 设置静态目录
    static_dir = setup_static_directory()

    # 初始化概念数据缓存，使用传递的初始数据
    if initial_concept_df is not None and not initial_concept_df.empty:
        update_concept_df_cache(initial_concept_df)
        print(f"[worker] 使用传递的concept_df初始化缓存，记录数: {len(initial_concept_df)}")
    else:
        print("[worker] 未传递concept_df，缓存可能为空")
        
    if initial_picked_df is not None and not initial_picked_df.empty:
        update_picked_df_cache(initial_picked_df)
        print(f"[worker] 使用传递的picked_df初始化缓存，记录数: {len(initial_picked_df)}")
    else:
        print("[worker] 未传递picked_df或picked_df为空")

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

            # 应用与concept_df相同的排序（使用当前最新的concept_df）
            if not master_df.empty:
                master_df = apply_sorting(master_df, for_frontend=False)
                print(f"[get_changes_worker_queue] 已对现有数据应用排序")
        except Exception as e:
            print(f"[get_changes_worker_queue] 读取当日changes.csv失败: {e}")
            master_df = pd.DataFrame()
    else:
        print(f"[get_changes_worker_queue] changes文件不存在: {changes_path}")
        master_df = pd.DataFrame()

    # 启动时，将现有数据推送到队列
    if not master_df.empty:
        # 为前端格式化数据
        frontend_df = apply_sorting(master_df, for_frontend=True)
        # Replace NaN values with 0 before converting to dict
        frontend_df = frontend_df.fillna(0)  # Replace NaN with 0 for numeric fields
        initial_data = {
            "columns": list(frontend_df.columns),
            "values": frontend_df.values.tolist()
        }
        queue.put(initial_data)
        print(f"[get_changes_worker_queue] 已将当日现有数据推送到队列")
    else:
        print(f"[get_changes_worker_queue] 现有数据为空，不推送到队列")


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
                    uplimit_cache = {}
                    print(f"[get_changes_worker_queue] 已清空数据缓存，准备收集新交易日数据")
                else:
                    print(f"[get_changes_worker_queue] 交易日未变化: {current_date}")
            else:
                # 使用缓存的交易日
                current_date = last_date


            # 检查是否为交易日且在交易时间内
            if is_trading_time():
                print("[get_changes_worker_queue] 当前为交易日且在交易时间内，开始获取数据")

                # 获取基础股票异动数据（不包含概念板块信息）
                df = getChanges()
                print(f"[get_changes_worker_queue] getChanges返回数据列名: {list(df.columns) if not df.empty else '空DataFrame'}")
                if not df.empty:
                    print(f"[get_changes_worker_queue] getChanges返回数据类型值: {df['类型'].unique().tolist()}")
            else:
                print("[get_changes_worker_queue] 当前非交易日或不在交易时间内，跳过数据获取")
                df = pd.DataFrame()

            # 无论是否在交易时间内，都要推送数据到前端
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
                                print(f"[get_changes_worker_queue] 发现新的封涨停板股票: {list(new_stocks)}")
                        else:
                            # master_df为空或无类型列，说明都是新的
                            has_new_limit_up = True
                            print(f"[get_changes_worker_queue] 发现封涨停板股票: {new_limit_up_stocks}")

                # 只有发现新的封涨停板时才更新涨停板数据
                if has_new_limit_up:
                    try:
                        print(f"[get_changes_worker_queue] 发现新的封涨停板，开始获取涨停板数据，交易日: {current_date}")
                        uplimit_df = uplimit10jqka(current_date)
                        print(f"[get_changes_worker_queue] 获取到涨停板数据，记录数: {len(uplimit_df)}")
                        # 创建股票名称到high_days的映射
                        uplimit_cache = dict(zip(uplimit_df['name'].astype(str), uplimit_df['high_days']))
                        print(f"[get_changes_worker_queue] 涨停板数据缓存更新完成，映射数量: {len(uplimit_cache)}")
                    except Exception as e:
                        print(f"[get_changes_worker_queue] 获取涨停板数据失败: {e}")

                master_df = master_df[master_df['四舍五入取整'] != 0]
                # 将新数据追加到主DataFrame
                master_df = pd.concat([master_df, df], ignore_index=True)

                # 为确保每个板块中的每只股票只保留最新的一条记录，
                master_df.drop_duplicates(subset=['时间', '名称'], keep='last', inplace=True, ignore_index=True)

                # 应用统一排序，保留完整数据用于存储
                master_df = apply_sorting(master_df, uplimit_cache, for_frontend=False)

                # 调试：检查排序后的数据结构
                print(f"[get_changes_worker_queue] 排序后完整数据列名: {list(master_df.columns)}")
                if not master_df.empty:
                    print(f"[get_changes_worker_queue] 排序后数据示例: {master_df.iloc[0].to_dict()}")

                # 为前端准备格式化数据
                frontend_df = apply_sorting(master_df, uplimit_cache, for_frontend=True)
                # Replace NaN values with 0 or null before converting to dict
                frontend_df = frontend_df.fillna(0)  # Replace NaN with 0 for numeric fields
                full_data = {
                    "columns": list(frontend_df.columns),
                    "values": frontend_df.values.tolist()
                }
                queue.put(full_data)
                print(f"[get_changes_worker_queue] 已将格式化数据({len(frontend_df)}条)推送到队列")
            elif not master_df.empty:
                # 即使没有新数据，也要检查是否需要重新排序（picked.csv可能已更新）
                master_df = apply_sorting(master_df, uplimit_cache, for_frontend=False)

                # 强制推送数据，确保前端获得最新排序
                frontend_df = apply_sorting(master_df, uplimit_cache, for_frontend=True)
                # Replace NaN values with 0 or null before converting to dict
                frontend_df = frontend_df.fillna(0)  # Replace NaN with 0 for numeric fields
                full_data = {
                    "columns": list(frontend_df.columns),
                    "values": frontend_df.values.tolist()
                }
                queue.put(full_data)
                print(f"[get_changes_worker_queue] 已推送现有历史数据({len(frontend_df)}条)到队列，检查排序更新")

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
