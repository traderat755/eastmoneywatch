import pandas as pd
from cache_manager import get_current_concept_df, get_current_picked_df


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
            df['标识'] = df['名称'].map(lambda x: uplimit_cache.get(x, ''))


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
            final_columns = ['股票代码', '时间', '名称', '相关信息', '类型', '四舍五入取整', '上下午', '标识']
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

        # 检查合并所需的列
        available_columns = ['股票代码'] + [col for col in ['板块名称', '板块代码'] if col in concept_df.columns]

        if len(available_columns) < 2:  # 至少需要股票代码和一个板块信息列
            print(f"[add_concept_data_to_changes] concept_df缺少必要的列，可用列: {available_columns}")
            return df

        # 合并概念信息到股票异动数据
        first_concept_df = concept_df.drop_duplicates(subset=['股票代码'], keep='first').copy()
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

        # 2. 其他concept_df的板块在最后
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