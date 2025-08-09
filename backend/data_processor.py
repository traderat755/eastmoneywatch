import pandas as pd
from services.pick_service import get_shared_picked_df
import logging

def apply_sorting(df,concept_df, uplimit_cache=None, for_frontend=True):
    """统一的排序函数：添加概念板块信息并按板块排序（picked_df最前，rising concepts次之，其他concept_df最后）

    Args:
        df: 输入数据
        uplimit_cache: 涨停板缓存
        for_frontend: 是否为前端格式化（只返回指定列），False时返回完整数据用于存储
    """
    if df.empty:
        return df

    picked_df = get_shared_picked_df()

    try:
        if concept_df is None or concept_df.empty:
            logging.debug("[apply_sorting] concept_df缓存为空，这不应该发生")
            return df

        #  为股票异动数据添加概念板块信息（使用过滤后的concept_df）
        df = add_concept_data_to_changes(df, concept_df)
        with_picked_df = add_concept_data_to_changes(df, picked_df)
        logging.debug(f"[apply_sorting] 添加picked_df概念信息后数据量: {len(df)}")

        if len(with_picked_df)>0:
            df = pd.concat([with_picked_df, df], ignore_index=True)
        
        # 按时间+股票代码去重，保留第一次出现的（即picked优先）
        df = df.drop_duplicates(subset=['股票代码','时间'], keep='first').reset_index(drop=True)
        logging.debug(f"[apply_sorting] 去重后df列: {list(df.columns)}")
        logging.debug(f"[apply_sorting] 去重后df前3行数据:\n{df.head(3)}")

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

            # 添加调试日志
            logging.debug(f"[apply_sorting] 当前df的所有列: {list(df.columns)}")

            # 寻找正确的板块代码和名称列
            for col in ['板块代码', '板块代码_x', '板块代码_y']:
                if col in df.columns:
                    sector_code_col = col
                    break

            for col in ['板块名称', '板块名称_x', '板块名称_y']:
                if col in df.columns:
                    sector_name_col = col
                    break

            # 添加调试日志
            logging.debug(f"[apply_sorting] 找到的板块代码列: {sector_code_col}")
            logging.debug(f"[apply_sorting] 找到的板块名称列: {sector_name_col}")

            # 构建最终列名列表，去掉板块代码列以减少数据传输量
            final_columns = ['股票代码', '时间', '名称', '相关信息', '类型', '四舍五入取整', '上下午', '标识']
            # 添加板块名称和板块代码
            if sector_name_col:
                final_columns.insert(1, sector_name_col)
                logging.debug(f"[apply_sorting] 已将板块名称列添加到final_columns: {sector_name_col}")
            else:
                logging.debug(f"[apply_sorting] 未找到板块名称列，不添加到final_columns")
                
            if sector_code_col:
                final_columns.insert(2, sector_code_col)
                logging.debug(f"[apply_sorting] 已将板块代码列添加到final_columns: {sector_code_col}")
            else:
                logging.debug(f"[apply_sorting] 未找到板块代码列，不添加到final_columns")

            available_columns = [col for col in final_columns if col in df.columns]
            logging.debug(f"[apply_sorting] available_columns: {available_columns}")
            result_df = df[available_columns].copy()
            logging.debug(f"[apply_sorting] result_df前3行数据:\n{result_df.head(3)}")

            # 如果使用了带后缀的列名，重命名为标准名称
            if sector_name_col and sector_name_col != '板块名称':
                result_df = result_df.rename(columns={sector_name_col: '板块名称'})
                
            if sector_code_col and sector_code_col != '板块代码':
                result_df = result_df.rename(columns={sector_code_col: '板块代码'})

            logging.debug(f"[apply_sorting] 处理完成，返回{len(result_df)}条记录给前端，列: {list(result_df.columns)}")
            logging.debug(f"[apply_sorting] 最终返回数据前3行:\n{result_df.head(3)}")
        else:
            # 为存储返回完整数据，确保包含股票代码
            result_df = df.copy()
            # 验证股票代码列是否存在
            if '股票代码' not in result_df.columns:
                logging.debug(f"[apply_sorting] 警告：存储数据中缺少股票代码列，当前列: {list(result_df.columns)}")
            else:
                logging.debug(f"[apply_sorting] 股票代码列验证通过，非空数量: {result_df['股票代码'].notna().sum()}/{len(result_df)}")
            logging.debug(f"[apply_sorting] 处理完成，返回{len(result_df)}条完整记录用于存储，列: {list(result_df.columns)}")

        return result_df

    except Exception as e:
        logging.debug(f"[apply_sorting] 排序时出错: {e}")
        return df


def add_concept_data_to_changes(df, concept_df):
    """为股票异动数据添加概念板块信息"""
    try:

        # 检查合并所需的列
        available_columns = ['股票代码'] + [col for col in ['板块名称', '板块代码'] if col in concept_df.columns]
        logging.debug(f"[add_concept_data_to_changes] concept_df列: {list(concept_df.columns)}")
        logging.debug(f"[add_concept_data_to_changes] concept_df前3行数据:\n{concept_df.head(3)}")
        logging.debug(f"[add_concept_data_to_changes] available_columns: {available_columns}")

        # 合并概念信息到股票异动数据
        first_concept_df = concept_df.drop_duplicates(subset=['股票代码'], keep='first').copy()
        logging.debug(f"[add_concept_data_to_changes] first_concept_df前3行数据:\n{first_concept_df.head(3)}")
        df['股票代码'] = df['股票代码'].astype(str)
        first_concept_df['股票代码'] = first_concept_df['股票代码'].astype(str)

        # 只合并概念相关的列，排除股票代码避免重复
        merge_columns = [col for col in available_columns if col != '股票代码']
        logging.debug(f"[add_concept_data_to_changes] merge_columns: {merge_columns}")

        if merge_columns:
            # 检查是否有重复列名，如果有则先删除df中的同名列
            overlapping_cols = [col for col in merge_columns if col in df.columns]
            if overlapping_cols:
                logging.debug(f"[add_concept_data_to_changes] 发现重复列，将删除df中的: {overlapping_cols}")
                df = df.drop(columns=overlapping_cols)

            # 执行合并
            df = pd.merge(df, first_concept_df[['股票代码'] + merge_columns], on='股票代码', how='left')
            logging.debug(f"[add_concept_data_to_changes] 合并后df前3行数据:\n{df.head(3)}")

        # 删除板块名称为空的行（按行删除，不是按列删除）
        if '板块名称' in df.columns:
            before_count = len(df)
            df = df.dropna(subset=['板块名称'], axis=0, inplace=False)
            after_count = len(df)
            logging.debug(f"[add_concept_data_to_changes] 删除板块名称为空的行: {before_count} -> {after_count}")

        logging.debug(f"[add_concept_data_to_changes] 合并完成，最终列: {list(df.columns)}")
        logging.debug(f"[add_concept_data_to_changes] 股票代码列非空数量: {df['股票代码'].notna().sum()}/{len(df)}")
        logging.debug(f"[add_concept_data_to_changes] 合并后df前3行数据:\n{df.head(3)}")

        return df

    except Exception as e:
        logging.debug(f"[add_concept_data_to_changes] 添加概念板块信息时出错: {e}")
        import traceback
        logging.debug(f"[add_concept_data_to_changes] 详细错误信息: {traceback.format_exc()}")
        return df