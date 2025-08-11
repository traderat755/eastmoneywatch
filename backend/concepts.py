import time as t
import pandas as pd
import akshare as ak
import os
from typing import List
import logging
import time

def getConcepts(concepts_path=None, concept_stocks_path=None) -> None:
    """
    Fetch concept stock data and save to a CSV file.

    This function retrieves stock concepts and their constituent stocks from akshare,
    filters them by market cap, and saves the results to the appropriate static directory.
    
    Args:
        concepts_path: Path to save concepts.csv. If None, will use default location.
        concept_stocks_path: Path to save concept_stocks.csv. If None, will use default location.
    """
    logging.debug("[getConcepts] 开始获取概念股数据")

    # If paths are not provided, calculate default paths
    if not concepts_path or not concept_stocks_path:
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        static_dir = os.path.join(backend_dir, "static")
        os.makedirs(static_dir, exist_ok=True)
        concepts_path = os.path.join(static_dir, "concepts.csv") if not concepts_path else concepts_path
        concept_stocks_path = os.path.join(static_dir, "concept_stocks.csv") if not concept_stocks_path else concept_stocks_path
    
    logging.debug(f"[getConcepts] 概念数据将保存到: {concepts_path}")
    logging.debug(f"[getConcepts] 概念成分股数据将保存到: {concept_stocks_path}")

    concepts: List[List[str]] = [['板块代码', '板块名称']]
    concept_stocks: List[List[str]] = [['板块代码', '股票代码']]
    logging.debug("[getConcepts] 开始获取板块概念数据")
    stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
    logging.debug(f"[getConcepts] 获取到 {len(stock_board_concept_name_em_df)} 个板块概念")

    stock_board_concept_name_em_df.sort_values(by='总市值', ascending=True, inplace=True)

    for idx, v in stock_board_concept_name_em_df.iterrows():
        logging.debug(f"[getConcepts] 处理板块: {v['板块名称']} (市值: {v['总市值']})")

        if int(v['总市值']) > 30000000000000 or '昨日' in v['板块名称']:
            logging.debug(f"[getConcepts] 跳过板块: {v['板块名称']} (市值过大或包含'昨日')")
            continue

        stock_board_concept_spot_em_df = ak.stock_board_concept_cons_em(symbol=v['板块代码'])
        if len(stock_board_concept_spot_em_df)>99:
            continue
        logging.debug(f"[getConcepts] 板块 {v['板块名称']} 包含 {len(stock_board_concept_spot_em_df)} 只股票")

        # 只存板块代码和板块名称
        concepts.append([v['板块代码'], v['板块名称']])
        for _, v2 in stock_board_concept_spot_em_df.iterrows():
            concept_stocks.append([v['板块代码'], v2['代码']])
        logging.debug(f"[getConcepts] 当前concepts记录数: {len(concepts)}，concept_stocks记录数: {len(concept_stocks)}")
        if len(concepts) % 20 == 0:
            logging.debug("[getConcepts] 暂停15秒避免请求过于频繁")
            t.sleep(15)

    logging.debug(f"[getConcepts] 数据获取完成，concepts总记录数: {len(concepts)}，concept_stocks总记录数: {len(concept_stocks)}")
    df_concepts = pd.DataFrame(concepts, columns=['板块代码', '板块名称']).drop_duplicates()
    df_concept_stocks = pd.DataFrame(concept_stocks, columns=['板块代码', '股票代码']).drop_duplicates()
    
    # Save files and verify they were written
    df_concepts.to_csv(concepts_path, index=False)
    
    # Ensure the file is properly closed and flushed
    if os.path.exists(concepts_path):
        # Get file size before closing
        concepts_size = os.path.getsize(concepts_path)
        logging.debug(f"[getConcepts] 概念数据已保存到: {concepts_path}")
        logging.debug(f"[getConcepts] 文件大小: {concepts_size} bytes")
    else:
        concepts_size = 0
        logging.error("[getConcepts] 未能成功保存概念数据文件!")
        
    # Flush and close the file
    if 'df_concepts' in locals():
        del df_concepts
    if 'df_concept_stocks' in locals():
        del df_concept_stocks
        
    # Force garbage collection to ensure files are closed
    import gc
    gc.collect()
    
    # Add a small delay to ensure file handles are released
    time.sleep(1)
    
    df_concept_stocks.to_csv(concept_stocks_path, index=False)
    
    if os.path.exists(concept_stocks_path):
        stocks_size = os.path.getsize(concept_stocks_path)
        logging.debug(f"[getConcepts] 概念成分股数据已保存到: {concept_stocks_path}")
        logging.debug(f"[getConcepts] 文件大小: {stocks_size} bytes")
    else:
        stocks_size = 0
        logging.error("[getConcepts] 未能成功保存概念成分股数据文件!")
        
    # Final verification
    logging.debug(f"[getConcepts] 验证文件: concepts_size={concepts_size}, stocks_size={stocks_size}")
    if concepts_size > 0 and stocks_size > 0:
        logging.debug("[getConcepts] 文件写入验证成功")
    else:
        logging.error("[getConcepts] 文件写入验证失败")