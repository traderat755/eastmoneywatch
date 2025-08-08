import time as t
import pandas as pd
import akshare as ak
import os
from typing import List, List
from utils import setup_static_directory
import logging


def getConcepts() -> None:
    """
    Fetch concept stock data and save to a CSV file.

    This function retrieves stock concepts and their constituent stocks from akshare,
    filters them by market cap, and saves the results to the appropriate static directory.
    """
    logging.debug("[getConcepts] 开始获取概念股数据")

    # 设置静态目录
    static_dir = setup_static_directory()
    csv_path = os.path.join(static_dir, "concepts.csv")
    logging.debug(f"[getConcepts] CSV文件将保存到: {csv_path}")

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
    concepts_csv_path = os.path.join(static_dir, "concepts.csv")
    concept_stocks_csv_path = os.path.join(static_dir, "concept_stocks.csv")
    df_concepts.to_csv(concepts_csv_path, index=False)
    logging.debug(f"[getConcepts] 概念数据已保存到: {concepts_csv_path}")
    df_concept_stocks.to_csv(concept_stocks_csv_path, index=False)
    logging.debug(f"[getConcepts] 概念成分股数据已保存到: {concept_stocks_csv_path}")
