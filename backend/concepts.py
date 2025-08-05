import time as t
import pandas as pd
import akshare as ak
import os
from typing import List, List
from utils import setup_static_directory
import requests


def getConcepts() -> None:
    """
    Fetch concept stock data and save to a CSV file.

    This function retrieves stock concepts and their constituent stocks from akshare,
    filters them by market cap, and saves the results to the appropriate static directory.
    """
    print("[getConcepts] 开始获取概念股数据")

    # 设置静态目录
    static_dir = setup_static_directory()
    csv_path = os.path.join(static_dir, "concepts.csv")
    print(f"[getConcepts] CSV文件将保存到: {csv_path}")

    concepts: List[List[str]] = [['板块代码', '板块名称', '股票代码', '股票名称']]
    print("[getConcepts] 开始获取板块概念数据")
    stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
    print(f"[getConcepts] 获取到 {len(stock_board_concept_name_em_df)} 个板块概念")

    stock_board_concept_name_em_df.sort_values(by='总市值', ascending=True, inplace=True)

    for idx, v in stock_board_concept_name_em_df.iterrows():
        print(f"[getConcepts] 处理板块: {v['板块名称']} (市值: {v['总市值']})")

        if int(v['总市值']) > 30000000000000 or '昨日' in v['板块名称']:
            print(f"[getConcepts] 跳过板块: {v['板块名称']} (市值过大或包含'昨日')")
            continue

        stock_board_concept_spot_em_df = ak.stock_board_concept_cons_em(symbol=v['板块代码'])
        print(f"[getConcepts] 板块 {v['板块名称']} 包含 {len(stock_board_concept_spot_em_df)} 只股票")

        for _, v2 in stock_board_concept_spot_em_df.iterrows():
            row = [v['板块代码'], v['板块名称'], v2['代码'], v2['名称']]
            concepts.append(row)

        print(f"[getConcepts] 当前总记录数: {len(concepts)}")
        if len(concepts) % 20 == 0:
            print("[getConcepts] 暂停15秒避免请求过于频繁")
            t.sleep(15)

    print(f"[getConcepts] 数据获取完成，总记录数: {len(concepts)}")
    df = pd.DataFrame(concepts, columns=['板块代码', '板块名称', '股票代码', '股票名称'])
    df.to_csv(csv_path, index=False)
    print(f"[getConcepts] 数据已保存到: {csv_path}")
