import re
import json
import pandas as pd
import requests
from typing import Optional
import logging


def filter_stock_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    过滤股票数据，根据涨跌幅、类型和股票名称进行筛选

    Args:
        df: 包含股票数据的DataFrame，需要包含'涨跌幅'、'类型'和'股票名称'列

    Returns:
        过滤后的DataFrame，如果输入为None或空则返回None
    """
    if df is None or df.empty:
        return None

    # 过滤涨跌幅条件：涨幅<100% 且 (涨幅≥5% 或 跌幅≤-5%)
    df = df.copy()
    df = df[(df['涨跌幅'] < 1) & ((df['涨跌幅'] >= 0.05) | (df['涨跌幅'] <= -0.05))]

    # 过滤负面类型
    negative_types = ['8194', '8', '128', '8208', '8210', '8212', '8214', '8216', '8203', '99', '106']
    df = df[~df['类型'].astype(str).isin(negative_types)]

    # 过滤ST股票
    df = df[~df['股票名称'].str.contains('ST')]

    return df



HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6,ja;q=0.5',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'script',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-site',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
}

def parse_jsonp(jsonp_str):
    if not jsonp_str or not isinstance(jsonp_str, str):
        logging.debug("错误：输入不是有效的字符串")
        return None
    match = re.match(r'^[a-zA-Z0-9_]+\s*\(\s*(.*)\s*\)\s*;?\s*$', jsonp_str.strip(), re.DOTALL)
    if match:
        json_str = match.group(1)
        data = json.loads(json_str)
        return data
    else:
        return json.loads(jsonp_str)


def getChanges():
    """获取股票异动数据，不包含概念板块数据的拼接"""

    response = requests.get(
        'https://push2ex.eastmoney.com/getAllStockChanges?type=8201,8202,8193,4,32,64,8207,8209,8211,8213,8215,8204,8203,8194,8,16,128,8208,8210,8212,8214,8216&cb=jQuery35108409427522251944_1753773534498&ut=7eea3edcaed734bea9cbfc24409ed989&pageindex=0&pagesize=1000&dpt=wzchanges&_=1753773534514',
        headers={**HEADERS, 'Referer': 'https://quote.eastmoney.com/changes/'},
    )

    # 解析JSONP响应
    data = parse_jsonp(response.text)

    if data and 'data' in data and 'allstock' in data['data']:
        # 转换为DataFrame
        df = pd.DataFrame(data['data']['allstock'])

        # 重命名列名，使其更易读
        column_mapping = {
            'c': '股票代码',
            'n': '股票名称',
            'tm': '时间',
            'm': '市场',
            't': '类型',
            'i': '信息'
        }
        df = df.rename(columns=column_mapping)

        # 解析info字段（包含涨跌幅、最新价、涨跌额）
        if '信息' in df.columns:
            info_df = df['信息'].str.split(',', expand=True)
            if len(info_df.columns) >= 3:
                # 清理并转换数据
                info_df[0] = pd.to_numeric(info_df[0], errors='coerce')

                if not df.empty:
                    df['涨跌幅'] = info_df[0]

        df = filter_stock_data(df)

        # 转换为指定的输出格式
        def format_time(tm):
            """将时间戳格式化为 HH:MM"""
            tm_str = str(tm)
            if len(tm_str) < 6:
                tm_str = tm_str.zfill(6)
            return f"{tm_str[:2]}:{tm_str[2:4]}"  # 只返回小时和分钟

        # 创建新的DataFrame用于输出
        output_df = pd.DataFrame()
        output_df['股票代码'] = df['股票代码']
        output_df['时间'] = df['时间'].apply(format_time)
        output_df['名称'] = df['股票名称']
        # 直接放原始类型和涨跌幅，便于调试
        output_df['原始类型'] = df['类型']
        output_df['原始涨跌幅'] = df['涨跌幅']

        # 类型映射字典
        type_mapping = {
            '8201': '火箭发射',
            '8202': '快速反弹',
            '8193': '大笔买入',
            '4': '封涨停板',
            '32': '打开跌停板',
            '64': '有大买盘',
            '8207': '竞价上涨',
            '8209': '高开5日线',
            '8211': '向上缺口',
            '8213': '60日新高',
            '8215': '60日大幅上涨',
            '8204': '加速下跌',
            '8203': '高台跳水',
            '8194': '大笔卖出',
            '8': '封跌停板',
            '16': '打开涨停板',
            '128': '有大卖盘',
            '8208': '竞价下跌',
            '8210': '低开5日线',
            '8212': '向下缺口',
            '8214': '60日新低',
            '8216': '60日大幅下跌'
        }

        # 增加映射后的类型和相关信息，便于对比
        logging.debug(f"[getChanges] 原始类型值: {df['类型'].unique().tolist()}")
        mapped_types = df['类型'].astype(str).map(type_mapping)
        logging.debug(f"[getChanges] 映射后类型值: {mapped_types.unique().tolist()}")

        # 检查未映射的类型
        unmapped_types = df[~df['类型'].astype(str).isin(type_mapping.keys())]['类型'].unique()
        if len(unmapped_types) > 0:
            logging.debug(f"[getChanges] 发现未映射的类型: {unmapped_types.tolist()}")

        output_df['类型'] = mapped_types.fillna('未知类型').infer_objects(copy=False)
        logging.debug(f"[getChanges] 最终类型值: {output_df['类型'].unique().tolist()}")

        # 先加一列'四舍五入取整'，在加百分号前处理
        output_df['四舍五入取整'] = df['涨跌幅'].apply(lambda x: int(round(x * 100)) if pd.notnull(x) else None)
        output_df['相关信息'] = df['涨跌幅'].apply(
            lambda x: f"%+.2f" % (x * 100) + "%" if pd.notnull(x) else 'NaN'
        )

        # 创建基础输出DataFrame，只包含原始股票异动数据
        html_df = output_df[['股票代码', '时间', '名称', '相关信息', '类型', '四舍五入取整']].copy()

        # 确保股票代码不为空
        logging.debug(f"[getChanges] 处理后包含股票代码的记录数: {html_df['股票代码'].notna().sum()}/{len(html_df)}")
        if html_df['股票代码'].isna().any():
            logging.debug(f"[getChanges] 警告：发现股票代码为空的记录，将移除这些记录")
            html_df = html_df.dropna(subset=['股票代码'])

        # 添加上下午字段
        def am_pm_col(tm):
            hour = int(tm[:2])
            return '上午' if hour < 12 else '下午'
        html_df['上下午'] = html_df['时间'].apply(am_pm_col)

        # 只在内存处理和去重，改为按"名称+类型"去重
        html_df = html_df.drop_duplicates(subset=['股票代码', '类型'], keep='last')
        html_df = html_df.drop_duplicates(subset=['股票代码', '时间'], keep='last')

        return html_df



def getRisingConcepts():

    url = "https://79.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "200",
        "po": "1",  # 按涨跌幅排序，1为降序
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",  # 按涨跌幅排序
        "fs": "m:90 t:3 f:!50",
        "fields": "f3,f12,f14,f20",
        "_": "1626075887768",
    }
    response=requests.get(url=url,params=params,headers={**HEADERS, 'Referer': 'https://quote.eastmoney.com/center/gridlist.html'})
    data = parse_jsonp(response.text)['data']['diff']
    bkcodes = [ x['f12'] for x in data if int(x['f20'])<5000000000000 and not '昨日' in x['f14']]
    return bkcodes





