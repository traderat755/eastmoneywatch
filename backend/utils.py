import os
import sys
import requests
import pandas as pd
from datetime import datetime
import akshare as ak
import logging

def is_trading_time():
    """判断当前是否为交易日且在交易时间内"""
    try:
        # 获取当前北京时间
        now = datetime.now()

        # 判断是否为工作日（周一到周五）
        if now.weekday() >= 5:  # 5=周六, 6=周日
            logging.debug(f"[is_trading_time] 当前为周末，非交易日")
            return False

        # 获取当前时间的小时和分钟
        current_hour = now.hour
        current_minute = now.minute
        current_time_minutes = current_hour * 60 + current_minute

        # 交易时间：上午9:30-11:30，下午13:00-15:00
        morning_start = 9 * 60 + 30  # 9:30
        morning_end = 11 * 60 + 30   # 11:30
        afternoon_start = 13 * 60     # 13:00
        afternoon_end = 15 * 60       # 15:00

        # 判断是否在交易时间内
        is_trading_hours = (
            (current_time_minutes >= morning_start and current_time_minutes < morning_end) or
            (current_time_minutes >= afternoon_start and current_time_minutes < afternoon_end)
        )

        if not is_trading_hours:
            logging.debug(f"[is_trading_time] 当前时间 {current_hour:02d}:{current_minute:02d} 不在交易时间内")
            return False

        # 检查是否为交易日（通过获取最新交易日来判断）
        latest_trade_date = get_latest_trade_date()
        current_date = now.strftime("%Y%m%d")

        if current_date != latest_trade_date:
            logging.debug(f"[is_trading_time] 当前日期 {current_date} 不是最新交易日 {latest_trade_date}")
            return False

        logging.debug(f"[is_trading_time] 当前为交易日且在交易时间内: {current_hour:02d}:{current_minute:02d}")
        return True

    except Exception as e:
        logging.debug(f"[is_trading_time] 判断交易时间失败: {e}")
        return False


def get_latest_trade_date():
    """使用akshare获取上证指数的最后一个交易日"""
    try:
        df = ak.stock_zh_index_daily_em(symbol="sh000688")
        latest_trade_date = df['date'].values[-1]
        # 将日期转换为YYYYMMDD格式
        if isinstance(latest_trade_date, str):
            # 如果是字符串格式，需要解析
            if '-' in latest_trade_date:
                # 格式可能是 YYYY-MM-DD
                date_obj = datetime.strptime(latest_trade_date, '%Y-%m-%d')
            else:
                # 格式可能是 YYYYMMDD
                date_obj = datetime.strptime(latest_trade_date, '%Y%m%d')
        else:
            # 如果是datetime对象
            date_obj = pd.to_datetime(latest_trade_date)

        return date_obj.strftime("%Y%m%d")
    except Exception as e:
        logging.debug(f"[worker_queue] 获取最新交易日失败: {e}，使用当前日期")
        return datetime.now().strftime("%Y%m%d")

def get_data_dir():
    """获取用户数据目录"""
    if sys.platform == "darwin":  # macOS
        return os.path.expanduser("~/Library/Application Support/eastmoneywatch")
    elif sys.platform == "win32":  # Windows
        return os.path.expanduser("~/AppData/Local/eastmoneywatch")
    else:  # Linux
        return os.path.expanduser("~/.local/share/eastmoneywatch")


def get_resource_path(relative_path):
    """获取资源文件的路径，支持开发环境和打包环境"""
    try:
        # PyInstaller 创建临时文件夹 _MEIpass，并将路径存储在 _MEIPASS 中
        if hasattr(sys, '_MEIPASS'):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.dirname(os.path.abspath(__file__))

        full_path = os.path.join(base_path, relative_path)
        if os.path.exists(full_path):
            return full_path
        else:
            logging.debug(f"Warning: Resource not found at {full_path}")
            return None
    except Exception as e:
        logging.debug(f"Error accessing resource path: {e}")
        return None


def setup_static_directory():
    """设置静态文件目录"""
    if hasattr(sys, '_MEIPASS'):
        # 打包后：在用户数据目录创建
        static_dir = os.path.join(get_data_dir(), "static")
    else:
        # 开发模式：在当前目录创建
        static_dir = "static"

    os.makedirs(static_dir, exist_ok=True)
    return static_dir

def uplimit10jqka(date:str=''):
    '''
    Index(['open_num', 'first_limit_up_time', 'last_limit_up_time', 'code',
       'limit_up_type', 'order_volume', 'is_new', 'limit_up_suc_rate',
       'currency_value', 'market_id', 'is_again_limit', 'change_rate',
       'turnover_rate', 'reason_type', 'order_amount', 'high_days', 'name',
       'high_days_value', 'change_tag', 'market_type', 'latest',
       'time_preview'],
      dtype='object')

    股票代码=code, 几天几板文案=high_days
    '''
    cookies = {
    'v': 'A5lFEWDLFZlL3MkNmn0O1b5bro52JoyfdxqxbLtOFUA_wrfwA3adqAdqwTFI',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'zh-CN,zh-TW;q=0.9,zh;q=0.8,en-US;q=0.7,en;q=0.6,ja;q=0.5',
        'priority': 'u=1, i',
        'referer': 'https://data.10jqka.com.cn/datacenterph/limitup/limtupInfo.html?client_userid=nM9Y3&back_source=hyperlink&share_hxapp=isc&fontzoom=no',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        # 'cookie': 'v=A5lFEWDLFZlL3MkNmn0O1b5bro52JoyfdxqxbLtOFUA_wrfwA3adqAdqwTFI',
    }

    response = requests.get(
        f'https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool?page=1&limit=200&field=199112,10,9001,330323,330324,330325,9002,330329,133971,133970,1968584,3475914,9003,9004&filter=HS,GEM2STAR&date={date}&order_field=330324&order_type=0&_=1754378627951',
        cookies=cookies,
        headers=headers,
    )
    result = response.json()['data']['info']

    df = pd.DataFrame(result)
    df.to_csv('static/uplimit.csv')
    return df
