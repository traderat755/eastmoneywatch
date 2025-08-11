import os
import sys
import logging
import logging.handlers
from multiprocessing import Queue


# 类型映射字典
type_mapping = {
    '8201': '火箭发射',
    '8202': '快速反弹',
    # '8193': '大笔买入',
    '4': '封涨停板',
    # '32': '打开跌停板',
    # '64': '有大买盘',
    '8207': '竞价上涨',
    '8209': '高开5日线',
    # '8211': '向上缺口',
    # '8213': '60日新高',
    '8215': '60日大幅上涨',
    # '8204': '加速下跌',
    # '8203': '高台跳水',
    # '8194': '大笔卖出',
    # '8': '封跌停板',
    # '16': '打开涨停板',
    # '128': '有大卖盘',
    # '8208': '竞价下跌',
    # '8210': '低开5日线',
    # '8212': '向下缺口',
    # '8214': '60日新低',
    # '8216': '60日大幅下跌'
}


def setup_logging(level=logging.INFO):
    """配置日志记录"""
    # 1. 配置主进程日志
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # 2. 创建用于进程间通信的日志队列
    log_queue = Queue()

    # 3. 设置监听器以处理来自子进程的日志
    # 此监听器从log_queue获取日志，并将其发送到主进程的处理程序
    queue_listener = logging.handlers.QueueListener(log_queue, *logging.getLogger().handlers)
    queue_listener.start()

    logging.debug("[logging] 日志系统已配置完成。")
    
    return log_queue, queue_listener
