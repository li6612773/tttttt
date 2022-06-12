#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'li6612773@163.com'

import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getData.get_EveryDayData import get_data_by_reload_all, get_data_by_date, deal_wrong_date


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    ts = init_ts()
    currentDate = init_currentDate()
    # 指定日期是注意日期格式应为：'20210901'
    # str_date = currentDate
    # end_date = currentDate
    str_date = '20040302'
    end_date = currentDate
    # str_date = '20120101'
    # end_date = '20220121'

    # 加载列表信息，该类接口均为清空后重新加载，其中日期表建议加载一次就可以了
    # get_data_by_reload_all(db_engine, ts_pro)
    # 按日期段加载每日数据
    # get_data_by_date(db_engine, ts_pro, str_date, end_date)
    # 按日期段进行数据整理
    deal_wrong_date(db_engine, ts_pro, ts, str_date, end_date)

    # 输出信息
    print('数据加载完毕，数据日期：', end_date)
    end_str = input("今日数据加载完毕，请复核是否正确执行！")
