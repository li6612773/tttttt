#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'li6612773@163.com'

import pandas as pd

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getDataFromTushare.Get_Stock_Daily_Basic_ToDB import get_Stock_Daily_Basic


def init_LostList_by_date(db_engine):
    sql = """select distinct lost_Type,trade_Date from hq_lost order by lost_Type desc,trade_Date """
    lostList = pd.read_sql_query(sql, db_engine)
    return lostList


def get_LostData_by_date_and_lost_Type(db_engine, ts_pro, get_data, trade_Date, lost_Type):
    # 删除多余数据
    idate = str(trade_Date.strftime('%Y%m%d'))
    sql = "delete a from %s a where trade_date = \'%s\' " % (lost_Type, idate)
    res = db_engine.execute(sql)
    print('get_LostData_By_Date.', lost_Type, ':已删除表中trade_date为：', idate, '的数据')
    # 加载数据
    get_data(db_engine, ts_pro, idate, idate)
    # 删除lost数据库数据
    sql = "delete a from hq_lost a where lost_Type = \'%s\' and trade_Date = \'%s\'  " % (lost_Type, idate)
    res = db_engine.execute(sql)
    print('get_LostData_By_Date.', lost_Type, ':补全数据后，已删除hq_lost中lost_Type为', lost_Type, '，trade_date日期为', idate,
          ' 的数据')


def get_LostData_By_Date(db_engine, ts_pro, ts, tableDict_date):
    # 读取缺失数据列表
    lostListArray = init_LostList_by_date(db_engine).__array__()

    # 补充缺失数据
    for lost_Type, trade_Date in lostListArray:
        if lost_Type in tableDict_date.keys():
            get_LostData_by_date_and_lost_Type(db_engine, ts_pro, tableDict_date[lost_Type], trade_Date, lost_Type)
        else:
            print('get_LostData_By_Date：找不到：', lost_Type,
                  '接口的处理模块！请到get_EveryDayData.py的tableDict_date数组中添加处理模块！')


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    ts = init_ts()
    currentDate = init_currentDate()
    # str_date = currentDate
    # end_date = currentDate
    str_date = '20211105'
    end_date = '20211110'
    tableDict_date = {'hq_stock_daily': get_Stock_Daily_Basic}

    get_LostData_By_Date(db_engine, ts_pro, ts, tableDict_date)

    # 输出信息
    print('数据加载完毕，数据日期：', end_date)
    # end_str = input("今日数据加载完毕，请复核是否正确执行！")
