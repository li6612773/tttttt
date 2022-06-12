#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'li6612773@163.com'

import os
import sys

import pandas as pd

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getDataFromTushare.Get_Adj_Factor_By_Code_ToDB import get_Adj_Factor_By_Code
from getDataFromTushare.Get_Stock_Daily_Basic_By_Code_ToDB import get_Daily_Basic_By_Code
from getDataFromTushare.Get_Stock_Daily_By_Code_ToDB import get_Daily_By_Code
from getDataFromTushare.Get_Fund_Daily_By_Code_ToDB import get_Fund_Daily_By_Code

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)


def init_LostList_by_code(db_engine):
    sql = """select distinct lost_Type,ts_Code from hq_lost where ts_Code is not null order by lost_Type ,ts_Code """
    lostList = pd.read_sql_query(sql, db_engine)
    return lostList


def get_LostData_by_code_and_lost_Type(db_engine, ts_pro, get_data, ts_code, lost_Type):
    # 删除多余数据
    sql = "delete from %s where ts_code = \'%s\' " % (lost_Type, ts_code)
    res = db_engine.execute(sql)
    print('get_LostData_By_Code.', lost_Type, ':已删除表中ts_code为：', ts_code, '的数据')
    # 加载数据
    get_data(db_engine, ts_pro, ts_code)
    # 删除lost数据库数据
    sql = "delete from hq_lost where lost_Type = \'%s\' and ts_code = \'%s\'  " % (lost_Type, ts_code)
    res = db_engine.execute(sql)
    print('get_LostData_By_Code.', lost_Type, ':补全数据后，已删除hq_lost中lost_Type为',
          lost_Type, '，ts_code为', ts_code, '的数据')


def get_LostData_By_Code(db_engine, ts_pro, ts, tableDict_code):
    # 读取缺失数据列表
    lostListArray = init_LostList_by_code(db_engine).__array__()
    # 补充缺失数据
    for lost_Type, trade_Date in lostListArray:
        if lost_Type in tableDict_code.keys():
            get_LostData_by_code_and_lost_Type(db_engine, ts_pro, tableDict_code[lost_Type], trade_Date, lost_Type)
        else:
            print('get_LostData_By_Code：找不到：', lost_Type,
                  '接口的处理模块！请到get_EveryDayData.py的函数中添加处理模块！（如添加了table_Dict_date,该告警可忽略）')


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    ts = init_ts()
    currentDate = init_currentDate()
    # str_date = currentDate
    # end_date = currentDate
    # str_date = '20211105'
    # end_date = '20211110'
    tableDict_code = {'hq_stock_daily_basic': get_Daily_Basic_By_Code,
                      'hq_stock_daily': get_Daily_By_Code,
                      'hq_adj_factor': get_Adj_Factor_By_Code,
                      'hq_fund_daily': get_Fund_Daily_By_Code
                      }

    get_LostData_By_Code(db_engine, ts_pro, ts, tableDict_code)

    # 输出信息
    print('数据加载完毕，数据日期：', currentDate)
    # end_str = input("今日数据加载完毕，请复核是否正确执行！")
