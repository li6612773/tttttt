#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'li6612773@163.com'

import datetime
import time

import pandas as pd

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getData.get_LostData_By_Code import get_LostData_By_Code
from getData.get_LostData_By_Date import get_LostData_By_Date
from getDataFromTushare.Get_Adj_Factor_By_Code_ToDB import get_Adj_Factor_By_Code
from getDataFromTushare.Get_Adj_Factor_ToDB import get_Adj_Factor
from getDataFromTushare.Get_Cb_Daily_ToDB import get_Cb_Daily
from getDataFromTushare.Get_Fund_Daily_By_Code_ToDB import get_Fund_Daily_By_Code
from getDataFromTushare.Get_Fund_Daily_ToDB import get_Fund_Daily
from getDataFromTushare.Get_HSGT_North_Top10_ToDB import get_hsgt_north_top10
from getDataFromTushare.Get_Index_Weight_ToDB import get_Index_Weight
from getDataFromTushare.Get_Repo_Daily_ToDB import get_Repo_Daily
from getDataFromTushare.Get_Stock_Daily_Basic_By_Code_ToDB import get_Daily_Basic_By_Code
from getDataFromTushare.Get_Stock_Daily_Basic_ToDB import get_Stock_Daily_Basic
from getDataFromTushare.Get_Stock_Daily_By_Code_ToDB import get_Daily_By_Code
from getDataFromTushare.Get_Stock_Daily_ToDB import get_Stock_Daily
from getDataFromTushare.Get_Stock_Moneyflow_ToDB import get_Stock_Moneyflow
from getDataFromTushare.Get_TopInst_ToDB import get_TopInst
from getDataFromTushare.Get_TopList_ToDB import get_TopList


def insert_lost_into_hq_lost_by_date(db_engine, prefix, str_date, end_date):
    print('===================================================================')
    print('deal_lost_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '缺失数据登记')
    try:

        sql_str = 'insert into hq_lost select \'%s\' as lost_Type, jg.cal_date, jg.ts_code, \'lost\' as reason from ' \
                  '(select * from (select cal_date from hq_trade_cal where is_open = \'1\' and exchange = \'SSE\' ' \
                  'and cal_date between \'%s\' and \'%s\') cal left join %s tb on cal.cal_date = tb.trade_date ) jg ' \
                  'where jg.trade_date is null' % (prefix, str_date, end_date, prefix)
        res = db_engine.execute(sql_str)
    except:
        try:
            sql_str = 'insert into hq_lost select \'%s\' as lost_Type, ' \
                      'jg.cal_date as trade_Date, \' \' as ts_Code, \'lost\' as reason from ' \
                      '(select * from (select cal_date from hq_trade_cal where is_open = \'1\' and exchange = \'SSE\' ' \
                      'and cal_date between \'%s\' and \'%s\') cal left join %s tb on cal.cal_date = tb.trade_date ) jg ' \
                      'where jg.trade_date is null' % (prefix, str_date, end_date, prefix)
            res = db_engine.execute(sql_str)
        except:
            print('deal_lost_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  '无法识别该表在该段日期中是否有缺失数据，可能是该表缺少trade_date日期字段!')
    sql = sql = 'select count(*) from hq_lost where reason = \'lost\' and lost_Type = \'%s\'' % prefix
    ilost = pd.read_sql_query(sql, db_engine)
    if ilost.iat[0, 0] == 0:
        print('deal_lost_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '该表在日期段：', str_date, '至', end_date, '期间无缺失数据!')
        return ilost
    else:
        print('deal_lost_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '该表', '在日期段：', str_date, '至', end_date, '期间，有缺失数据：', ilost.iat[0, 0], ' 条！',
              '缺失数据所对应的日期，已登记到丢失数据表hq_lost中，后续数据补全程序会补全数据！')


def deal_lost_data(db_engine, ts_pro, ts, tableDict_date, tableDict_code, str_date, end_date):
    # 登记缺失数据
    for tb in tableDict_date:
        insert_lost_into_hq_lost_by_date(db_engine, tb, str_date, end_date)
    # 补全数据
    print('===================================================================')
    print('deal_lost_data', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '开始补全')
    icode = how_many_by_code(db_engine).iat[0, 0]
    idate = how_many_by_date(db_engine).iat[0, 0]
    if (icode <= idate):
        get_LostData_By_Code(db_engine, ts_pro, ts, tableDict_code)
        get_LostData_By_Date(db_engine, ts_pro, ts, tableDict_date)
    else:
        get_LostData_By_Date(db_engine, ts_pro, ts, tableDict_date)
        get_LostData_By_Code(db_engine, ts_pro, ts, tableDict_code)


def how_many_by_code(db_engine):
    sql = """select count(distinct lost_Type,ts_Code) from hq_lost where reason = \'lost\' """
    ibycode = pd.read_sql_query(sql, db_engine)
    return ibycode


def how_many_by_date(db_engine):
    sql = """select count(distinct lost_Type,trade_Date) from hq_lost where reason = \'lost\'  """
    ibydate = pd.read_sql_query(sql, db_engine)
    return ibydate


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    ts = init_ts()
    currentDate = init_currentDate()
    tableList = ['hq_toplist']
    str_date = '20120101'
    end_date = currentDate

    tableDict_date = {'hq_stock_daily': get_Stock_Daily,
                      'hq_stock_daily_basic': get_Stock_Daily_Basic,
                      'hq_adj_factor': get_Adj_Factor,
                      'hq_topinst': get_TopInst,
                      'hq_toplist': get_TopList,
                      'hq_fund_daily': get_Fund_Daily,
                      # 'hq_hsgt_north_top10': get_hsgt_north_top10,
                      'hq_cb_daily': get_Cb_Daily,
                      'hq_repo_daily': get_Repo_Daily,
                      'hq_index_weight': get_Index_Weight,
                      # 'hq_stock_moneyflow': get_Stock_Moneyflow,
                      # 'hq_alternative_cctv_news': get_Alternative_CCTV_News
                      # 'hq_cb_min': get_Cb_Min_By_date_and_codelist,
                      # 'hq_stock_min': get_stock_Min_By_date_and_codelist
                      }
    tableDict_code = {'hq_stock_daily_basic': get_Daily_Basic_By_Code,
                      'hq_stock_daily': get_Daily_By_Code,
                      'hq_adj_factor': get_Adj_Factor_By_Code,
                      'hq_fund_daily': get_Fund_Daily_By_Code,
                      # 'hq_stock_moneyflow': get_financial_income
                      }
    deal_lost_data(db_engine, ts_pro, ts, tableDict_date, tableDict_code, str_date, end_date)

