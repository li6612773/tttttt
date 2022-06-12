#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'steven li'

import datetime

import pandas as pd
from sqlalchemy import create_engine
import tushare as ts
from sqlalchemy.exc import OperationalError

import basis.Constant


def init_db():
    connect_info = basis.Constant.get_db_path()
    engine = create_engine(connect_info)  # use sqlalchemy to build link-engine
    init_schema(engine)
    init_hq_lost(engine)
    print(engine, '数据库链接初始化成功!')
    return engine


def init_ts_pro():
    print('ts_pro:', ts.__version__)
    token = basis.Constant.get_pro_token()  # 初始化pro接口
    ts.set_token(token)  # 设置token
    ts_pro = ts.pro_api()
    return ts_pro


def init_ts():
    print('ts:', ts.__version__)
    token = basis.Constant.get_pro_token()  # 初始化pro接口
    ts.set_token(token)  # 设置token
    return ts


def init_hq_lost(engine):
    sql = 'create table if not exists hq_lost(lost_Type varchar(200) null, ' \
          'trade_Date date null, ts_Code varchar(20) null, reason varchar(200) null)'
    engine.execute(sql)


def init_schema(engine):
    # 获得用户配置的数据库名称
    connect_info = basis.Constant.get_db_path()
    dbname_start = connect_info.rfind('/')
    dbname = connect_info[dbname_start + 1:]

    sql = 'create schema if not exists '
    sql = sql + dbname
    try:
        engine.execute(sql)
    except OperationalError:  # 如果第一次使用，找不到qtdb会报错，这里捕捉并处理
        connect_info = connect_info[0:dbname_start]
        engine = create_engine(connect_info)
        engine.execute(sql)


def init_currentDate():
    currentDate = datetime.datetime.now().strftime('%Y%m%d')
    return currentDate


def init_stock_codeList(engine):
    sql = """select distinct ts_code from hq_stock_basic order by ts_code """
    codeList = pd.read_sql_query(sql, engine)
    return codeList


def init_cb_codeList(engine):
    sql = """select distinct ts_code from hq_cb_basic order by ts_code """
    codeList = pd.read_sql_query(sql, engine)
    return codeList


def init_index_codeList(engine):
    sql = 'select distinct ts_code from hq_index_basic order by ts_code'
    codeList = pd.read_sql_query(sql, engine)
    return codeList


def is_working_date(engine, market, idate):
    df = pd.DataFrame()
    if market == 'CN':
        sql = 'select is_open from hq_trade_cal where exchange = \'SSE\' and cal_date = %s ' % idate
        df = pd.read_sql_query(sql, engine)
    elif market == 'HK':
        sql = 'select is_open from hq_hk_trade_cal where cal_date = %s ' % idate
        df = pd.read_sql_query(sql, engine)
    if df.__sizeof__() == 0:
        return True
    else:
        is_open = df.iloc[0][0]
        if is_open == '1':
            return True
        else:
            return False
