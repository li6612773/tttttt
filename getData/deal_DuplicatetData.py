#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'li6612773@163.com'

import datetime
import time

import pandas as pd

from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getData.get_LostData_By_Code import get_LostData_By_Code
from getData.get_LostData_By_Date import get_LostData_By_Date


def deal_duplicate_in_hq_lost(db_engine, prefix, pkey, str_date, end_date):
    print('===================================================================')
    print('deal_duplicate_data.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '开始查找重复数据')
    # 登记可能重复数据，有些情况不属于重复(比如hq_toplist中通日期和代码的数据有不重复的)，为方便通用处理也一并登记
    try:
        sql_str = 'select count(ct) from (select count(*) as ct from %s ' \
                  'where trade_date between \'%s\' and \'%s\' group by  %s ' \
                  'having count(*)>1)rt' % (prefix, str_date, end_date, pkey)
        ilost = pd.read_sql_query(sql_str, db_engine)
    except:
        sql_str = 'select count(*) from %s group by  %s having count(*)>1' % (prefix, pkey)
        ilost = pd.read_sql_query(sql_str, db_engine)
    # 如果没有缺失数据，就不用继续傻做了，节省时间
    if len(ilost.index) == 0 or ilost.iat[0, 0] == 0:
        print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '该表无重复数据!')
        return ilost
    else:
        print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '该表有重复数据：', ilost.iat[0, 0], ' 条！')
    # 给原表增加自增主键，便于后续找到重复数据并删除
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '给表增加自增主键，便于后续找到重复数据并删除!')
    sql_str = 'alter table %s add id int auto_increment primary  key ' % prefix
    try:
        res = db_engine.execute(sql_str)
    except Exception as ex:
        print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
              '给表增加自增主键出错，由于已经有自增主键，所以无需添加，继续往后处理!')

    # 将重复数据，从原表中移动到临时表tmp_duplicate
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '将数据去重后，登记到临时表tmp_duplicate！')
    sql_str = 'drop table if exists tmp_duplicate'
    res = db_engine.execute(sql_str)
    try:
        sql_str = 'create table tmp_duplicate select max(id) as id from %s ' \
                  'where trade_date between \'%s\' and \'%s\' ' \
                  'group by  %s ' % (prefix, str_date, end_date, pkey)
        res = db_engine.execute(sql_str)
    except:
        sql_str = 'create table tmp_duplicate select max(id) as id from %s tb ' \
                  'group by  %s ' % (prefix, pkey)
        res = db_engine.execute(sql_str)

    # 将数据从原表中删除
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '将重复数据从原表中删除！')
    try:
        sql_del = 'delete from %s a where a.trade_date between \'%s\' and \'%s\' ' \
                  'and a.id not in (select b.id from tmp_duplicate b)' % (prefix, str_date, end_date)
        res = db_engine.execute(sql_del)
    except:
        sql_del = 'delete from %s a where a.id not in (select b.id from tmp_duplicate b)' % prefix
        res = db_engine.execute(sql_del)

    # drop 临时表 tmp_duplicate
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '删除临时表 tmp_duplicate！')
    sql_str = 'drop table if exists tmp_duplicate'
    res = db_engine.execute(sql_str)
    # 去掉原表中的自增主键id
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '恢复原表表结构！')
    sql_str = 'alter table %s DROP COLUMN id' % prefix
    res = db_engine.execute(sql_str)
    print('deal_duplicate_date.', prefix, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
          '已完成对该表指定日期段，重复数据的去重处理！')


def deal_duplicate_data(db_engine, ts_pro, ts, tableList, str_date, end_date):
    # 登记重复数据
    for tb_pkey in tableList:
        tb = tb_pkey[0]
        pkey = tb_pkey[1]
        try:
            deal_duplicate_in_hq_lost(db_engine, tb, pkey, str_date, end_date)
        except Exception as e:
            print('%s table has %s error.' % (tb,e))
            continue


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = '20000101'
    end_date = currentDate
    ts = init_ts()

    # 用一个二维数组装[接口名，主键值]的列表
    ableList_d = [['hq_alternative_anns', 'ts_code, ann_date, title'],
                  # ['hq_stock_daily', 'trade_date, ts_code'],
                  # ['hq_stock_daily_basic', 'trade_date, ts_code'],
                  # ['hq_adj_factor', 'trade_date, ts_code'],
                  # ['hq_topinst', 'trade_date, ts_code, exalter, side, net_buy, reason'],
                  # ['hq_toplist', 'trade_date, ts_code, reason'],
                  # ['hq_fund_daily', 'trade_date, ts_code'],
                  # ['hq_hsgt_north_top10', 'trade_date,ts_code,market_type'],
                  # ['hq_cb_daily', 'trade_date, ts_code'],
                  # ['hq_repo_daily', 'trade_date, ts_code'],
                  # ['hq_financial_income', 'ts_code, ann_date, f_ann_date, end_date, update_flag'],
                  # ['hq_alternative_cctv_news', 'date, title'],
                  # ['hq_index_weight', 'index_code, con_code, trade_date'],
                  # ['hq_stock_moneyflow', 'trade_date, ts_code']
                  ]

    deal_duplicate_data(db_engine, ts_pro, ts, ableList_d, str_date, end_date)
