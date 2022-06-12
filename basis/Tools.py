#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'steven li'

import datetime
import math
import time

import pandas as pd
from retry import retry

from basis.Init_Env import init_currentDate, is_working_date

currentDate = init_currentDate()


def drop_Table(db_engine, prefix):
    sql_str = 'drop table if exists ' + prefix
    db_engine.execute(sql_str)
    print(prefix, '数据表，已删除:', sql_str)


def truncate_Table(db_engine, prefix):
    sql_str = 'truncate table ' + prefix
    db_engine.execute(sql_str)
    print(prefix, '数据表，已清空:', sql_str)


def get_and_write_data_by_limit(prefix, db_engine, ts_pro,
                                get_data, write_db, rows_limit, times_limit, sleeptime):
    print(prefix, '接口：已调用：')
    itimes = 1
    offset = 0
    while True:
        df = get_data(ts_pro, rows_limit, offset)
        res = write_db(df, db_engine)

        print(prefix, '接口：已调用：', itimes, '次，返回结果(None表示成功):', res, '  数据日期:', currentDate, '  数据条数:', len(df))

        if itimes % times_limit == 0:
            isleeptime = sleeptime
            print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', isleeptime, 's')
            time.sleep(isleeptime)
        elif len(df) < rows_limit:
            # itimes = itimes + 1
            break  # 读不到了，就跳出去读下一个日期的数据
        offset = offset + rows_limit
        itimes = itimes + 1
    return df


def get_and_write_data_by_code(db_engine, ts_pro, code,
                               get_data, write_db, prefix, times_limit, rows_limit):
    itimes = 1
    offset = 0
    while True:
        df = get_data(ts_pro, code, offset)
        res = write_db(df, db_engine)
        print(prefix, '接口：已调用：', itimes, '次，返回结果(None表示成功):', res, '  代码:', code, '  数据条数:', len(df))

        if itimes % times_limit == 0:
            isleeptime = 61
            print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', isleeptime, 's')
            time.sleep(isleeptime)
        elif len(df) < rows_limit:
            itimes = itimes + 1
            break  # 读不到了，就跳出去读下一个日期的数据
        offset = offset + rows_limit
        itimes = itimes + 1

    return df


def get_and_write_data_by_date(db_engine, ts_pro, market, start_date, end_date,
                               get_data, write_db, prefix, rows_limit, times_limit, sleeptime):
    print(prefix, '接口：已调用：传入日期： 开始日期：', start_date, '结束日期：', end_date)
    idate = start_date
    predate = 10010101
    itimes = 1
    offset = 0
    df = pd.DataFrame
    while idate <= end_date:
        is_open = is_working_date(db_engine, market, idate)
        if is_open:
            while True:
                df = get_data(ts_pro, idate, offset, rows_limit)
                res = write_db(df, db_engine)
                print(prefix, '接口：已调用：', itimes, '次，返回结果(None表示成功):', res, ' 数据日期:', idate, '  数据条数:', len(df))

                if itimes % times_limit == 0:
                    isleeptime = sleeptime
                    print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', isleeptime, 's')
                    time.sleep(isleeptime)
                elif len(df) < rows_limit:
                    itimes = itimes + 1
                    break  # 读不到了，就跳出去读下一个日期的数据
                offset = offset + rows_limit
                itimes = itimes + 1
            # 取下一日
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
            offset = 0
        else:
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
            offset = 0
            continue
    return df


def get_and_write_data_by_codelist(db_engine, ts_pro, codeList, prefix,
                                   get_data, write_db,
                                   rows_limit, times_limit, sleeptimes):
    itimes = 1
    iTotal = codeList.__len__()
    df = pd.DataFrame
    codeListArray = codeList.__array__()
    for code in codeListArray:
        offset = 0
        while True:
            df = get_data(ts_pro, code, offset)
            res = write_db(df, db_engine)
            print(prefix, '接口：已调用，调用时间：', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  '当前：', itimes, '/', codeList.__len__(), '次,代码：', code[0], '，写入数据库返回(None表示成功):', res,
                  '  数据条数:', len(df))

            if itimes % times_limit == 0:
                isleeptime = sleeptimes
                print(prefix, '接口：已调用', times_limit, '次，根据该接口限制，sleep ', sleeptimes, 's，后再继续调用！')
                time.sleep(isleeptime)
            elif len(df) < rows_limit:
                itimes = itimes + 1
                break  # 读不到了，就跳出去读下一个日期的数据
            offset = offset + rows_limit
            itimes = itimes + 1

    return df


def get_and_write_data_by_long_codelist(db_engine, ts_pro, codeList, prefix,
                                        get_data, write_db, codes_onetime,
                                        rows_limit, times_limit, sleeptimes):
    # codes_onetime ：一次调用最多获取多少个代码对应的数据，针对可以一次传多个代码的接口的快速取数
    itimes = 1  # 第几次调用
    i = 1  # 循环了几次
    codelist_len = codeList.__len__()
    codeListArray = codeList.__array__()
    codes = ''
    offset = 0
    df = pd.DataFrame
    for code in codeListArray:
        if codes == '':
            codes = code[0]
        else:
            codes = codes + ',' + code[0]
        if i % codes_onetime == 0 or i + 1 == codelist_len:
            # 对于每次代码列表的调用，按照该接口单次代码的限制进行多次取数据
            offset = 0
            while True:
                df = get_data(ts_pro, codes, rows_limit, offset)
                res = write_db(df, db_engine)

                print(prefix, '接口：已调用：', itimes,
                      '次，返回结果(None表示成功):', res, '  数据日期:', currentDate, '  数据条数:', len(df))

                if itimes % times_limit == 0:
                    print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', sleeptimes, 's')
                    time.sleep(sleeptimes)
                elif len(df) < rows_limit:
                    itimes = itimes + 1
                    break  # 读不到了，就跳出去读下一个日期的数据
                offset = offset + rows_limit
                itimes = itimes + 1
            codes = ''  # 本次证券列表用完后，清空，以便下一次调用时不会重复调用

        i = i + 1

    return df


def get_and_write_data_by_date_and_codelist(db_engine, ts_pro, prefix, times_limit, sleeptimes,
                                            get_data, write_db, codeList, str_date, end_date):
    idate = str_date
    iTimes = 0
    iTotal = codeList.__len__()
    codeListArray = codeList.__array__()
    df = pd.DataFrame
    while idate <= end_date:
        is_open = is_working_date(db_engine, idate)
        if is_open:
            for code in codeListArray:
                df = get_data(ts_pro, code, idate)
                res = write_db(df, db_engine)
                iTimes = iTimes + 1

                print(prefix, '接口：已调用，读取日期为', idate, '的数据， ', iTimes, '/',
                      codeList.__len__(), '次,代码：', code[0], '，写入数据库返回(None表示成功):', res,
                      '  抓取数据条数:', len(df), time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime()))

                if iTimes % times_limit == 0:
                    print(prefix, '接口：已调用', times_limit, '次，根据该接口限制，sleep ', sleeptimes, 's，后再继续调用！')
                    time.sleep(sleeptimes)

            # 取下一日
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
        else:
            # 取下一日
            stridate = datetime.datetime.strptime(idate, "%Y%m%d") + datetime.timedelta(days=1)
            idate = stridate.strftime('%Y%m%d')
            continue
    return df


def get_and_write_data_by_start_end_date_and_codelist(db_engine, ts_pro, prefix, get_data, write_db, times_limit,
                                                      sleeptimes, rows_limit, codeList, str_date_iso, end_date_iso):
    print(prefix, '接口：已调用：传入日期： 开始日期：', str_date_iso, '结束日期：', end_date_iso)
    iTimes = 1
    iCode = 1
    iTotal = codeList.__len__()
    codeListArray = codeList.__array__()
    df = pd.DataFrame
    for code in codeListArray:
        offset = 0
        while True:
            df = get_data(ts_pro, code, offset, str_date_iso, end_date_iso)
            res = write_db(df, db_engine)

            print(prefix, '接口：已调用，调用时间：', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
                  '读取日期从:', str_date_iso, '到 ', end_date_iso, '的数据， ', iCode, '/',
                  iTotal, '次,代码：', code[0], '，写入数据库返回(None表示成功):', res,
                  '  抓取数据条数:', len(df))
            if iTimes % times_limit == 0:
                isleeptime = sleeptimes
                print(prefix, '接口：在一分钟内已调用', times_limit, '次：sleep ', isleeptime, 's')
                time.sleep(isleeptime)
            elif len(df) < rows_limit:
                iTimes = iTimes + 1
                iCode = iCode + 1
                break  # 读不到了，就跳出去读下一个日期的数据
            offset = offset + rows_limit
            iTimes = iTimes + 1
    return df
