#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'li6612773@163.com'

import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from getDataFromTushare.Get_HSGT_North_Top10_ToDB import get_hsgt_north_top10
from getDataFromTushare.Get_Index_Daily_ToDB import get_index_daily
from getDataFromTushare.Get_Stock_Daily_ToDB import get_Stock_Daily
from getDataFromTushare.Get_Index_Basic_ToDB import get_index_basic
from getData.deal_DuplicatetData import deal_duplicate_data
from getData.deal_LostData import deal_lost_data
from getDataFromTushare.Get_Cb_Basics_ToDB import get_Cb_Basic
from basis.Init_Env import init_db, init_ts_pro, init_currentDate, init_ts
from getDataFromTushare.Get_Adj_Factor_ToDB import get_Adj_Factor
from getDataFromTushare.Get_Stock_Daily_Basic_ToDB import get_Stock_Daily_Basic
from getDataFromTushare.Get_Fund_Basics_ToDB import get_Fund_Basic
from getDataFromTushare.Get_Fund_Daily_ToDB import get_Fund_Daily
from getDataFromTushare.Get_Stock_Basics_ToDB import get_Stock_Basic
from getDataFromTushare.Get_TopInst_ToDB import get_TopInst
from getDataFromTushare.Get_TopList_ToDB import get_TopList
from getDataFromTushare.Get_TradeCal_ToDB import get_Trade_Cal
from getDataFromTushare.Get_Adj_Factor_By_Code_ToDB import get_Adj_Factor_By_Code
from getDataFromTushare.Get_Cb_Daily_ToDB import get_Cb_Daily
from getDataFromTushare.Get_Fund_Daily_By_Code_ToDB import get_Fund_Daily_By_Code
from getDataFromTushare.Get_Repo_Daily_ToDB import get_Repo_Daily
from getDataFromTushare.Get_Stock_Daily_Basic_By_Code_ToDB import get_Daily_Basic_By_Code
from getDataFromTushare.Get_Stock_Daily_By_Code_ToDB import get_Daily_By_Code
from getDataFromTushare.Get_Stock_Moneyflow_ToDB import get_Stock_Moneyflow
from getDataFromTushare.Get_Index_Weight_ToDB import get_Index_Weight
from getDataFromTushare.Get_Alternative_CCTV_News_ToDB import get_Alternative_CCTV_News
from getDataFromTushare.Get_HK_Basics_D_ToDB import get_hk_Basic_D
from getDataFromTushare.Get_HK_Basics_ToDB import get_hk_Basic
from getDataFromTushare.Get_HK_CCASS_Hold_Detail_ToDB import get_HK_CCASS_Hold_Detail
from getDataFromTushare.Get_HK_Daily_ToDB import get_HK_Daily
from getDataFromTushare.Get_HK_TradeCal_ToDB import get_HK_Trade_Cal
from getDataFromTushare.Get_Financial_Income_ToDB import get_financial_income
from getDataFromTushare.Get_report_rc import get_report_rc
from getDataFromTushare.Get_Alternative_stk_factor_ToDB import get_anns_daily
from getDataFromTushare.Get_hk_hold import get_hk_hold

from getDataFromTushare.Get_Cb_Min_ToDB import get_Cb_Min_By_date_and_codelist
from getDataFromTushare.Get_Stock_Min_ToDB import get_stock_Min_By_date_and_codelist
from getDataFromTushare.Get_Stock_STK_Rewards_Fast_ToDB import get_stock_stk_rewards_fast


def get_data_by_reload_all(db_engine, ts_pro):
    # 加载日期表，可注释掉，不用每次都加
    get_Trade_Cal(db_engine, ts_pro)  # 加载日期表
    # 股票
    get_Stock_Basic(db_engine, ts_pro)  # 证券信息
    # get_stock_stk_rewards_fast(db_engine, ts_pro)  # 管理层薪酬和持股
    # 港股
    get_HK_Trade_Cal(db_engine, ts_pro)
    get_hk_Basic(db_engine, ts_pro)
    get_hk_Basic_D(db_engine, ts_pro)
    # 转债
    get_Cb_Basic(db_engine, ts_pro)
    # 基金
    get_Fund_Basic(db_engine, ts_pro)  # 公募基金列表
    # 指数
    get_index_basic(db_engine, ts_pro)


def get_data_by_date(db_engine, ts_pro, str_date, end_date):
    # 加载当日数据

    # 股票
    get_Stock_Daily(db_engine, ts_pro, str_date, end_date)  # 日线行情
    get_Stock_Daily_Basic(db_engine, ts_pro, str_date, end_date)  # 行情扩展
    get_Adj_Factor(db_engine, ts_pro, str_date, end_date)  # 复权因子
    get_TopInst(db_engine, ts_pro, str_date, end_date)  # 龙虎榜机构明细
    get_TopList(db_engine, ts_pro, str_date, end_date)  # 龙虎榜每日明细
    get_Stock_Moneyflow(db_engine, ts_pro, str_date, end_date)  # 个股资金流向

    # 港股
    get_HK_Daily(db_engine, ts_pro, str_date, end_date)
    get_HK_CCASS_Hold_Detail(db_engine, ts_pro, str_date, end_date)

    # 转债
    get_Cb_Daily(db_engine, ts_pro, str_date, end_date)

    # 回购
    get_Repo_Daily(db_engine, ts_pro, str_date, end_date)

    # 基金
    get_Fund_Daily(db_engine, ts_pro, str_date, end_date)  # 场内基金日线行情

    # 港股通
    get_Index_Weight(db_engine, ts_pro, str_date, end_date)  # 指数成分和权重
    get_hsgt_north_top10(db_engine, ts_pro, str_date, end_date)  # 沪深股通十大成交股

    # 财务数据
    get_financial_income(db_engine, ts_pro)  # 利润表

    # 另类数据
    get_Alternative_CCTV_News(db_engine, ts_pro, str_date, end_date)  # 新闻联播文字稿，建议5000分以上再开启

    # 指数
    get_index_daily(db_engine, ts_pro, str_date, end_date)

    # tick 数据，如果需要抓取tick数据解除该处注释即可. 注意：该接口权限需向tushare官方单独购买
    # get_stock_Min_By_date_and_codelist(db_engine, ts, str_date, end_date)  # 1min tick数据
    # get_Cb_Min_By_date_and_codelist(db_engine, ts, str_date, end_date)

    get_report_rc(db_engine, ts_pro, str_date, end_date)
    get_hk_hold(db_engine, ts_pro,str_date, end_date)
    get_anns_daily(db_engine, ts_pro, str_date, end_date)


def deal_wrong_date(db_engine, ts_pro, ts, str_date, end_date):
    # 整理重复数据和该日期段缺失数据
    print('===================整理重复数据和该日期段缺失数据======================')
    # 这里需要设置接口名和主键值得二维数组
    tableList_d = [['hq_stock_daily', 'trade_date, ts_code'],
                   ['hq_stock_daily_basic', 'trade_date, ts_code'],
                   ['hq_hk_daily', 'trade_date, ts_code'],
                   ['hq_hk_ccass_hold_detail', 'ts_code, trade_date, col_participant_id'],
                   ['hq_adj_factor', 'trade_date, ts_code'],
                   ['hq_topinst', 'trade_date, ts_code, exalter, side, net_buy, reason'],
                   ['hq_toplist', 'trade_date, ts_code, reason'],
                   ['hq_fund_daily', 'trade_date, ts_code'],
                   ['hq_hsgt_north_top10', 'trade_date,ts_code,market_type'],
                   ['hq_cb_daily', 'trade_date, ts_code'],
                   ['hq_repo_daily', 'trade_date, ts_code'],
                   ['hq_financial_income', 'ts_code, ann_date, f_ann_date, end_date, update_flag'],
                   ['hq_alternative_cctv_news', 'date, title'],
                   ['hq_index_weight', 'index_code, con_code, trade_date'],
                   ['hq_stock_moneyflow', 'trade_date, ts_code']
                   ]

    deal_duplicate_data(db_engine, ts_pro, ts, tableList_d, str_date, end_date)
    # 这里，按传入的日期段和接口表列表和对应的函数入口，补全缺失数据,在tableDict_date字典里的接口就是要补全的接口，
    # tableDict_code里面有没有登记没关系，只是个效率优化手段
    tableDict_date = {'hq_stock_daily': get_Stock_Daily,
                      'hq_stock_daily_basic': get_Stock_Daily_Basic,
                      'hq_hk_daily': get_HK_Daily,
                      'hq_hk_ccass_hold_detail': get_HK_CCASS_Hold_Detail,
                      'hq_adj_factor': get_Adj_Factor,
                      'hq_topinst': get_TopInst,
                      'hq_toplist': get_TopList,
                      'hq_fund_daily': get_Fund_Daily,
                      'hq_hsgt_north_top10': get_hsgt_north_top10,
                      'hq_cb_daily': get_Cb_Daily,
                      'hq_repo_daily': get_Repo_Daily,
                      'hq_index_weight': get_Index_Weight,
                      'hq_stock_moneyflow': get_Stock_Moneyflow,
                      'hq_alternative_cctv_news': get_Alternative_CCTV_News
                      # 'hq_cb_min': get_Cb_Min_By_date_and_codelist,
                      # 'hq_stock_min': get_stock_Min_By_date_and_codelist
                      }
    tableDict_code = {'hq_stock_daily_basic': get_Daily_Basic_By_Code,
                      'hq_stock_daily': get_Daily_By_Code,
                      'hq_adj_factor': get_Adj_Factor_By_Code,
                      'hq_fund_daily': get_Fund_Daily_By_Code,
                      'hq_stock_moneyflow': get_financial_income
                      }
    deal_lost_data(db_engine, ts_pro, ts, tableDict_date, tableDict_code, str_date, end_date)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    ts = init_ts()
    currentDate = init_currentDate()
    # 指定日期是注意日期格式应为：'20210901'
    # str_date = currentDate
    # end_date = currentDate
    str_date = '20090101'
    end_date = currentDate

    # 加载列表信息，该类接口均为清空后重新加载，其中日期表建议加载一次就可以了
    get_data_by_reload_all(db_engine, ts_pro)
    # 按日期段加载每日数据
    get_data_by_date(db_engine, ts_pro, str_date, end_date)
    # 按日期段进行数据整理
    deal_wrong_date(db_engine, ts_pro, ts, str_date, end_date)

    # 输出信息
    print('数据加载完毕，数据日期：', end_date)
    end_str = input("今日数据加载完毕，请复核是否正确执行！")
