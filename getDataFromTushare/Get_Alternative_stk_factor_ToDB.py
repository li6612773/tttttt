'''
Created on 2022年6月4日

@author: 无相大师 版权所有：li6612773@163.com 购买授权可使用，非许可禁止传播

股票技术因子（量化因子）
接口：stk_factor
描述：获取股票每日技术面因子数据，用于跟踪股票当前走势情况，数据由Tushare社区自产，覆盖全历史
限量：单次最大10000条，可以循环或者分页提取
积分：120积分可查看数据，5000积分每分钟可以请求100次，8000积分以上每分钟500次，具体请参阅积分获取办法

注：
1、本接口的前复权行情是从最新一个交易日开始往前复权，跟行情软件一致。
2、pro_bar接口的前复权是动态复权，即以end_date参数开始往前复权，与本接口会存在不一致的可能，属正常。
3、本接口技术指标都是基于前复权价格计算。


输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码
trade_date	str	N	交易日期 （yyyymmdd，下同）
start_date	str	N	开始日期
end_date	str	N	结束日期


输出参数

名称	类型	默认显示	描述
ts_code	str	Y	股票代码
trade_date	str	Y	交易日期
close	float	Y	收盘价
open	float	Y	开盘价
high	float	Y	最高价
low	float	Y	最低价
pre_close	float	Y	昨收价
change	float	Y	涨跌额
pct_change	float	Y	涨跌幅
vol	float	Y	成交量 （手）
amount	float	Y	成交额 （千元）
adj_factor	float	Y	复权因子
open_hfq	float	Y	开盘价后复权
open_qfq	float	Y	开盘价前复权
close_hfq	float	Y	收盘价后复权
close_qfq	float	Y	收盘价前复权
high_hfq	float	Y	最高价后复权
high_qfq	float	Y	最高价前复权
low_hfq	float	Y	最低价后复权
low_qfq	float	Y	最低价前复权
pre_close_hfq	float	Y	昨收价后复权
pre_close_qfq	float	Y	昨收价前复权
macd_dif	float	Y	MCAD_DIF (基于前复权价格计算，下同)
macd_dea	float	Y	MCAD_DEA
macd	float	Y	MCAD
kdj_k	float	Y	KDJ_K
kdj_d	float	Y	KDJ_D
kdj_j	float	Y	KDJ_J
rsi_6	float	Y	RSI_6
rsi_12	float	Y	RSI_12
rsi_24	float	Y	RSI_24
boll_upper	float	Y	BOLL_UPPER
boll_mid	float	Y	BOLL_MID
boll_lower	float	Y	BOLL_LOWER
cci	float	Y	CCI
'''
import datetime
import time

import pandas as pd
from retry import retry
from sqlalchemy import TEXT
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate, init_index_codeList, init_stock_codeList
from basis.Tools import get_and_write_data_by_start_end_date_and_codelist

rows_limit = 8000  # 该接口限制每次调用，最大获取数据量
times_limit = 10000  # 该接口限制,每分钟最多调用次数
sleeptimes = 61
currentDate = init_currentDate()
prefix = 'hq_alternative_stk_factor'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE})
    tosqlret = df.to_sql(prefix+'_test', db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE})
    return tosqlret


@retry(tries=5, delay=61)
def get_data(ts_pro, code, offset, str_date, end_date):
    df = ts_pro.stk_factor(ts_code=code[0], start_date=str_date, end_date=end_date, limit=rows_limit, offset=offset)
    return df


def get_anns_daily(db_engine, ts_pro, start_date, end_date):
    # drop_table(db_engine)
    codelist = init_stock_codeList(db_engine)
    codelist = codelist
    # 读取行情数据，并存储到数据库
    df = get_and_write_data_by_start_end_date_and_codelist(db_engine, ts_pro, prefix, get_data, write_db, times_limit,
                                                           sleeptimes, rows_limit, codelist, start_date, end_date)


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()

    str_date = '20220101'
    end_date = currentDate

    get_anns_daily(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("加载完成，请复核是否正确执行！")
