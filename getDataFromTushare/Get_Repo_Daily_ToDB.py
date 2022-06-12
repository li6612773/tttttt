'''
Created on 2021年7月18日

@author: 无相大师

债券回购日行情
接口：repo_daily
描述：债券回购日行情
限量：单次最大2000条，可多次提取，总量不限制
权限：用户需要累积2000积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
ts_code	str	N	TS代码
trade_date	str	N	交易日期(YYYYMMDD格式，下同)
start_date	str	N	开始日期
end_date	str	N	结束日期
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS代码
trade_date	str	Y	交易日期
repo_maturity	str	Y	期限品种
pre_close	float	Y	前收盘(%)
open	float	Y	开盘价(%)
high	float	Y	最高价(%)
low	float	Y	最低价(%)
close	float	Y	收盘价(%)
weight	float	Y	加权价(%)
weight_r	float	Y	加权价(利率债)(%)
amount	float	Y	成交金额(万元)
num	int	Y	成交笔数(笔)


接口使用


pro = ts.pro_api()

#获取2020年8月4日债券回购日行情
df = pro.repo_daily(trade_date='20200804')



数据样例

    ts_code trade_date repo_maturity      weight            amount
0   131800.SZ   20200804         R-003  2.02150000      42783.000000
1   131801.SZ   20200804         R-007  2.23240000     618050.300000
2   131802.SZ   20200804         R-014  2.24820000      59506.300000
3   131803.SZ   20200804         R-028  2.35080000      21210.700000
4   131805.SZ   20200804         R-091  2.35550000       2566.000000
5   131806.SZ   20200804         R-182  2.10840000        113.200000
6   131809.SZ   20200804         R-004  2.06990000      24218.900000
7   131810.SZ   20200804         R-001  2.03600000   10748048.000000
8   131811.SZ   20200804         R-002  2.01270000      39459.200000
9   131981.SZ   20200804        RR-001  6.70000000       1000.000000
10  131982.SZ   20200804        RR-007  6.05000000      22800.000000
11  131983.SZ   20200804        RR-014  5.82000000      18500.000000
12  131985.SZ   20200804         RR-1M  7.00000000       4900.000000
13  204001.SH   20200804         GC001  2.10000000   85393260.000000
14  204002.SH   20200804         GC002  2.09200000     488300.000000
15  204003.SH   20200804         GC003  2.11900000    1260240.000000
16  204004.SH   20200804         GC004  2.16500000     352040.000000
17  204007.SH   20200804         GC007  2.21200000   13110650.000000
18  204014.SH   20200804         GC014  2.25900000    2318820.000000
19  204028.SH   20200804         GC028  2.32100000    1204850.000000
20  204091.SH   20200804         GC091  2.41500000      16330.000000
21  204182.SH   20200804         GC182  2.25800000         80.000000
22  206001.SH   20200804          R001  4.00300000      66518.000000
23  206007.SH   20200804          R007  4.36600000     530473.000000
24  206014.SH   20200804          R014  5.16900000     344245.000000
25  206021.SH   20200804          R021  5.97600000      17976.000000
26  206030.SH   20200804           R1M  5.33200000      56671.000000
27  206090.SH   20200804           R3M  7.59900000       9285.000000
28  207007.SH   20200804        TPR007  2.29900000      37500.000000
29   DR001.IB   20200804         DR001  1.90740000  196463895.000000
30   DR007.IB   20200804         DR007  2.11440000    8751142.000000
31   DR014.IB   20200804         DR014  1.99320000    2810816.000000
32   DR021.IB   20200804         DR021  2.08610000    1800794.000000
33    DR1M.IB   20200804          DR1M  2.02160000     239369.000000
34    DR3M.IB   20200804          DR3M  2.58500000      49956.000000
35    DR6M.IB   20200804          DR6M  2.60000000      10000.000000
36   OR001.IB   20200804         OR001  1.91850000    2677840.000000
37   OR007.IB   20200804         OR007  2.05950000     358750.000000
38   OR014.IB   20200804         OR014  2.41020000     129650.000000
39   OR021.IB   20200804         OR021  1.76630000      42000.000000
40    OR1M.IB   20200804          OR1M  2.36910000      34000.000000
41    R001.IB   20200804          R001  1.96000000  350750823.000000
42    R007.IB   20200804          R007  2.17850000   42502804.000000
43    R014.IB   20200804          R014  2.24390000    8123663.000000
44    R021.IB   20200804          R021  1.97400000    6072093.000000
45     R1M.IB   20200804           R1M  2.44950000    1163185.000000
46     R2M.IB   20200804           R2M  3.91140000      37170.000000
47     R3M.IB   20200804           R3M  2.92950000      88436.000000
48     R4M.IB   20200804           R4M  6.50000000       1750.000000
49     R6M.IB   20200804           R6M  2.60000000      10000.000000
'''
import datetime
import math
import time

import pandas as pd
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL

from basis.Init_Env import init_ts_pro, init_db, init_currentDate
from basis.Tools import get_and_write_data_by_date

rows_limit = 5000  # 该接口限制每次调用，最大获取数据量
times_limit = 50000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
prefix = 'hq_repo_daily'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'ts_code': NVARCHAR(20),
                                'trade_date': DATE,
                                'repo_maturity': NVARCHAR(50),
                                'pre_close': DECIMAL(17, 2),
                                'open': DECIMAL(17, 2),
                                'high': DECIMAL(17, 2),
                                'low': DECIMAL(17, 2),
                                'close': DECIMAL(17, 2),
                                'weight': DECIMAL(17, 2),
                                'weight_r': DECIMAL(17, 2),
                                'amount': DECIMAL(17, 2),
                                'num': DECIMAL(17, 2)})
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.repo_daily(start_date=idate, end_date=idate, limit=rows_limit, offset=offset)
    return df


def get_Repo_Daily(db_engine, ts_pro, start_date, end_date):
    df = get_and_write_data_by_date(db_engine, ts_pro, 'CN', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit,
                                    sleeptime)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()
    str_date = '19900101'
    end_date = '20220219'

    get_Repo_Daily(db_engine, ts_pro, str_date, end_date)

    print('数据日期：', currentDate)
    end_str = input("当日日线行情加载完毕，请复核是否正确执行！")
