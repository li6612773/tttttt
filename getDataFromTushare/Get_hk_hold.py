'''
Created on 2021年7月18日

@author: 无相大师 版权所有：li6612773@163.com 购买授权可使用，非许可禁止传播

沪深港股通持股明细
接口：hk_hold，可以通过数据工具调试和查看数据。
描述：获取沪深港股通持股明细，数据来源港交所。
限量：单次最多提取3800条记录，可循环调取，总量不限制
积分：用户积120积分可调取试用，2000积分可正常使用，单位分钟有流控，积分越高流量越大，请自行提高积分，具体请参阅积分获取办法



输入参数

名称	类型	必选	描述
code	str	N	交易所代码
ts_code	str	N	TS股票代码
trade_date	str	N	交易日期
start_date	str	N	开始日期
end_date	str	N	结束日期
exchange	str	N	类型：SH沪股通（北向）SZ深股通（北向）HK港股通（南向持股）


输出参数

名称	类型	默认显示	描述
code	str	Y	原始代码
trade_date	str	Y	交易日期
ts_code	str	Y	TS代码
name	str	Y	股票名称
vol	int	Y	持股数量(股)
ratio	float	Y	持股占比（%），占已发行股份百分比
exchange	str	Y	类型：SH沪股通SZ深股通HK港股通


接口示例


pro = ts.pro_api()

#获取单日全部持股
df = pro.hk_hold(trade_date='20190625')

#获取单日交易所所有持股
df = pro.hk_hold(trade_date='20190625', exchange='SH')



数据示例

      code  trade_date    ts_code      name        vol  ratio exchange
0     90000   20190625  600000.SH  浦发银行  443245164   1.57       SH
1     90004   20190625  600004.SH  白云机场  155708039   7.52       SH
2     90006   20190625  600006.SH  东风汽车     601353   0.03       SH
3     90007   20190625  600007.SH  中国国贸   17604694   1.74       SH
4     90008   20190625  600008.SH  首创股份   49944370   1.03       SH
5     90009   20190625  600009.SH  上海机场  288832383  26.41       SH
6     90010   20190625  600010.SH  包钢股份  324923948   1.02       SH
7     90011   20190625  600011.SH  华能国际   58734656   0.55       SH
8     90012   20190625  600012.SH  皖通高速   24047942   2.06       SH
9     90015   20190625  600015.SH  华夏银行  121539342   0.94       SH
10    90016   20190625  600016.SH  民生银行  541638767   1.52       SH
11    90017   20190625  600017.SH   日照港   32949908   1.07       SH
12    90018   20190625  600018.SH  上港集团   74011645   0.31       SH
13    90019   20190625  600019.SH  宝钢股份  511044106   2.31       SH
14    90020   20190625  600020.SH  中原高速   12439016   0.55       SH
15    90021   20190625  600021.SH  上海电力    2882596   0.13       SH
16    90023   20190625  600023.SH  浙能电力   38130882   0.28       SH
17    90025   20190625  600025.SH  华能水电  280356836   3.14       SH
18    90026   20190625  600026.SH  中远海能   81911786   2.99       SH
19    90027   20190625  600027.SH  华电国际   65877064   0.94       SH
20    90028   20190625  600028.SH  中国石化  709509578   0.74       SH
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
prefix = 'hk_hold'


def write_db(df, db_engine):
    tosqlret = df.to_sql(prefix, db_engine, chunksize=1000000, if_exists='append', index=False,
                         dtype={'code': NVARCHAR(20),
                                'trade_date': DATE,
                                'ts_code': NVARCHAR(20),
                                'name': NVARCHAR(200),
                                'vol':DECIMAL(17, 4),
                                'ratio':DECIMAL(17, 4),
                                'exchange': NVARCHAR(20),


                                })
    return tosqlret


@retry(tries=2, delay=61)
def get_data(ts_pro, idate, offset, rows_limit):
    df = ts_pro.hk_hold(trade_date=idate, end_date=idate, limit=rows_limit, offset=offset)
    return df


def get_hk_hold(db_engine, ts_pro, start_date, end_date):
    df = get_and_write_data_by_date(db_engine, ts_pro, 'HK', start_date, end_date,
                                    get_data, write_db, prefix, rows_limit, times_limit,
                                    sleeptime)  # 读取行情数据，并存储到数据库


if __name__ == '__main__':
    # 初始化
    db_engine = init_db()
    ts_pro = init_ts_pro()
    currentDate = init_currentDate()

    get_hk_hold(db_engine, ts_pro, '20220101', currentDate)

    print('数据日期：', currentDate)
    end_str = input("当日线行情加载完毕，请复核是否正确执行！")
