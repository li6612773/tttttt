'''
Created on 2021年8月04日

@author: 无相大师

交易日历
接口：trade_cal
描述：获取各大交易所交易日历数据,默认提取的是上交所

输入参数

名称	类型	必选	描述
exchange	str	N	交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源,IB 银行间,XHKG 港交所
start_date	str	N	开始日期 （格式：YYYYMMDD 下同）
end_date	str	N	结束日期
is_open	str	N	是否交易 '0'休市 '1'交易
输出参数

名称	类型	默认显示	描述
exchange	str	Y	交易所 SSE上交所 SZSE深交所
cal_date	str	Y	日历日期
is_open	str	Y	是否交易 0休市 1交易
pretrade_date	str	N	上一个交易日
接口示例
pro = ts.pro_api()
df = pro.trade_cal(exchange='', start_date='20180101', end_date='20181231')
或者
df = pro.query('trade_cal', start_date='20180101', end_date='20181231')
'''
from retry import retry
from sqlalchemy.types import NVARCHAR, DATE, Integer, DECIMAL
from basis.Init_Env import init_db, init_ts_pro, init_currentDate
from basis.Tools import drop_Table, get_and_write_data_by_limit

rows_limit = 8000  # 该接口限制每次调用，最大获取数据量
times_limit = 1000000  # 该接口限制,每分钟最多调用次数
sleeptime = 61
currentDate = init_currentDate()
prefix = 'hq_trade_cal'


def write_db(df, db_engine):
    res = df.to_sql(prefix, db_engine, index=False, if_exists='append', chunksize=10000,
                    dtype={'exchange': NVARCHAR(20),
                           'cal_date': DATE,
                           'is_open': NVARCHAR(1),
                           'pretrade_date': DATE})
    return res


@retry(tries=2, delay=61)
def get_data(ts_pro, rows_limit, offset):
    df = ts_pro.trade_cal(limit=rows_limit, offset=offset)
    return df


def get_Trade_Cal(db_engine, ts_pro):
    drop_Table(db_engine, prefix)
    get_and_write_data_by_limit(prefix, db_engine, ts_pro,
                                get_data, write_db, rows_limit, times_limit, sleeptime)


if __name__ == '__main__':
    ts_pro = init_ts_pro()
    db_engine = init_db()

    get_Trade_Cal(db_engine, ts_pro)

    print('数据日期：', currentDate)
    end_str = input("交易日历更新完毕，请复核是否正确执行！")
