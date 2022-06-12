drop index  hq_daily_trade_date_ts_code_index on hq_daily ;
create index hq_daily_trade_date_ts_code_index
    on hq_stock_daily (ts_code,trade_date);

drop index  hq_daily_trade_date_index on hq_daily ;
create index hq_daily_trade_date_index
    on hq_stock_daily (trade_date);

drop index  hq_daily_basic_trade_date_ts_code_index on hq_daily_basic ;
create index hq_daily_basic_trade_date_ts_code_index
    on hq_stock_daily_basic (ts_code,trade_date);

drop index  hq_daily_basic_trade_date_index on hq_daily_basic ;
create index hq_daily_basic_trade_date_index
    on hq_stock_daily_basic (trade_date);

drop indexhq_adj_factor_ts_code_trade_date_index on hq_adj_factor
create index hq_adj_factor_ts_code_trade_date_index
    on hq_adj_factor (ts_code, trade_date);