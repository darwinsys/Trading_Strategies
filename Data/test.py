import StockManager as sm
import odo.odo as odo
import pandas as pd
import blaze


def test_download_price():
    codes = ['000001', '000002']
    fields = ['close', 'open']
    start = '2015-01-01'
    end = '2015-12-31'

    ###
    jobmgr = sm.BatchJobManager()

    output = dict()
    stock_price_uri = jobmgr._db_manager.get_default_mysql_uri() + \
                      "::" + jobmgr._stock_manager._default_mysql_stockprice_daily_table
    print stock_price_uri

    stock_price_blz = blaze.Data(stock_price_uri)

    for field in fields:
        data = stock_price_blz[stock_price_blz['code'] in codes]
        pd_price = odo(data, pd.DataFrame)


test_download_price()
