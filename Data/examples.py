from Data import DBManager
from Data import StockManager

### Test the connection
db = DBManager.dbManager()
hostname = "188.166.179.144"
mysql_uri = db.set_mysql_conn(hostname, "darwin", "darwinlab", "darwindb")

stock_mgr = StockManager.StockManager(mysql_uri)
var = stock_mgr.download_stock_info()

import pandas as pd
import odo
import blaze

stock_price_uri = mysql_uri + "::Stock_Price_Daily_tmp"
bl_stock_price = blaze.Data(stock_price_uri)
codes = ['000001']

df_stock_prices = pd.DataFrame()
for code in codes:
    print "loading " + code
    a = bl_stock_price[bl_stock_price['code'] == code]
    a = a[['date', 'close']]

    prices = odo.odo(a, pd.DataFrame)
    print prices.tail()
    prices.columns = ['date', code]
    # df_stock_prices = df_stock_prices.join(prices, how='outer')
