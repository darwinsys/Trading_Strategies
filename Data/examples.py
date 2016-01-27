from Data import DBManager
from Data import StockManager

'''
### Test the connection
db = DBManager.dbManager()
hostname = "188.166.179.144"
mysql_uri = db.set_mysql_conn(hostname, "darwin", "darwinlab", "darwindb")

stock_mgr = StockManager.StockManager(mysql_uri)
var = stock_mgr.download_stock_info()


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
'''

db = DBManager.dbManager()
hostname = "188.166.179.144"
mysql_uri = db.set_mysql_conn(hostname, "darwin", "darwinlab", "darwindb")
print mysql_uri
codes = ['000024']  # , '000002']
stock_mgr = StockManager.StockManager(mysql_uri)
# stock_mgr.load_stock_hist_price_db(codes, '2010-01-01', '2016-01-01', 'Stock_Price_Daily_tmp')
