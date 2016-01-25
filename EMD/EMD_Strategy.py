import odo.odo
import blaze
import pandas as pd

'''
from Data import StockManager

dbmgr = StockManager.DBManager()

mongo_uri = dbmgr.get_mongo_uri(database=dbmgr._default_mongo_database,
                                collection=dbmgr._default_mongo_table_stock_daily_price)

'''


quandl_code = 'AuFngLLqDpLf672K9W85'

import Quandl
shanghai_gold = Quandl.get('CHRIS/SHFE_AU5', authtoken=quandl_code)
price = shanghai_gold['Settle']

import ptsa.emd as emd
import numpy
imfs = emd.emd(price[1:500])