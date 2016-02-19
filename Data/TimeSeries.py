import pymysql
import StockDataManager as sdm
import sqlalchemy as sa
from sqlalchemy import select
import pandas as pd
import datetime

class TimeSeries :
    def __init__(self, settings):
        self._settings = settings

    def get_stock_series(self, codes, start, end=None, fields=None):
        tl_price = self._settings.get_mysql_table(self._settings._table_price)
        if end is None:
            end = str(datetime.date.today())

        query = select([tl_price]). \
            where(tl_price.c.code.in_(codes)).  \
            where(tl_price.c.date >= start). \
            where(tl_price.c.date <= end)
        data = pd.read_sql(query, self._settings.get_mysql_engine())
        data.set_index('date', inplace=True)



        ### reorder the dataframe
        ### using a dict as a container. Each field will be a separate dataframe stored in the dict.
        ### the key will be the field name
        di_data = dict()
        for field in fields :
            df_field = pd.DataFrame()
            for code in codes:
                df_one = data[data['code'] == code]
                df_two = df_one[[field]]
                df_two.columns = [code]
                df_field = df_field.join(df_two, how='outer')
            di_data[field] = df_field

        return di_data

    def get_equity_market_values(self, field = None, tickers = None, start = None, end = None):
        mongo_coll_eqMkt = self._settings.get_mongo_coll_equity_market()
        df_mkt = pd.DataFrame()
        if tickers is not None:
            for ticker in tickers :
                query = mongo_coll_eqMkt.find({'ticker': ticker})
                df_fields = pd.DataFrame(list(query))


        return df_mkt

def get_data_from_mongo(self):
        mongo_coll_price = self._settings.get_mongo_coll_price()

        df_prices = pd.DataFrame(list(mongo_coll_price.find()))
        df_prices['date'] = pd.to_datetime(df_prices['date'] * 1000 * 1000)
        df_prices = df_prices.set_index('date')
        df_prices = df_prices.sort_index()
        return df_prices



## testing
if __name__ == '__main__' :
    settings = sdm.Settings()
    ts = TimeSeries(settings)

    #data = ts.get_stock_series(['300382', '603008'], start='2015-01-01', fields=['open', 'close'])
    #data1 = ts.get_stock_series(['300382', '603008'], start='2015-01-01')
    data = ts.get_equity_market_values()