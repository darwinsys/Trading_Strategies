import pymysql
import StockDataManager as sdm
import sqlalchemy as sa
import pandas as pd

class TimeSeries :
    def __init__(self, settings):
        self._settings = settings

    def get_stock_series(self, codes, fields, start, end=None):
        tl_price = self._settings.get_mysql_table(self._settings._table_price)
        query = sa.select([tl_price]).where(tl_price.c.code.in_(codes))
        query = sa.select([tl_price]).where(tl_price.c.date > start)
        if end is not None:
            query = sa.select(query).where(tl_price.c.date < end)

        data = pd.read_sql(query, self._settings.get_mysql_engine())
        data = data.columns[fields]
        return data

## testing
if __name__ == '__main__' :
    settings = sdm.Settings()
    ts = TimeSeries(settings)

    data = ts.get_stock_series(['300382', '603008'], ['open', 'close'], start='2015-01-01')