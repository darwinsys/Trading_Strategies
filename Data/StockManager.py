import json

import blaze
import odo
import pandas as pd
import tushare as ts

import DBManager


class StockManager:
    def __init__(self):
        pass
        # self.mysql_uri = mysql_uri



    # downloading stock related data from Tushare
    def download_stock_info(self):
        stock_info_tmp = ts.get_stock_basics()
        stock_info_tmp = stock_info_tmp.reset_index()

        stock_info = stock_info_tmp[['code', 'name', 'industry', 'area', 'timeToMarket']]
        stock_info.columns = ['code', 'name', 'industry', 'listedLoc', 'listedDate']
        return stock_info

    def download_stock_industry(self):
        stock_industry_tmp = ts.get_industry_classified()
        stock_industry = stock_industry_tmp[['code', 'c_name']]
        stock_industry.columns = ['code', 'industryName']
        return stock_industry

    def download_stock_concept(self):
        stock_concept_tmp = ts.get_concept_classified()
        stock_concept = stock_concept_tmp[['code', 'c_name']]
        stock_concept.columns = ['code', 'conceptName']
        return stock_concept

    def download_stock_area(self):
        stock_area_tmp = ts.get_area_classified()
        stock_area = stock_area_tmp[['code', 'area']]
        stock_area.columns = ['code', 'location']
        return stock_area

    def download_stock_hist_price(self, codes, start, end):
        prices = pd.DataFrame()
        for code in codes:
            print "downloading " + code
            price = ts.get_h_data(code, start, end)
            prices = pd.concat([prices, price])
        return prices

    def download_stock_latest_price(self, codes):
        prices = pd.DataFrame()
        for code in codes:
            print "downloading " + code
            price = ts.get_h_data(code)
            prices = pd.concat(prices, price)
        return prices


    # this functon will merge all the stock information toegether
    # and create a total stock info table
    def download_total_stock_info(self):
        ### download stock infomration
        print "downloading stock basic info:"
        stock_info = self.download_stock_info()
        # stock_industry = download_stock_industry()
        # stock_concept = download_stock_concept()
        print "downloading stock areas info:"
        stock_area = self.download_stock_area()

        ### download sme and gem stock categories
        print "downloading sme stocks:"
        sme_stocks = ts.get_sme_classified()
        print "downloading gem stocks:"
        gem_stocks = ts.get_gem_classified()

        ### Merge all the tables together
        stock_total_info = stock_info.set_index('code')
        # stock_total_info = stock_total_info.join(stock_industry.set_index('code'), how='left')
        stock_total_info = stock_total_info.join(stock_area.set_index('code'), how='left')

        ### include sme and gem categorization
        sme_stocks['size'] = 'sme'
        gem_stocks['size'] = 'gem'
        small_stocks = pd.concat([sme_stocks, gem_stocks])
        small_stocks = small_stocks[['code', 'size']]

        stock_total_info = stock_total_info.join(small_stocks.set_index('code'), how='left')
        stock_total_info['size'] = stock_total_info['size'].fillna('large')

        return stock_total_info

    def load_stockinfo_db(self, table_stock_info):
        stock_info_uri = self.mysql_uri + "::" + table_stock_info
        stock_info = self.get_total_stock_info()

        bl_stock_info = blaze.Data(stock_info.reset_index())
        odo.odo(bl_stock_info, stock_info_uri)

    def load_stock_latest_price_db(self, codes, table_stock_price):
        stock_price_uri = self.mysql_uri + "::" + table_stock_price
        stock_price = self.download_stock_latest_price(codes)
        bl_stock_price = blaze.Data(stock_price.reset_index())
        odo.odo(bl_stock_price, stock_price_uri)

    def load_stock_hist_price_db(self, codes, start, end, table_stock_price):
        stock_price_uri = self.mysql_uri + "::" + table_stock_price
        stock_price = self.download_stock_hist_price(codes, start, end)
        bl_stock_price = blaze.Data(stock_price.reset_index())
        odo.odo(bl_stock_price, stock_price_uri)

    def get_all_stock_codes(self, db_uri):
        stock_info_uri = db_uri + "::Stock_Info"
        pd_codes = odo.odo(blaze.Data(stock_info_uri).code, pd.DataFrame)
        return pd_codes

    def batchjob_load_all_stock_histprice_db(self, start, end, db_uri, log_dir):
        codes = self.get_all_stock_codes(db_uri)

        ## loading the task entris into mongo db
        mongo_db = DBManager.Manager().get_default_mongo_conn()

        ## batch job db in mongo


class BatchJobManager:
    def __init__(self, mongo_conn):
        self._mongo_db = 'darwin_lab'
        self._mongo_coll_stockdaily = 'Batchjobs_Stock_Daily_Price'

        self._mongo_conn = mongo_conn

    def get_stockprice_collection(self):
        db = self._mongo_conn[self._mongo_db]
        coll = db[self._mongo_coll_stockdaily]
        return db, coll

    def add_stockprice_job(self, codes, action, start, end):
        db, coll = self.get_stockprice_collection()

        jobs = pd.DataFrame(codes)
        jobs['action'] = 'load'
        jobs['start'] = start
        jobs['end'] = end
        jobs['status'] = 0

        records = json.loads(jobs.T.to_json()).values()
        coll.insert(records)

    def process_stockprice_job(self):
        db, coll = self.get_stockprice_collection()
        jobs = coll.find({'status': 0})
        return jobs
