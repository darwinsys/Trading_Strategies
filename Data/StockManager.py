import json

import blaze
import odo
import pandas as pd
import tushare as ts

import pymongo


class Settings:
    MAX_DOWNLOAD_TRIALS = 6
    _local_mongo_collection_batch_job_stock_daily_price = "BatchJobs_Stock_Price_Daily"
    _local_mongo_collection_stock_daily_price = "Stock_Price_Daily"
    _local_mongo_collection_stock_daily_price_tmp = "Stock_Price_Daily_tmp"
    _local_mongo_collection_stock_info = "Stock_info"

    _local_mongo_hostname = 'localhost'
    _local_mongo_port = 27017
    _local_mongo_conn_uri = 'mongodb://' + _local_mongo_hostname + ':' + str(_local_mongo_port)

    _local_mongo_db_name = "darwin_lab"

    _local_mongo_conn = None


    _remote_mongo_hostname = '188.166.179.144'
    _remote_mongo_port = 27017
    _remote_mongo_db_name = "darwin_lab"
    _remote_mongo_conn_uri = 'mongodb://' + _remote_mongo_hostname + ':' + str(_remote_mongo_port)

    _remote_mongo_conn = None

    def __init__(self):
        self._local_mongo_conn = self.get_local_mongo_conn()
        self._local_mongo_db = self.get_local_mongo_db()

    def get_local_mongo_conn(self):
        if self._local_mongo_conn is None:
            self._local_mongo_conn = pymongo.MongoClient(self._local_mongo_hostname, self._local_mongo_port)
        return self._local_mongo_conn

    def get_remote_mongo_conn(self):
        if self._remote_mongo_conn is None:
            self._remote_mongo_conn = pymongo.MongoClient(self._remote_mongo_hostname, self._remote_mongo_port)
        return self._remote_mongo_conn

    def get_local_mongo_db(self):
        self.get_local_mongo_conn()
        return self._local_mongo_conn[self._local_mongo_db_name]

    def get_remote_mongo_db(self):
        self.get_remote_mongo_conn()
        return self._remote_mongo_conn[self._remote_mongo_db_name]

    def get_mongo_db(self, name_db, mongo_conn=None):
        if mongo_conn is not None:
            return mongo_conn[name_db]
        else:
            return None

    ### functions to get local mongo collections
    def get_local_mongo_coll_job(self):
        return self.get_local_mongl_coll(self._local_mongo_collection_batch_job_stock_daily_price)

    def get_local_mongo_coll_price_tmp(self):
        return self.get_local_mongl_coll(self._local_mongo_collection_stock_daily_price_tmp)

    def get_local_mongo_coll_price(self):
        return self.get_local_mongl_coll(self._local_mongo_collection_stock_daily_price)

    def get_local_mongo_coll_info(self):
        return self.get_local_mongl_coll(self._local_mongo_collection_stock_info)

    def get_local_mongl_coll(self, name_coll):
        mongo_db = self.get_local_mongo_db()
        return mongo_db[name_coll]


class DBManager:
    def __init__(self, settings):
        self.mysql_db = None
        self.mysql_uri = None

        self.mysql_hostname = None
        self.mysql_username = None
        self.mysql_password = None
        self.mysql_port_default = 3306
        self.mysql_database = None

        self.mysqlb_uri = None
        self.mongo_db = None

        self._default_mongo_conn = None
        self._default_mongo_host = '188.166.179.144'
        self._default_mongo_hostname = 'localhost'
        self._default_mongo_port = 27017

        self._default_mongo_database = "darwin_lab"
        self._default_mongo_table_stock_daily_price = "Stock_Price_Daily"

        self._default_mysql_uri = "mysql+mysqlconnector://darwin:darwinlab@188.166.179.144:3306/darwindb"
        self._default_mysql_host = '188.166.179.144'
        self._default_mysql_port = 3306
        self._default_mysql_username = 'darwin'
        self._default_mysql_password = 'darwinlab'
        self._default_mysql_database = 'darwindb'

        self._local_mongo_host = 'localhost'
        self._local_mongo_port = 27017
        self._local_mongo_conn_uri = 'mongodb://' + self._local_mongo_host + ":" \
                                     + str(self._local_mongo_port)
        self._local_mongo_db = 'darwin_lab'
        self._local_mongo_db_uri = self._local_mongo_conn_uri + "/" + self._local_mongo_db

    def set_mysql_conn(self, hostname, username, password, db):
        self.mysql_hostname = hostname
        self.mysql_password = password
        self.mysql_username = username
        self.mysql_database = db

        self.mysql_uri = "mysql+mysqlconnector://" + self.mysql_username + ":" \
                         + self.mysql_password + "@" + self.mysql_hostname + ":" \
                         + str(self.mysql_port_default) + "/" + self.mysql_database
        return self.mysql_uri

    def get_default_mysql_uri(self):
        self.mysql_uri = "mysql+mysqlconnector://" + self._default_mysql_username + ":" \
                         + self._default_mysql_password + "@" + self._default_mysql_host + ":" \
                         + str(self._default_mysql_port) + "/" + self._default_mysql_database
        return self.mysql_uri

    def get_mongo_uri(self, hostname='localhost', port=27017, username=None, password=None, database=None,
                      collection=None):
        mongo_uri = "mongodb://"
        if username is not None and password is not None:
            mongo_uri = mongo_uri + username + ":" + password + "@"
        mongo_uri = mongo_uri + hostname + ":" + str(port)
        if database is not None:
            mongo_uri = mongo_uri + "/" + database
            if collection is not None:
                mongo_uri = mongo_uri + "::" + collection

        return mongo_uri

    def get_default_mongo_conn(self):
        if self._default_mongo_conn == None:
            try:
                self._default_mongo_conn = pymongo.MongoClient(self._default_mongo_host, self._default_mongo_port)
                print 'successfully build the connection'
            except:
                print 'failed'

        return self._default_mongo_conn


class StockManager:
    def __init__(self, settings):
        #self._db_manager = DBManager()
        # self._default_mysql_stockinfo_table = "Stock_Info"
        # self._default_mysql_stockprice_daily_table_tmp = "Stock_Price_Daily_tmp"
        # self._default_mysql_stockprice_daily_table = "Stock_Price_Daily"

        self._settings = settings
    #     self._mongo_conn = None
    #     self._mongo_db = None
    #     self._mongo_coll_price_daily = None
    #     self._mongo_coll_price_daily_tmp = None
    #
    # def init_mongo_db(self, mongo_conn):
    #     self._mongo_conn = mongo_conn
    #     self._mongo_db = self._settings.get_local_mongo_db()
    #     self._mongo_coll_price_daily = self._settings.get_local_mongo_coll_price()
    #     self._mongo_coll_price_daily_tmp = self._settings.get_local_mongo_coll_price_tmp()

    # downloading stock related data from Tushare
    def download_stock_info(self):
        stock_info_tmp = ts.get_stock_basics()
        stock_info_tmp = stock_info_tmp.reset_index()

        stock_info = stock_info_tmp[['code', 'name', 'industry', 'area', 'timeToMarket']]
        stock_info.columns = ['code', 'name', 'industry', 'listedLoc', 'listedDate']

        stock_info['listedDate'][stock_info['listedDate'] == 0] = 20160101
        # stock_info['listedDate'] = pd.to_datetime(stock_info['listedDate'], format='%Y%m%d')
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

    # downloading stock historical price
    def download_stock_hist_price(self, code, start, end=None):
        print "downloading " + code
        if end is None:
            price = ts.get_h_data(code, start)
        else:
            price = ts.get_h_data(code, start, end)
        return price

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
        # stock_info.head()
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
        stock_total_info = stock_total_info.reset_index()
        return stock_total_info

    # loading data into database
    # destionation: stock_info_uri can be either mysql connection or mongo db connection
    def load_stock_info_into_mongo_db(self):
        mongo_coll = self._settings.get_local_mongo_coll_info()
        stock_info = self.download_total_stock_info()
        records = json.loads(stock_info.T.to_json()).values()
        mongo_coll.insert(records)

    def load_stock_price_into_mongo_db(self, code, start, end):
        mongo_coll = self._settings.get_local_mongo_coll_price_tmp()
        stock_prices = self.download_stock_hist_price(code, start, end)
        stock_prices = stock_prices.reset_index()
        stock_prices['code'] = code
        # stock_prices['date'] = str(stock_prices['date'])
        # stock_prices = stock_prices.reset_ind`ex()
        print "loading " + code + " into db"
        records = json.loads(stock_prices.T.to_json()).values()
        mongo_coll.insert(records)
        print "success"

    # def load_stock_into_db(self, stock_info_uri):
    #     info_dshape = blaze.dshape("var * {'code':string, 'name':string, 'industry':string, "
    #                                "'listedDate':datetime, 'location':string, 'listedLoc':string, "
    #                                "'size':string}")
    #     stock_info = self.download_total_stock_info()
    #     odo.odo(blaze.Data(stock_info.reset_index(), info_dshape), stock_info_uri)

    # def load_stock_info_db(self):
    #     stock_info_uri = self._db_manager.get_default_mysql_conn() + "::" + self._default_mysql_stockinfo_table
    #     stock_info = self.download_total_stock_info()
    #
    #     bl_stock_info = blaze.Data(stock_info.reset_index())
    #     odo.odo(bl_stock_info, stock_info_uri)

    def get_all_stock_info(self, stock_info_uri):
        info_dshape = blaze.dshape("var * {'code':string, 'name':string, 'industry':string, "
                                   "'listedDate':datetime, 'location':string, 'listedLoc':string, "
                                   "'size':string}")

        pd_codes = odo.odo(blaze.Data(stock_info_uri, info_dshape), pd.DataFrame)
        return pd_codes

    def get_all_stock_info(self):
        mongo_coll = self._settings.get_local_mongo_coll_info()
        df_stock_info = pd.DataFrame(list(mongo_coll.find()))
        return df_stock_info

    # def load_stock_latest_price_db(self, codes):
    #     stock_price_uri = self._db_manager.get_default_mysql_conn() + \
    #                       "::" + self._default_mysql_stockprice_daily_table_tmp
    #     stock_price = self.download_stock_latest_price(codes)
    #     bl_stock_price = blaze.Data(stock_price.reset_index())
    #     odo.odo(bl_stock_price, stock_price_uri)

    # def load_stock_hist_price_db(self, codes, start, end):
    #     stock_price_uri = self._db_manager.get_default_mysql_conn() + \
    #                       "::" + self._default_mysql_stockprice_daily_table_tmp
    #     stock_price = self.download_stock_hist_price(codes, start, end)
    #     bl_stock_price = blaze.Data(stock_price.reset_index())
    #     odo.odo(bl_stock_price, stock_price_uri)



class BatchJobManager:
    def __init__(self, settings):
        self._settings = settings
        self._db_manager = DBManager(settings)
        self._stock_manager = StockManager(settings)


    def add_job_download_all_stock_daily_price(self, start, end):

        mongo_coll_stock_info = self._settings.get_local_mongo_coll_info()

        df_stock_info = self._stock_manager.get_all_stock_info()
        codes = df_stock_info['code']
        self.add_job_download_stock_daily_price(codes, start, end)

    def add_job_download_stock_daily_price(self, codes, start, end):
        mongo_coll_jobs = self._settings.get_local_mongo_coll_job()
        jobs = pd.DataFrame()
        jobs['code'] = codes
        jobs['action'] = 'load'
        jobs['start'] = start

        if end is None:
            jobs['end'] = None
        else:
            jobs['end'] = end
        jobs['status'] = 0
        # jobs = jobs.set_index('code')
        records = json.loads(jobs.T.to_json()).values()
        mongo_coll_jobs.insert(records)
        print jobs

    def update_job_download_stock_daily_price(self, job_id, status):
        mongo_coll_jobs = self._settings.get_local_mongo_coll_job()
        mongo_coll_jobs.find_one_and_update({"_id": job_id}, {"$set": {"status": status}})

    def restart_failed_job(self):
        count = 0
        while True:
            tuple = self._settings.get_local_mongo_coll_job().find_one_and_update({"status" : {"$gt":0}}, {"$set" : {"status" : 0}})
            count = count + 1
            print "restart " + str(count)
            if tuple is None:
                print "finish all the restart!"
                return


    ## Process Tasks
    def task_CopyStockDailyPrice(self, code):
        job_status = 1
        while True:
            tuple = self._settings.get_local_mongo_coll_price_tmp().find_one({'code': code})
            if tuple is None:
                print "no tuple exists"
                return job_status
            else:
                code = tuple["code"]
                date = tuple["date"]

                tuple_src = self._settings.get_local_mongo_coll_price().find_one({'code': code, 'date': date})
                if tuple_src is None:
                    print "insert tuple"
                    try:
                        self._settings.get_local_mongo_coll_price.insert_one(tuple)
                    except:
                        print "fail to insert"
                        job_status = 2

    def process_job_copy_stock_daily_price(self, mongo_coll_job, mongo_coll_price_tmp, mongo_coll_price_dest):
        pass

    def process_job_download_stock_daily_price(self):
        while True:
            # jobs = mongo_coll_job.find_one({'status': 0})
            jobs = self._settings.get_local_mongo_coll_job().find_one({'status': 0})
            if jobs == None:
                print "no more job to process!"
                return

            n_trials = 0
            while n_trials <= self._settings.MAX_DOWNLOAD_TRIALS:
                jobid = jobs["_id"]
                code = jobs["code"]
                start = jobs["start"]
                end = jobs["end"]

                try:
                    print "----------------------------\n"
                    print "loading " + code + "\n"
                    self._stock_manager.load_stock_price_into_mongo_db(code, start, end)
                    print "success "
                    self.update_job_download_stock_daily_price(jobid, -1)
                    break
                    # self._mongo_coll.find_one_and_update({"_id":jobid}, {"$set": {"status": 1}})
                except:
                    print "failed "
                    n_trials = n_trials + 1
                    self.update_job_download_stock_daily_price(jobid, n_trials)
                    continue
                    # self._mongo_coll.find_one_and_update({"_id":jobid}, {"$set": {"status": 2}})




class DataManager:
    def __init__(self, settings):
        self._settings = settings
        self._jobManager = BatchJobManager(settings)
        self._stockManager = self._jobManager._stock_manager

    # def test_download_price(self):
    #     codes = ['000001', '000002']
    #     fields = ['close', 'open']
    #     start = '2015-01-01'
    #     end = '2015-12-31'
    #
    #     jobmgr = self._jobManager
    #
    #     output = dict()
    #     stock_price_uri = jobmgr._db_manager.get_default_mysql_uri() + \
    #                       "::" + jobmgr._stock_manager._default_mysql_stockprice_daily_table
    #     print stock_price_uri
    #
    #     stock_price_blz = blaze.Data(stock_price_uri)
    #
    #     for field in fields:
    #         data = stock_price_blz[stock_price_blz['code'] in codes]
    #         pd_price = odo.odo(data, pd.DataFrame)

    # test_download_price()


    def download_and_store_stock_info(self):
        jobmgr = self._jobManager
        jobmgr._stock_manager.load_stock_info_into_mongo_db()

    # download_and_store_stock_info()
    def add_download_jobs(self, start, end=None):
        jobmgr = self._jobManager
        infos = jobmgr._stock_manager.get_all_stock_info()
        codes = infos['code']

        jobmgr.add_job_download_stock_daily_price(codes, start, end)

    def process_download_jobs(self):
        jobmgr = self._jobManager
        jobmgr.process_job_download_stock_daily_price()

    def get_data_from_mongo(self):
        jobmgr = self._jobManager
        mongo_coll_price = self._settings.get_local_mongo_coll_price()

        df_prices = pd.DataFrame(list(mongo_coll_price.find()))
        df_prices['date'] = pd.to_datetime(df_prices['date'] * 1000 * 1000)
        df_prices = df_prices.set_index('date')
        df_prices = df_prices.sort_index()
        return df_prices

