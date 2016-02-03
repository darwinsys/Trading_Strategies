import json

import blaze
import odo
import pandas as pd
import tushare as ts

import pymongo
from sqlalchemy import *


class Settings :
    MAX_DOWNLOAD_TRIALS = 6

    _local_db = False

    if _local_db == True:
        _mysql_hostname = 'localhost'
        _mongo_hostname = 'localhost'
    else :
        _mysql_hostname = '188.166.179.144'
        _mongo_hostname = '188.166.179.144'

    _mysql_username = 'darwin'
    _mysql_password = 'darwinlab'
    _mysql_database = 'darwindb'
    _mysql_url = "mysql+pymysql://{user}:{passwd}@{host}/{db}".format(user=_mysql_username, passwd=_mysql_password,
            host=_mysql_hostname, db=_mysql_database)

    _mysql_engine = None
    _mysql_conn = None
    _mysql_metadata = None

    _table_price_tmp = 'Stock_Price_Daily_tmp'
    _table_price = 'Stock_Price_Daily'
    _table_info = 'Stock_Info'

    _mongo_collection_batch_job_stock_daily_price = "BatchJobs_Stock_Price_Daily"
    _mongo_collection_stock_daily_price = "Stock_Price_Daily"
    _mongo_collection_stock_daily_price_tmp = "Stock_Price_Daily_tmp"
    _mongo_collection_stock_info = "Stock_info"

    _mongo_port = 27017
    _mongo_conn_uri = 'mongodb://' + _mongo_hostname + ':' + str(_mongo_port)

    _mongo_db_name = "darwin_lab"

    _mongo_conn = None

    def __init__(self):
        self._mongo_conn = self.get_mongo_conn()
        self._local_mongo_db = self.get_mongo_db()

    def get_mongo_conn(self):
        if self._mongo_conn is None:
            self._mongo_conn = pymongo.MongoClient(self._mongo_hostname, self._mongo_port)
        return self._mongo_conn

    def get_mongo_db(self):
        self.get_mongo_conn()
        return self._mongo_conn[self._mongo_db_name]

    def get_mysql_engine(self):
        if self._mysql_engine is None :
            self._mysql_engine = sa.create_engine(Settings._mysql_url, echo=True)
        return self._mysql_engine

    def get_mysql_conn(self):
        self.get_mysql_engine()
        if self._mysql_conn is None:
            self._mysql_conn = self._mysql_engine.connect()
        return self._mysql_conn

    def get_mysql_metadata(self):
        if self._mysql_metadata is None:
            self._mysql_metadata = MetaData(bind=self.get_mysql_engine(), reflect=True)
        return self._mysql_metadata

    def get_mysql_table(self, table_name):
        table = Table(table_name, self.get_mysql_metadata(), autoload=True, autoload_with=self.get_mysql_engine() )
        return table

    ### functions to get local mongo collections
    def get_mongo_coll_job(self):
        return self.get_mongl_coll(Settings()._mongo_collection_batch_job_stock_daily_price)

    def get_mongo_coll_price_tmp(self):
        return self.get_mongl_coll(Settings()._mongo_collection_stock_daily_price_tmp)

    def get_mongo_coll_price(self):
        return self.get_mongl_coll(Settings()._mongo_collection_stock_daily_price)

    def get_mongo_coll_info(self):
        return self.get_mongl_coll(Settings()._mongo_collection_stock_info)

    def get_mongl_coll(self, name_coll):
        mongo_db = self.get_mongo_db()
        return mongo_db[name_coll]



class DownloadManager:
    def __init__(self):
        pass

    # downloading stock historical price
    def download_stock_hist_price(self, code, start, end=None):
        print "downloading " + code
        if end is None:
            price = ts.get_h_data(code, start)
        else:
            price = ts.get_h_data(code, start, end)
        return price


class TaskManager :
    _downloader = None

    def __init__(self):
        self._downloader = DownloadManager()

    def load_stock_price_into_db(self, code, start, end=None):
        df_prices = self._downloader.download_stock_hist_price(code, start, end)
        df_prices = df_prices.reset_index()
        df_prices['code'] = code
        df_prices['date_num'] = pd.to_numeric(df_prices['date'])
        df_prices['key'] = df_prices['code'] + '_' + df_prices['date_num'].astype(str)



        df_1 = df_prices[['key', 'code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
        print 'loading {code} with {num} rows into db'.format(code=code, num=str(len(df_1['key'])))

        try :
            odo.odo(blaze.Data(df_1), Settings._mysql_url + '::' + Settings._table_price_tmp)
        except :
            print 'failed'


    def sync_stock_price(self):
        '''
        function: copy the downloaded daily prices from the temporary table to the final table
        :return:
        '''

        # step 1: find the duplicate entries in the final table, and delete them
        table_temp = Settings.get_mysql_table(Settings._table_price_tmp)
        table_final = Settings.get_mysql_table(Settings._table_price)



class JobManager :
    def __init__(self):
        pass


    def process_job_download_stock_daily_price(self):
        while True:
            # jobs = mongo_coll_job.find_one({'status': 0})
            jobs = Settings().get_mongo_coll_job().find_one({'status': 0})
            if jobs == None:
                print "no more job to process!"
                return

            n_trials = 0
            while n_trials <= Settings().MAX_DOWNLOAD_TRIALS:
                jobid = jobs["_id"]
                code = jobs["code"]
                start = jobs["start"]
                end = jobs["end"]

                try:
                    print "----------------------------\n"
                    print "loading " + code + "\n"
                    TaskManager().load_stock_price_into_db(code, start, end)
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

    def update_job_download_stock_daily_price(self, job_id, status):
        mongo_coll_jobs = Settings().get_mongo_coll_job()
        mongo_coll_jobs.find_one_and_update({"_id": job_id}, {"$set": {"status": status}})

    def get_all_stock_info(self):
        mongo_coll = Settings().get_mongo_coll_info()
        df_stock_info = pd.DataFrame(list(mongo_coll.find()))
        return df_stock_info

    # download_and_store_stock_info()
    def add_download_jobs(self, start, end=None):
        infos = self.get_all_stock_info()
        codes = infos['code']

        self.add_job_download_stock_daily_price(codes, start, end)

    def add_job_download_stock_daily_price(self, codes, start, end):
        mongo_coll_jobs = Settings().get_mongo_coll_job()
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



    def get_data_from_mongo(self):
        mongo_coll_price = Settings().get_mongo_coll_price()

        df_prices = pd.DataFrame(list(mongo_coll_price.find()))
        df_prices['date'] = pd.to_datetime(df_prices['date'] * 1000 * 1000)
        df_prices = df_prices.set_index('date')
        df_prices = df_prices.sort_index()
        return df_prices

    def update_latest_stock_price(self):
        # todo
        pass



## testing
if __name__ == '__main__' :
    jobmgr = JobManager()
    #jobmgr.add_download_jobs('2015-01-01')
    jobmgr.process_job_download_stock_daily_price()