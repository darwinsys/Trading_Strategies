import json

import blaze
import odo
import pandas as pd
import tushare as ts

import pymongo
from sqlalchemy import *
import pymysql

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

    MONGO_COLL_Job_Daily = "Job_Daily"
    _mongo_collection_stock_daily_price = "Stock_Price_Daily"
    _mongo_collection_stock_daily_price_tmp = "Stock_Price_Daily_tmp"
    _mongo_collection_stock_info = "Stock_info"

    MONGO_COLL_Rawdata_Equity_Fundamental_IS = "RAW_Equity_IncomeStatment"
    MONGO_COLL_Rawdata_Equity_Market = "RAW_Equity_Market"

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
            self._mysql_engine = create_engine(Settings._mysql_url, echo=True)
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
        return self.get_mongl_coll(self.MONGO_COLL_Job_Daily)

    def get_mongo_coll_price_tmp(self):
        return self.get_mongl_coll(self._mongo_collection_stock_daily_price_tmp)

    def get_mongo_coll_price(self):
        return self.get_mongl_coll(self._mongo_collection_stock_daily_price)

    def get_mongo_coll_info(self):
        return self.get_mongl_coll(self._mongo_collection_stock_info)

    ### mongo collections for equity fundamental
    def get_mongo_coll_equity_funda_is(self):
        return self.get_mongl_coll(self.MONGO_COLL_Rawdata_Equity_Fundamental_IS)

    def get_mongo_coll_mkt_eq(self):
        return self.get_mongl_coll(self.MONGO_COLL_Rawdata_Equity_Market)






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
    _settings = None

    def __init__(self, settings):
        self._downloader = DownloadManager()
        self._settings = settings

    def load_stock_price_into_db(self, code, start, end=None):
        df_prices = self._downloader.download_stock_hist_price(code, start, end)
        df_prices = df_prices.reset_index()
        df_prices['code'] = code
        df_prices['date_num'] = pd.to_numeric(df_prices['date'])
        df_prices['key'] = df_prices['code'] + '_' + df_prices['date_num'].astype(str)

        df_1 = df_prices[['key', 'code', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]

        ### delete all the duplicated keys in the original database
        max_key = max(df_1['key'])
        min_key = min(df_1['key'])

        db_conn = self._settings.get_mysql_conn()
        tl_price = self._settings.get_mysql_table(self._settings._table_price)
        qr_delete = tl_price.delete().where(tl_price.c.key <= max_key).where(tl_price.c.key >= min_key)
        result = db_conn.execute(qr_delete)

        try :
            df_1.to_sql(Settings._table_price, db_conn, chunksize=1000, index=False, if_exists='append' )
        except pymysql.err.IntegrityError :
            print 'Duplicate Entry: Fail to insert'

        #db_conn.close()
        ### load the data into the
        #tl_price_tmp = self._settings.get_mysql_table(self._settings._table_price_tmp)
        #qr_insert = tl_price.insert()
        #result = db_conn.execute(qr_insert, df_1.to_records(index=False,convert_datetime64= False ))
        #odo.odo(blaze.Data(df_1), Settings._mysql_url + '::' + Settings._table_price)


import factors
from datetime import datetime

class JobManager :
    _settings = None
    _taskmanager = None
    _factorfactory = None

    JOB_STATUS_READY = 'ready'
    JOB_STATUS_FAILED = 'failed'
    JOB_STATUS_SUCCESS = 'success'

    TASK_DOWNLOAD_MARKET_EQUITY = 'download_market_equity'
    TASK_DOWNLOAD_MARKET_EQUITY_BYDATE = 'download_market_equity_dydate'
    TASK_DOWNLOAD_EQUITY_FACTOR_BYDATE = 'download_equity_factor_dydate'

    def __init__(self, settings):
        self._settings = settings
        self._taskmanager = TaskManager(settings)
        self._factorfactory = factors.FactorFactory()

    def processJob_DownloadEquityMktByDate(self):
        while True:
            jobs = self._settings.get_mongo_coll_job().find_one({'status':self.JOB_STATUS_READY, \
                'task':self.TASK_DOWNLOAD_MARKET_EQUITY_BYDATE})
            if jobs is None:
                print 'No more job to process'
                return

            n_trials = 0
            while n_trials <= self._settings.MAX_DOWNLOAD_TRIALS:
                jobid = jobs['_id']
                tradeDate = datetime.strptime(jobs['tradeDate'], '%Y-%m-%d').strftime('%Y%m%d')

                params = {}
                params['tradeDate'] = tradeDate
                try :
                    print "-------------------------------\n"
                    print "Downloading Market Overview for the equity market for date:{tradeDate}".format(tradeDate=tradeDate)
                    df_mk = self._factorfactory.getMarketEquity(params)
                    print 'Uploading to Mongo Server\n'
                    records = json.loads(df_mk.T.to_json()).values()
                    self._settings.get_mongo_coll_mkt_eq().insert(records)

                    print "Success"
                    self.update_job_status(jobid, self.JOB_STATUS_SUCCESS)
                    break
                except:
                    print "Failed"
                    n_trials = n_trials + 1
                    self.update_job_retry(jobid, n_trials)
                    continue
            self.update_job_status(jobid, self.JOB_STATUS_FAILED)
        pass

    def processJob_DownloadStockFactorByDate(self):
        while True:
            jobs = self._settings.get_mongo_coll_job().find_one({'status':self.JOB_STATUS_READY, \
                'task':self.TASK_DOWNLOAD_EQUITY_FACTOR_BYDATE})
            if jobs is None:
                print 'No more job to process'
                return

            n_trials = 0
            while n_trials <= self._settings.MAX_DOWNLOAD_TRIALS:
                jobid = jobs['_id']
                tradeDate = datetime.strptime(jobs['tradeDate'], '%Y-%m-%d').strftime('%Y%m%d')

                params = {}
                params['tradeDate'] = tradeDate
                try :
                    print "-------------------------------\n"
                    print "Downloading Stock Factors for date:{tradeDate}".format(tradeDate=tradeDate)
                    df_mk = self._factorfactory.getStockFactors(params)
                    print 'Uploading to Mongo Server\n'
                    records = json.loads(df_mk.T.to_json()).values()
                    self._settings.get_mongo_coll_mkt_eq().insert(records)

                    print "Success"
                    self.update_job_status(jobid, self.JOB_STATUS_SUCCESS)
                    break
                except:
                    print "Failed"
                    n_trials = n_trials + 1
                    self.update_job_retry(jobid, n_trials)
                    continue
            self.update_job_status(jobid, self.JOB_STATUS_FAILED)
        pass


    def processJob_Fundamental_Equity_IS(self):
        while True:
            jobs = self._settings.get_mongo_coll_job().find_one({'action':'EquityIS', 'status':0})
            if jobs is None:
                print "no more job to process"
                return

            n_trials = 0
            while n_trials <= self._settings.MAX_DOWNLOAD_TRIALS:
                jobid = jobs['_id']
                code = jobs['code']
                start = jobs['start']
                end = jobs['end']

                try :
                    print "----------------------------\n"
                    print "Downloading Equity Income Statement for  " + code + "\n"
                    self.task_Fundamental_Equity_IS(code, start, end)

                    print "success "
                    self.update_job_status(jobid, -1)
                    break
                    # self._mongo_coll.find_one_and_update({"_id":jobid}, {"$set": {"status": 1}})
                except:
                    print "failed "
                    n_trials = n_trials + 1
                    self.update_job_status(jobid, n_trials)
                    continue
                    # self._mongo_coll.find_one_and_update({"_id":jobid}, {"$set": {"status": 2}})




    def task_Fundamental_Equity_IS(self, code, start, end):
        try :
            print '---------------------------------------\n'
            print 'Downloading Equity Income Statement for {code}\n'.format(code=code)
            params = {}
            params['ticker'] = code

            params['beginDate'] = datetime.strptime(start, '%Y%m%d')
            params['endDate'] = datetime.strftime(end, '%Y%m%d')
            df_is = self._factorfactory.getData(self._factorfactory.form_funda_cf, params)

            if df_is is None:
                print 'Failed to download\n'
                return
            df_is['ticker'] = code

            print 'Uploading to Mongo Server\n'
            records = json.loads(df_is.T.to_json()).values()
            self._settings.get_mongo_coll_equity_funda_is().insert(records)

            print 'Success'
        except Exception, e:
            print 'Failed'
            raise e

    def process_job_download_stock_daily_price(self):
        while True:
            # jobs = mongo_coll_job.find_one({'status': 0})
            jobs = self._settings.get_mongo_coll_job().find_one({'$and'[{'action':'load'}, {'status': 0 }]})
            if jobs is None:
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
                    self._taskmanager.load_stock_price_into_db(code, start, end)
                    print "success "
                    self.update_job_status(jobid, -1)
                    break
                    # self._mongo_coll.find_one_and_update({"_id":jobid}, {"$set": {"status": 1}})
                except:
                    print "failed "
                    n_trials = n_trials + 1
                    self.update_job_status(jobid, n_trials)
                    continue
                    # self._mongo_coll.find_one_and_update({"_id":jobid}, {"$set": {"status": 2}})

    def update_job_status(self, job_id, status):
        mongo_coll_jobs = self._settings.get_mongo_coll_job()
        mongo_coll_jobs.find_one_and_update({"_id": job_id}, {"$set": {"status": status}})

    def update_job_retry(self, job_id, retries):
        mongo_coll_jobs = self._settings.get_mongo_coll_job()
        mongo_coll_jobs.find_one_and_update({"_id": job_id}, {"$set": {"retry": retries}})

    def get_all_stock_info(self):
        mongo_coll = self._settings.get_mongo_coll_info()
        df_stock_info = pd.DataFrame(list(mongo_coll.find()))
        return df_stock_info

    # download_and_store_stock_info()
    def add_download_jobs(self, start, end=None):
        infos = self.get_all_stock_info()
        codes = infos['code']

        self.add_download_job(codes, start, end)

    def add_download_job(self, codes, start, end):
        mongo_coll_jobs = self._settings.get_mongo_coll_job()
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

    def addJob_Fundamental_Equity_IS(self, codes=None, start=None, end=None):
        mongo_coll_jobs = self._settings.get_mongo_coll_job()
        jobs = pd.DataFrame()

        if codes is None:
            infos = self.get_all_stock_info()
            codes = infos['code']

        jobs['code'] = codes
        jobs['action'] = 'EquityIS'
        jobs['start'] = start
        jobs['end'] = end

        jobs['status'] = 0

        records = json.loads(jobs.T.to_json()).values()
        mongo_coll_jobs.insert(records)


    def addJob_DownloadEquityMktByDate(self, start=None, end=None):
        mongo_coll_jobs = self._settings.get_mongo_coll_job()
        jobs = pd.DataFrame()

        if end is None:
            end = datetime.today().strftime('%Y%m%d')

        if start is None:
            jobs['tradeDate'] = {end}
        else :
            days = self._factorfactory.getTradingDays(start, end)
            jobs['tradeDate'] = days.iloc[:, 0]

        jobs['task'] = self.TASK_DOWNLOAD_MARKET_EQUITY_BYDATE
        jobs['status'] = self.JOB_STATUS_READY
        jobs['retry'] = 0

        records = json.loads(jobs.T.to_json()).values()
        mongo_coll_jobs.insert(records)

    def addJob_DownloadStockFactorByDate(self, start=None, end=None):
        mongo_coll_jobs = self._settings.get_mongo_coll_job()
        jobs = pd.DataFrame()

        if end is None:
            end = datetime.today().strftime('%Y%m%d')

        if start is None:
            jobs['tradeDate'] = {end}
        else :
            days = self._factorfactory.getTradingDays(start, end)
            jobs['tradeDate'] = days.iloc[:, 0]

        jobs['task'] = self.TASK_DOWNLOAD_EQUITY_FACTOR_BYDATE
        jobs['status'] = self.JOB_STATUS_READY
        jobs['retry'] = 0

        records = json.loads(jobs.T.to_json()).values()
        mongo_coll_jobs.insert(records)

    def get_data_from_mongo(self):
        mongo_coll_price = self._settings.get_mongo_coll_price()

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
    settings = Settings()
    jobmgr = JobManager(settings)
    #jobmgr.add_download_jobs('2016-01-01')
    #jobmgr.process_job_download_stock_daily_price()

    # Test case 2:
    #jobmgr.addJob_Fundamental_Equity_IS()
    #jobmgr.processJob_Fundamental_Equity_IS()

    # Test case 3:
    #jobmgr.addJob_DownloadEquityMktByDate('20080101')
    #jobmgr.processJob_DownloadEquityMktByDate()

    # Test case 4:
    #jobmgr.addJob_DownloadStockFactorByDate('20080101')
    jobmgr.processJob_DownloadStockFactorByDate()
