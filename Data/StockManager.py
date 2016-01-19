import json

import blaze
import odo
import pandas as pd
import tushare as ts

import pymongo


class DBManager:
    def __init__(self):
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
        self._default_mongo_port = 27017

        self._default_mysql_uri = "mysql+mysqlconnector://darwin:darwinlab@188.166.179.144:3306/darwindb"
        self._default_mysql_host = '188.166.179.144'
        self._default_mysql_port = 3306

    def set_mysql_conn(self, hostname, username, password, db):
        self.mysql_hostname = hostname
        self.mysql_password = password
        self.mysql_username = username
        self.mysql_database = db

        self.mysql_uri = "mysql+mysqlconnector://" + self.mysql_username + ":" \
                         + self.mysql_password + "@" + self.mysql_hostname + ":" \
                         + str(self.mysql_port_default) + "/" + self.mysql_database
        return self.mysql_uri


    def get_default_mongo_conn(self):
        if self._default_mongo_conn == None:
            try:
                self._default_mongo_conn = pymongo.MongoClient(self._default_mongo_host, self._default_mongo_port)
                print 'successfully build the connection'
            except:
                print 'failed'

        return self._default_mongo_conn

    def get_default_mysql_conn(self):
        return self._default_mysql_uri




class StockManager:
    def __init__(self):
        self._db_manager = DBManager()
        self._default_mysql_stockinfo_table = "Stock_Info"
        self._default_mysql_stockprice_daily_table = "Stock_Price_Daily_tmp"


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

    def load_stock_info_db(self):
        stock_info_uri = self._db_manager.get_default_mysql_conn() + "::" + self._default_mysql_stockinfo_table
        stock_info = self.download_total_stock_info()

        bl_stock_info = blaze.Data(stock_info.reset_index())
        odo.odo(bl_stock_info, stock_info_uri)

    def load_stock_latest_price_db(self, codes):
        stock_price_uri = self._db_manager.get_default_mysql_conn() + "::" + self._default_mysql_stockprice_daily_table
        stock_price = self.download_stock_latest_price(codes)
        bl_stock_price = blaze.Data(stock_price.reset_index())
        odo.odo(bl_stock_price, stock_price_uri)

    def load_stock_hist_price_db(self, codes, start, end):
        stock_price_uri = self._db_manager.get_default_mysql_conn() + "::" + self._default_mysql_stockprice_daily_table
        stock_price = self.download_stock_hist_price(codes, start, end)
        bl_stock_price = blaze.Data(stock_price.reset_index())
        odo.odo(bl_stock_price, stock_price_uri)

    def get_all_stock_codes(self):
        stock_info_uri = self._db_manager.get_default_mysql_conn() + "::" + self._default_mysql_stockinfo_table
        pd_codes = odo.odo(blaze.Data(stock_info_uri).code, pd.DataFrame)
        return pd_codes



from bson.objectid import ObjectId

class BatchJobManager:
    def __init__(self):
        self._db_manager = DBManager()
        self._stock_manager = StockManager()

        self._mongo_conn = self._db_manager.get_default_mongo_conn()
        self._mongo_db_table = 'darwin_lab'
        self._mongo_coll_job_stockdaily_table = 'BatchJobs_Stock_Daily_Price'

        self._mongo_db =  self._mongo_conn[self._mongo_db_table]
        self._mongo_coll = self._mongo_db[self._mongo_coll_job_stockdaily_table]



    def add_job_download_all_stock_daily_price(self, start, end):
        codes = self._stock_manager.get_all_stock_codes()
        self.add_job_download_stock_daily_price(codes, start, end)

    def add_job_download_stock_daily_price(self, codes, start, end):

        jobs = pd.DataFrame()
        jobs['code'] = codes
        jobs['action'] = 'load'
        jobs['start'] = start
        jobs['end'] = end
        jobs['status'] = 0
        #jobs = jobs.set_index('code')
        records = json.loads(jobs.T.to_json()).values()
        self._mongo_coll.insert(records)
        print jobs

    def update_job_download_stock_daily_price(self, job_id, status):
        for job_id in job_id_list :
            self._mongo_coll.find_one_and_update({"_id":job_id}, {"$set": {"status": status}})



    def process_job_download_stock_daily_price(self):
        has_jobs = True
        while has_jobs :
            jobs = self._mongo_coll.find_one({'status': 0})
            if jobs.count() == 0:
                print "no more job to process!"
                return

            executed = 0
            for job in jobs :
                jobid = job["_id"]
                code = job["code"]
                start = job["start"]
                end = job["end"]

                try :
                    print "----------------------------\n"
                    print "loading " + code +"\n"
                    self._stock_manager.load_stock_hist_price_db(code, start, end)
                    print "success "
                    executed = 1
                    #self.update_job_download_stock_daily_price(jobid, 1)
                except :
                    print "failed "
                    #failure_jobids.append(jobid)
                    executed = 2
                    #self.update_job_download_stock_daily_price(jobid, 2)
            self.update_job_download_stock_daily_price(jobid, executed)


'''
jobmgr = BatchJobManager()
jobs = jobmgr.add_job_download_stock_daily_price(['000009'],'2015-12-31', '2016-01-19')
#jobmgr.process_job_download_stock_daily_price()
jobs = jobmgr._mongo_coll.find({'status':0})
for job in jobs :
    jobid = job["_id"]
    samejob = jobmgr._mongo_coll.find_one({'_id':jobid})
    print jobid
    if jobid == samejob["_id"] :
        print "find the same one"
        jobmgr._mongo_coll.find_one_and_update({'_id':jobid}, {'$set': {'status': 1}})
'''