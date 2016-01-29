from StockManager import *
import odo.odo as odo
import pandas as pd
import blaze


datamgr = DataManager(Settings())
#datamgr.add_download_jobs('2013-01-01')

# datamgr._jobManager.restart_failed_job()
datamgr.process_download_jobs()

#jobmgr = BatchJobManager()
#jobmgr.task_CopyStockDailyPrice('000001')



#download_and_store_stock_info()
# jobmgr.add_job_download_all_stock_daily_price('2014-01-01')
# jobmgr.process_job_download_stock_daily_price()


#add_download_jobs('2014-01-01', '2016-01-30')
#jobmgr.init_mongo_db()


## add download jobs for all the codes
#jobmgr.add_job_download_all_stock_daily_price('2014-01-01', '2016-01-30', mongo_coll_jobs)


#jobmgr.add_job_download_all_stock_daily_price('2014-01-01', '2016-01-30')
#jobs = jobmgr.add_job_download_stock_daily_price(['000009'],'2015-12-31', '2016-01-19')
#jobmgr.process_job_download_stock_daily_price()
#jobs = jobmgr._mongo_coll.find({'status':0})
#for job in jobs :
#    jobid = job["_id"]
    #samejob = jobmgr._mongo_coll.find_one({'_id':jobid})
#    print jobid
    #if jobid == samejob["_id"]:
    #    print "find the same one"
    #    jobmgr._mongo_coll.find_one_and_update({'_id':jobid}, {'$set': {'status': 1}})