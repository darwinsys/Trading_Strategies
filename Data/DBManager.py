# importing
import blaze
import pymongo


class dbManager:
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

    def set_mysql_conn(self, hostname, username, password, db):
        self.mysql_hostname = hostname
        self.mysql_password = password
        self.mysql_username = username
        self.mysql_database = db

        self.mysql_uri = "mysql+mysqlconnector://" + self.mysql_username + ":" \
                         + self.mysql_password + "@" + self.mysql_hostname + ":" \
                         + str(self.mysql_port_default) + "/" + self.mysql_database
        return self.mysql_uri

    def get_mysql_conn(self, hostname, username, password, db):
        # init DB
        print mysql_uri
        self.mysql_db = blaze.Data(mysql_uri)
        return mysql_uri, self.mysql_db

    def get_mongo_conn(self, mongo_host, mongo_port):
        try:
            mongo_db = pymongo.MongoClient(mongo_host, mongo_port)
            print 'success'
        except:
            print 'failed'
        return mongo_db
