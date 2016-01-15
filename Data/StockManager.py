import blaze
import odo
import tushare as ts


class StockManager:
    def __init__(self, mysql_uri):
        self.mysql_uri = mysql_uri

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

    # this functon will merge all the stock information toegether
    # and create a total stock info table
    def get_total_stock_info(self):
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

    def get_stock_daily_prices(self, codes, field):
        stock_price_uri = self.mysql_uri + "::Stock_Price_Daily_tmp"
        bl_stock_price = blaze.Data(stock_price_uri)

        for code in codes:
            bl_stock_price[bl_stock_price['code'] == code]
