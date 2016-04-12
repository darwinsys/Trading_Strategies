
from Data.TimeSeries import *

from Data.TimeSeries import *
import pandas as pd
import matplotlib

import cvxopt as opt
from cvxopt import blas, solvers

import numpy as np
import zipline
from zipline.api import (add_history, history, set_slippage,
                         slippage, set_commission, commission,
                         order_target_percent, symbol,symbols, record)


from zipline import TradingAlgorithm
from ETF.AAA import AAA
from ML.Features import *
from ML.Targets import *

import matplotlib.pyplot as plt

class TestNN(AAA) :

    params = {}
    train_win = 0
    nn_win = 0
    ml = 'SVM'
    atr = []
    enable_stoploss = False

    def set_params(self, t_win, n_win, ml='SVM', stoploss=False, rsi=False, vol=False) :
        self.train_win = t_win
        self.nn_win = n_win
        self.ml = ml
        self.atr_len = self.train_win
        self.enable_stoploss = stoploss
        self.enable_RSI = rsi
        self.enable_VOL = vol
        return self

    '''
    Using the last N days price directions as the features
    Target using the next day price direction
    '''
    def create_features(self, df, n = 5) :
        df_target = target_direction(df, 1)
        df_target.columns = ['target']

        list_df_features = []
        for i in xrange(n):
            list_df_features.append(direction(df, i+1))

        df_features = pd.DataFrame()
        for l in list_df_features:
            df_features = df_features.join(l, how='outer')

        if self.enable_RSI:
            df_features['RSI_3'] = ta.RSI(df.values.ravel(), n-1)
            # df_features['RSI_3'] = (ta.RSI(df.values.ravel(), n) > 50) * 1

        if self.enable_VOL:
            df_features['Std'] = pd.rolling_std(df, n-1)

        # adding the target
        df_features = df_features.join(df_target, how='outer')
        #df_features.dropna(inplace=True)

        return df_features.iloc[:, :-1], df_features.iloc[:, [-1]]

    def initialize(self, context):
        add_history(200, '1d', 'price')
        set_slippage(slippage.FixedSlippage(spread=0.0))
        set_commission(commission.PerShare(cost=0.01, min_trade_cost=1.0))
        context.tick = 0

        dp_data = self.data
        df_data = pd.DataFrame(index=dp_data.axes[1])
        df_data['close'] = dp_data[:, :, 'close']
        df_data['open'] = dp_data[:, :, 'open']
        df_data['high'] = dp_data[:, :, 'high']
        df_data['low'] = dp_data[:, :, 'low']
        df_data['volume'] = dp_data[:, :, 'volume']

        self.atr = atr_per_close(df_data, atrLen = self.atr_len)
        context.longstop = 0

    def handle_data(self, context, data):

        context.tick += 1
        total_window = self.train_win + self.nn_win + 1

        if context.tick < (total_window):
            return

        try :
#             print 'tick = {t}'.format(t = context.tick)
            price = history(total_window - 1, '1d', 'price').dropna()
            df_price = pd.DataFrame(data=price.values, index=price.index, columns=['close'])

            features, target = self.create_features(df_price, self.nn_win)
            features_insample = features.iloc[(self.nn_win -1):-1, :].values
            target_insample = target.iloc[(self.nn_win -1):-1, :].values.ravel()

            features_oosample = features.iloc[-1, :]
            features_oosample = features_oosample.values.reshape([1, len(features_oosample)])

            ATR = self.atr.loc[price.index[-1], :][0]

            symbol = price.columns[0]


            if self.enable_stoploss:
                if data[symbol].price < context.longstop:
                    print 'Stop Loss '
                    order_target_percent(symbol, 0.0)
                    context.longstop = 0.0
                    return

            if self.ml == 'SVM' :
                ### Training the SVM
                from sklearn import svm
                model_svm = svm.SVC()
                model_svm.fit(features_insample, target_insample)

                preds_svm = model_svm.predict(features_oosample)[0]
                if preds_svm < 0.5:
                    #print "Sell "
                    order_target_percent(symbol, 0.0)
                    context.longstop = 0.0

                else :
                    #print "Buy"
                    order_target_percent(symbol, 1.0)
                    context.longstop = max(context.longstop, data[symbol].price * (1 - 0.7*ATR))
                    print "target sl = {n}".format(n=context.longstop)

            if self.ml == 'KNN' :
                ### Training the SVM
                from sklearn import neighbors
                k = 10

                model_knn = neighbors.KNeighborsClassifier(k, 'distance')
                model_knn.fit(features_insample, target_insample)

                preds_knn = model_knn.predict(features_oosample)[0]

                if preds_knn < 0.5:
                    #print "Sell "
                    order_target_percent(symbol, 0.0)
                else :
                    #print "Buy"
                    order_target_percent(symbol, 1.0)

            record('price', data[symbol]['price'])
        except :
            pass


if __name__  == "__main__" :
    tickers = ['GOOG/NYSE_SPY']

    settings = Settings()
    dp = TimeSeries(settings).get_agg_ETF_data(tickers)
    dp = dp.fillna(method='pad', axis=0)
    dp = dp.fillna(method='bfill', axis=0)
    dp = dp[:,'2010-01-01'::,:]
    dp = dp.dropna()

    #dp1 = dp.reindex_axis(['open_price', 'high', 'low', 'close_price', 'volume', 'price'], axis=2)


    rets = pd.DataFrame()
    #nn = [2, 5, 10, 20, 50]
    #nn = [50, 100]
    nn = [10]
    for n in nn:
        print "running {n}".format(n=n)
        rets['nn={n}, with rsi, vol'.format(n=n)] = TestNN(dp).set_params(30, n, ml='KNN', rsi=True, vol=True).run_trading().portfolio_value
        rets['nn={n}, with rsi, w/o vol'.format(n=n)] = TestNN(dp).set_params(30, n, ml='KNN', rsi=True, vol=False).run_trading().portfolio_value
        rets.plot(figsize=[20,12])

    print 'done!'