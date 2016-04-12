import numpy as np
import scipy as sc
import pandas as pd
from scipy.stats import norm

def IQR(data) :
    q25 = np.percentile(data, q=25)
    q75 = np.percentile(data, q=75)
    return abs(q75 - q25)

def scaling(data, win):
    return data / pd.rolling_apply(data, window=win, func=IQR)

def centering(data, win):
    return data - pd.rolling_median(data, win)


def normalization(data, win):
    delta = data - pd.rolling_median(data, win)
    iqr = pd.rolling_apply(data, win, IQR)
    v = 100 * norm.cdf(0.5*delta/iqr) - 50
    return v


def regular_normalization(data, win):
    min = pd.rolling_min(data, win)
    max = pd.rolling_max(data, win)
    return (data - min) / (max - min) * 100 - 50



from Data.StockDataManager import *
from Data.TimeSeries import *

if __name__  == "__main__" :
    tickers = ['GOOG/NYSE_SPY']

    settings = Settings()
    dp = TimeSeries(settings).get_agg_ETF_data(tickers)
    df = dp[:, :,'price']

    #df.plot(figsize=[20,12])
    n = df.copy()
    # n['scaling'] = scaling(df, 50)
    # n['centering'] = centering(df, 50)
    n['normalization'] = normalization(df, 50)
    n['regnormal'] = regular_normalization(df, 50)
    n.plot()
    print 'done!'

