import sys, os
from Data.TimeSeries import *

import numpy as np
import zipline
from zipline.api import (add_history, history, set_slippage,
                         slippage, set_commission, commission,
                         order_target_percent)

from zipline import TradingAlgorithm



tickers = ['GOOG/NYSE_SPY', #S&P 500 ETF
           'GOOG/AMEX_EWJ', # iShares MSCI Japan ETF
           'GOOG/NYSE_IEV', # iShares Europe ETF
           'GOOG/NYSE_VWO', # Vanguard Emerging Market Stock ETF

           #'GOOG/NYSE_VNQ', # Vanguard MSCI US Reits
           'GOOG/NYSE_IYR', # iShares U.S. Real Estate ETF
           'GOOG/NYSE_RWX', # SPDR DJ Wilshire Intl Real Estate ETF

           'GOOG/NYSEARCA_TLT',  # 20 Years Treasury ETF
           'GOOG/NYSEARCA_TLH',  # 15-20 Years Treasury

           'GOOG/AMEX_GSG', # GSCI Commodity-Indexed Trust Fund
           'GOOG/NYSEARCA_GLD'  # SPDR Gold ETF

          ]

settings = Settings()
dp = TimeSeries(settings).get_agg_data(tickers)
dp = dp.fillna(method='pad', axis=0)
dp = dp.fillna(method='bfill', axis=0)
dp = dp[:,:,:]
dp[:, :,'price'].plot(figsize=[20,10])
df_rets = dp[:,:,'price'].pct_change().dropna()











from cvxpy import *

def minimize_vol(returns):
    n = len(returns)
    w  = Variable(n)

    gamma = Parameter(sign='positive')
    mu = returns.mean(axis=1)
    ret = np.array(mu)[np.newaxis] * w

    Sigma = np.cov(returns)
    risk = quad_form(w, Sigma)
    prob = Problem(Maximize(ret - 100*risk), [sum_entries(w)==1, w >=0])
    prob.solve()
    #print w.value.T * Sigma * w.value
    return np.asarray(w.value)

def initialize(context):
    add_history(120, '1d', 'price')
    set_slippage(slippage.FixedSlippage(spread=0.0))
    set_commission(commission.PerShare(cost=0.01, min_trade_cost=1.0))
    context.tick = 0

def handle_data(context, data):
    rebalance_period = 20

    context.tick += 1
    if context.tick < 120 :
        return
    if context.tick % rebalance_period != 0:
        return


    # Get rolling window of past prices and compute returns
    prices_6m = history(120, '1d', 'price').dropna()
    returns_6m = prices_6m.pct_change().dropna()
    prices_60d = history(60, '1d', 'price').dropna()
    returns_60d = prices_60d.pct_change().dropna()



    try:
        # Get the strongest 5 in momentum
        mom = returns_6m.T.sum(axis=1)
        selected_indices = mom[mom>0].order().tail(len(mom) /2).index
#         selected_indices = mom.index
#         selected_indices = mom[mom > 0 ].index
        selected_returns = returns_60d[selected_indices]

        weights = minimize_vol(selected_returns.T)
#         weights = minimize_vol(returns_60d.T)
        # Rebalance portfolio accordingly
        for stock, weight in zip(selected_returns.columns, weights):
            order_target_percent(stock, weight)
    except :
        # Sometimes this error is thrown
        # ValueError: Rank(A) < p or Rank([P; A; G]) < n
        pass


# Instantinate algorithm
algo = TradingAlgorithm(initialize=initialize,
                        handle_data=handle_data)
# Run algorithm
results = algo.run(dp.dropna())
ret_ports = pd.DataFrame()
ret_ports[5] = results.portfolio_value
ret_ports.plot(figsize=[20,10])

print results