import pandas as pd
import yfinance as yf
import warnings 
import numpy as np
import mplfinance as mpf

daily = pd.read_csv('SP500_NOV2019_Hist.csv',index_col=0,parse_dates=True)
daily.index.name = 'Date'
daily.shape
daily.head(3)
daily.tail(3)

"""
  calculate the short period and long period emas 
  the 20 and 50 here should be changed to whatever timeframes you wish to use
  the values are added into the same dataframe
"""
mpf.plot(daily)
mpf.show()