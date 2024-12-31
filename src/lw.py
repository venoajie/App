import pandas as pd
import yfinance as yf
import warnings 
import numpy as np
import mplfinance as mpf

# obtain the Bitcoin ticker in USD
bitCoinUSD = yf.Ticker("BTC-USD")
# save the historical market data to a dataframe
bitCoinUSD_values = bitCoinUSD.history(start="2024-12-21")

# now plot. For plotting styles see: https://github.com/matplotlib/mplfinance/blob/master/examples/styles.ipynb
mpf.plot(bitCoinUSD_values,type='candle',volume=True,figratio=(3,1),style='yahoo', title='Bitcoin (USD) from: 21 September 2021');
"""
  calculate the short period and long period emas 
  the 20 and 50 here should be changed to whatever timeframes you wish to use
  the values are added into the same dataframe
"""