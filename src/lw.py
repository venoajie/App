import pandas as pd
import yfinance as yf
import warnings 
import numpy as np
import mplfinance as mpf

warnings.filterwarnings("ignore")

pd.options.display.max_columns = None

#download the historical stock data
aapl_df = yf.download("AAPL", start="2018-03-24", end="2023-03-24")


"""
  calculate the short period and long period emas 
  the 20 and 50 here should be changed to whatever timeframes you wish to use
  the values are added into the same dataframe
"""
mpf.plot(aapl_df)