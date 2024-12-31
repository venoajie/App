import pandas as pd
import yfinance as yf
import warnings 
import numpy as np
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")

pd.options.display.max_columns = None

#download the historical stock data
aapl_df = yf.download("AAPL", start="2018-03-24", end="2023-03-24")


"""
  calculate the short period and long period emas 
  the 20 and 50 here should be changed to whatever timeframes you wish to use
  the values are added into the same dataframe
"""
aapl_df['ema_short'] = aapl_df['Close'].ewm(span=20, adjust=False).mean()
aapl_df['ema_long'] = aapl_df['Close'].ewm(span=50, adjust=False).mean()


"""
  New column 'bullish' will hold a value of 1.0 when the ema_short > ema_long, and a value of 0.0 when ema_short < ema_long. 
  'crossover' will tell us when the crossover actually happened - when the ema_short crossed above or below the ema_long.
  'crossover' column will hold 1.0 on a cross above, and -1.0 on a cross below
"""
aapl_df['bullish'] = 0.0
aapl_df['bullish'] = np.where(aapl_df['ema_short'] > aapl_df['ema_long'], 1.0, 0.0)
aapl_df['crossover'] = aapl_df['bullish'].diff()


"""
  Finally, we will plot the chart - showing the Close and the emas, as well as the buy and sell signals using the crossover column
  A cross to the upside can be used as a buy signal.
  A cross to the downside can be used as a sell signal 
"""
fig = plt.figure(figsize=(12,8))
ax1 = fig.add_subplot(111, ylabel='Price in $')

aapl_df['Close'].plot(ax=ax1, color='b', lw=2.)
aapl_df['ema_short'].plot(ax=ax1, color='r', lw=2.)
aapl_df['ema_long'].plot(ax=ax1, color='g', lw=2.)

ax1.plot(aapl_df.loc[aapl_df.crossover == 1.0].index, 
         aapl_df.Close[aapl_df.crossover == 1.0],
         '^', markersize=10, color='g')
ax1.plot(aapl_df.loc[aapl_df.crossover == -1.0].index, 
         aapl_df.Close[aapl_df.crossover == -1.0],
         'v', markersize=10, color='r')
plt.legend(['Close', 'EMA Short', 'EMA Long', 'Buy', 'Sell'])
plt.title('AAPL EMA Crossover')
plt.title('AAPL EMA Crossover')