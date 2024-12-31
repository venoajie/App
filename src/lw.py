import pandas as pd
import plotly.express as px
import cufflinks
import plotly.io as pio 
import yfinance as yf
import warnings 
import numpy as np

warnings.filterwarnings("ignore")

pd.options.display.max_columns = None

# get historical data of S&P500(^GSPC)
df = yf.download("^GSPC", period='1d', start='2019-01-01', end='2022-01-31')
df.head()

# use different colors to distinguish between an up or down day — green for up days, and red for down days. Otherwise the volume bars all have the same color.


df['diff'] = df['Close'] - df['Open']
df.loc[df['diff']>=0, 'color'] = 'green'
df.loc[df['diff']<0, 'color'] = 'red'
fig3_b = make_subplots(specs=[[{"secondary_y": True}]])
fig3_b.add_trace(go.Candlestick(x=df.index,
                              open=df['Open'],
                              high=df['High'],
                              low=df['Low'],
                              close=df['Close'],
                             ))
fig3_b.add_trace(go.Bar(x=df.index, y=df['Volume'], name='Volume', marker={'color':df['color']}),secondary_y=True)
fig3_b.update_layout(xaxis_rangeslider_visible=False)  # hide rangeslider below the Candlestick Chart
fig3_b.update_layout(title={'text': 'S&P500', 'x': 0.5})
fig3_b.update_yaxes(range=[0,5000])
fig3_b.update_yaxes(range=[0,5e10],secondary_y=True)
fig3_b.show()

# convert column names into lowercase
df = df.rename(
        columns={
            "Ticker": "ticker",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "adj close": "adj_close",
        }
    )
df.rename(columns={"adj close":"adj_close"},inplace=True)
print(df)
