import pandas as pd
import plotly.express as px
import cufflinks
import plotly.io as pio 
import yfinance as yf
import warnings 
import numpy as np

warnings.filterwarnings("ignore")
cufflinks.go_offline()
cufflinks.set_config_file(world_readable=True, theme='pearl')
pio.renderers.default = "notebook" # should change by looking into pio.renderers

pd.options.display.max_columns = None

# get historical data of S&P500(^GSPC)
df = yf.download("^AAPL", period='1d', start='2019-01-01', end='2022-01-31')
df.head()
fig1 = go.Figure(data=go.Scatter(x=df.index, y=df['Close'], mode='lines'))
fig1.update_layout(title={'text': 'S&P500', 'x': 0.5})
fig1.show()
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
