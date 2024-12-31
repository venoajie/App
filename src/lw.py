import pandas as pd
import plotly.express as px
import cufflinks
import plotly.io as pio 
import yfinance as yf
import warnings 
warnings.filterwarnings("ignore")
cufflinks.go_offline()
cufflinks.set_config_file(world_readable=True, theme='pearl')
pio.renderers.default = "notebook" # should change by looking into pio.renderers

pd.options.display.max_columns = None


symbols = ["AAPL"]

df = yf.download(tickers=symbols)
df.head()   
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

def moving_average(series, window=5, kind="sma"):
    if kind=="sma":
        return series.rolling(window=window, min_periods=window).mean()
    elif kind=="ema":
        return series.rolling(window=window, min_periods=window).mean()
    elif kind=="wma":
        return series.rolling(window=window, min_periods=window).apply(lambda x: np.average(x, weights=np.arange(1, window+1,1)))
        
        
        

tdf = df.copy()

window=30
tdf[f"close_sma_{window}"] = moving_average(tdf.close, window=window)
tdf[f"close_ema_{window}"] = moving_average(tdf.close, window=window, kind="ema")
tdf[f"close_wma_{window}"] = moving_average(tdf.close, window=window, kind="wma")
tdf

cols = [c for c in tdf.columns if "close" in c and "adj" not in c]
tdf[cols].iplot(kind="line")