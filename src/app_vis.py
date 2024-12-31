import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import pandas_ta as ta
from sklearn.preprocessing import MinMaxScaler 
from keras.models import load_model # type: ignore
from functions import get_sp500_tickers
from functions import df_for_candlestick, plot_candlestick
from functions import obtain_dataframe
from functions import get_features_targets, forecast, create_predictions_series, plot_forecast

pd.options.display.float_format = '{:.2f}'.format

stock_keys = [
'previousClose', 'open', 'dayLow', 'dayHigh', 'volume', 'averageVolume', 'averageVolume10days',
'averageDailyVolume10Day', 'marketCap', 'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 
'fiftyDayAverage', 'twoHundredDayAverage',
'52WeekChange', 'SandP52WeekChange', 'priceToSalesTrailing12Months', 
]

income_keys = [
    'totalRevenue', 'revenuePerShare', 'profitMargins', 'grossMargins', 'ebitdaMargins', 
    'operatingMargins', 'ebitda', 'trailingPE', 'forwardPE', 
    'trailingEps', 'forwardEps', 
    'enterpriseToRevenue', 'enterpriseToEbitda', 'trailingPegRatio'
'earningsQuarterlyGrowth', 'earningsGrowth', 'revenueGrowth', 
]

balance_keys = [
    'totalCash', 'totalDebt', 'quickRatio', 
    'currentRatio', 'debtToEquity',
    'bookValue', 'priceToBook', 'totalCashPerShare',  
    'returnOnAssets', 'returnOnEquity'
]

cashflow_keys = [
    'freeCashflow', 'operatingCashflow'
]

share_keys = [
    'floatShares', 'sharesOutstanding', 'sharesShort', 'sharesPercentSharesOut',
    'heldPercentInsiders', 'heldPercentInstitutions', 'shortRatio',
    'shortPercentOfFloat', 'impliedSharesOutstanding'
]

dividend_splits_keys = [
    'dividendRate', 'dividendYield', 'exDividendDate', 'payoutRatio',
    'fiveYearAvgDividendYield', 'trailingAnnualDividendRate', 'trailingAnnualDividendYield',
    'lastSplitFactor', 'lastSplitDate'
]

sidebar_text = '''
<div style="background-color: #9c2230; padding: 20px; border-radius: 10px; text-align: center;">
    <h1 style="color: #fff; font-size: 24px;">Stock Price Data, Statistics and Forecast App</h1>
    <p style="color: #fff; font-size: 16px;">A LSTM Model</p>
</div>

## About This App

This application allows users to visualise stock price data, statistics and financial data. Furthermore, the application leverages a pre-trained LSTM model to forecast stock prices for the next 30 days.
The LSTM model was trained and developed using Tensorflow Keras. The stock data is obtained using the Yahoo Finance Python API. 

### Disclaimer

- The information provided is for educational and demonstration purposes only.
- This is not financial advice.

Learn more about the app
'''

st.sidebar.markdown(sidebar_text, unsafe_allow_html=True)

st.title('Stock Data, Statistics and Forecasting 📊💸')

introduction = '''
To get started, just choose a stock ticker!
'''
st.markdown(introduction)

# Ticker Selection --------------------------------------------------------------------------------------------------------------------------------
tickers = get_sp500_tickers()
selected_ticker = st.selectbox('Select a stock ticker', tickers)

# Candlestick Chart --------------------------------------------------------------------------------------------------------------------------------
selection_horizon = ''

st.markdown('Choose a horizon for the stock price candlestick chart 📈')
one_day, five_day, one_month, six_month, one_year, five_year = st.columns(6)
if one_day.button("1d", use_container_width=True):
    selected_horizon='1d'
    candlestick_dataframe = df_for_candlestick(selected_horizon, selected_ticker)
    candlestick = plot_candlestick(selected_ticker, selected_horizon, candlestick_dataframe)
    st.plotly_chart(candlestick)
if five_day.button("5d", use_container_width=True):
    selected_horizon='5d'
    candlestick_dataframe = df_for_candlestick(selected_horizon, selected_ticker)
    candlestick = plot_candlestick(selected_ticker, selected_horizon, candlestick_dataframe)
    st.plotly_chart(candlestick)
if one_month.button("1mo", use_container_width=True):
    selected_horizon='1mo'
    candlestick_dataframe = df_for_candlestick(selected_horizon, selected_ticker)
    candlestick = plot_candlestick(selected_ticker, selected_horizon, candlestick_dataframe)
    st.plotly_chart(candlestick)
if six_month.button("6mo", use_container_width=True):
    selected_horizon='6mo'
    candlestick_dataframe = df_for_candlestick(selected_horizon, selected_ticker)
    candlestick = plot_candlestick(selected_ticker, selected_horizon, candlestick_dataframe)
    st.plotly_chart(candlestick)
if one_year.button("1y", use_container_width=True):
    selected_horizon='1y'
    candlestick_dataframe = df_for_candlestick(selected_horizon, selected_ticker)
    candlestick = plot_candlestick(selected_ticker, selected_horizon, candlestick_dataframe)
    st.plotly_chart(candlestick)
if five_year.button("5y", use_container_width=True):
    selected_horizon='5y'
    candlestick_dataframe = df_for_candlestick(selected_horizon, selected_ticker)
    candlestick = plot_candlestick(selected_ticker, selected_horizon, candlestick_dataframe)
    st.plotly_chart(candlestick)
# Forecast --------------------------------------------------------------------------------------------------------------------------------
forecast_dataframe = obtain_dataframe(selected_ticker)
target, feature_transform = get_features_targets(forecast_dataframe)
forecast_model = load_model('forecast_model.h5')
predictions = forecast(forecast_model, feature_transform, target, forecast_days=30)
predictions_series = create_predictions_series(forecast_dataframe, predictions)
forecast_plot = plot_forecast(selected_ticker, forecast_dataframe, predictions_series)

st.header("Stock Price Forecast")
forecast_desc = '''
This plot displays the forecasted prices of the selected stock for the next 30 days. This forecasting is executed by a LSTM model using the stock's historical price data as well as technical indicators.
'''
st.markdown(forecast_desc)
st.plotly_chart(forecast_plot)
