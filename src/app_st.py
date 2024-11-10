#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# built insfrom datetime import datetime
import asyncio
import numpy as np
import pandas as pd

import streamlit as st
import httpx
import asyncio


from db_management.sqlite_management import (
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,)

trade_db_table= "my_trades_all_json"


async def get_db_table():
                
    column_trade: str= "instrument_name","label", "amount", "price"
    my_trades_currency: list= await get_query(trade_db_table, 
                                                "BTC", 
                                                "all", 
                                                "all", 
                                                column_trade)
    
    return [o for o in my_trades_currency if "futureSpread" in o["label"]]

async def get_open_orders():
    order_db_table= "orders_all_json"
                
    column_order= "instrument_name","label","amount",
    orders_currency: list= await get_query(order_db_table, 
                                                "BTC", 
                                                "all", 
                                                "all", 
                                                column_order)
    
    return orders_currency

async def main():
    #data = await fetch_data('https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL')
    #st.dataframe(data["result"])

    data = await get_db_table()
    st.title("Current ")

    data_order = await get_open_orders()
    st.title("Current ")
    
    st.header("Current Positions")
    st.table()
    
    st.markdown("""---""")

    st.subheader("Positions")
    st.dataframe(data)
    st.markdown("##")
    
    left_column, right_column = st.columns(2)
        
    with left_column:
        st.subheader("Trades")
        st.dataframe (data)
    with right_column:
        st.subheader("Open orders")
        st.dataframe (data_order)
    
if __name__ == '__main__':
    
    st.set_page_config(page_title="Sales Dashboard", page_icon=":bar_chart:", layout="wide")
        
    # ---- HIDE STREAMLIT STYLE ----
    hide_st_style = """
                <style>
                #MainMenu {visibility: hidden;}
                footer {visibility: hidden;}
                header {visibility: hidden;}
                </style>
                """
    st.markdown(hide_st_style, unsafe_allow_html=True)
    
    asyncio.run(main())