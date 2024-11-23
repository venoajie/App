#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# built insfrom datetime import datetime
import asyncio
import numpy as np
import pandas as pd
import os
import tomli
import datetime
from loguru import logger as log
import streamlit as st
import httpx
import asyncio
from transaction_management.deribit.api_requests import get_tickers, get_currencies,get_instruments

from utilities.string_modification import (
    remove_dict_elements,
    remove_double_brackets_in_list,
    remove_redundant_elements,)


from utilities.pickling import (
    read_data,
    replace_data,)
from utilities.system_tools import (
    provide_path_for_file,
    async_raise_error_message,
    raise_error_message,)

from websocket_management.ws_management import (
    get_futures_instruments,)
from db_management.sqlite_management import (
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,)

def get_config(file_name: str) -> list:
    """ """
    config_path = provide_path_for_file (file_name)
    
    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read= tomli.load(handle)
                return read
    except:
        return []


def reading_from_pkl_data(
    end_point, 
    currency,
    status: str = None
    ) -> dict:
    """ """

    path: str = provide_path_for_file (end_point,
                                      currency,
                                      status)

    return read_data(path)




async def get_currencies_from_deribit() -> float:
    """ """

    return await get_currencies()


trade_db_table= "my_trades_all_json"


async def get_instruments_from_deribit(currency) -> float:
    """ """

    result = await get_instruments(currency)

    return result

def get_settlement_period (strategy_attributes) -> list:
    
    return (remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"]for o in strategy_attributes]))
            )
# registering strategy config file    
file_toml = "config_strategies.toml"

# parsing config file
config_app = get_config(file_toml)

# get tradable strategies
tradable_config_app = config_app["tradable"]

# get tradable currencies
currencies= [o["spot"] for o in tradable_config_app] [0]

strategy_attributes = config_app["strategies"]
                        
active_strategies =   [o["strategy_label"] for o in strategy_attributes \
    if o["is_active"]==True]

# get strategies that have not short/long attributes in the label 
non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes \
    if o["non_checked_for_size_label_consistency"]==True]

cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes \
    if o["cancellable"]==True]

trade_db_table= "my_trades_all_json"

archive_db_table= f"my_trades_all_btc_json"

order_db_table= "orders_all_json"         
                
settlement_periods= get_settlement_period (strategy_attributes)
                
futures_instruments= get_futures_instruments (currencies,
                                                    settlement_periods)  

active_futures = futures_instruments["active_futures"]   

active_combo_perp = futures_instruments["active_combo_perp"]  


async def get_ticker():
    
    log.critical("AAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                
    
    ticker = []
    for combo in active_combo_perp:
        
        instrument_name = combo["instrument_name"]
            
        instrument_ticker: list = await get_tickers(
        instrument_name
        )
        
        elements_to_be_removed=["stats", "combo_state", "state","timestamp"]
        
        modified_dict = remove_dict_elements(
                instrument_ticker,
                "stats"
                )
        modified_dict = remove_dict_elements(
                modified_dict,
                "combo_state"
                )
        modified_dict = remove_dict_elements(
                modified_dict,
                "state"
                )
        modified_dict = remove_dict_elements(
                modified_dict,
                "timestamp"
                )
        log.error (modified_dict)
        
        
        ticker.append (modified_dict)

    return (ticker)

async def get_db_trade():
                
    column_trade: str= "instrument_name","label", "amount", "price","trade_id"
    my_trades_currency: list= await get_query(trade_db_table, 
                                                "BTC", 
                                                "all", 
                                                "all", 
                                                column_trade)
    
    return [o for o in my_trades_currency if "futureSpread" in o["label"]]

async def get_db_archive():
                
    column_trade: str= "instrument_name","label", "amount", "price","trade_id"
    my_trades_currency: list= await get_query(archive_db_table, 
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

    try:
        st.title("Current ")
        
        data_trade = await get_db_trade()

        data_archive = await get_db_archive()

        data_order = await get_open_orders()
        st.title("Current ")
        
        st.header("Current Positions")
        st.table()
        
        st.markdown("""---""")
        
        left_column, right_column = st.columns(2)
            
        with left_column:
            st.subheader("Trades-archive")
            st.dataframe (data_archive)
        with right_column:
            st.subheader("Trades orders")
            st.dataframe (data_trade)
    
        #await  rerun_ticker ()
        #st.rerun()

        
        
    except Exception as error:
        await async_raise_error_message(error)
        
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