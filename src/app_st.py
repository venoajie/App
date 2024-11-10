#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# built insfrom datetime import datetime
import asyncio
import numpy as np
import pandas as pd
import streamlit as st

from db_management.sqlite_management import (
    back_up_db_sqlite,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables, 
    querying_arithmetic_operator,
    querying_table)

trade_db_table= "my_trades_all_json"


async def trade_db_table():
    extensions = ('.bak')
                
    column_trade: str= "instrument_name","label", "amount", "price","side"
    my_trades_currency: list= await get_query(trade_db_table, 
                                                "BTC", 
                                                "all", 
                                                "all", 
                                                column_trade)
    
    st.dataframe(my_trades_currency)


async def main():
    
    st.title("app_st")
    data = await trade_db_table()
    st.dataframe(data)
    

if __name__ == main():
    asyncio.run(main())