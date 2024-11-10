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
    back_up_db_sqlite,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables, 
    querying_arithmetic_operator,
    querying_table)

trade_db_table= "my_trades_all_json"


async def get_db_table():
    extensions = ('.bak')
                
    column_trade: str= "instrument_name","label", "amount", "price","side"
    my_trades_currency: list= await get_query(trade_db_table, 
                                                "BTC", 
                                                "all", 
                                                "all", 
                                                column_trade)
    
    return my_trades_currency


async def fetch_data(url):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()

async def main():
    #data = await fetch_data('https://www.deribit.com/api/v2/public/ticker?instrument_name=BTC-PERPETUAL')
    #st.dataframe(data["result"])

    data = await get_db_table()
    st.dataframe(data)

if __name__ == '__main__':
    asyncio.run(main())