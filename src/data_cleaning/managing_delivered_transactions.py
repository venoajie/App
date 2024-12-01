# -*- coding: utf-8 -*-
"""_summary_
"""
# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formula
from db_management.sqlite_management import(
    insert_tables,
    deleting_row,)
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,)
from utilities.string_modification import (
    remove_double_brackets_in_list,
    remove_redundant_elements)

def get_settlement_period (strategy_attributes) -> list:
    
    return (remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"]for o in strategy_attributes]))
            )
    
async def clean_up_closed_futures_because_has_delivered(
    currencies: list, 
    instrument_name: list,
    strategy_attributes: list,
    )-> None:
    
    settlement_periods = get_settlement_period (strategy_attributes)
    
    futures_instruments = get_futures_instruments (
            currencies,
            settlement_periods
            )  
    
    instrument_attributes_futures_all = futures_instruments["active_futures"]   
    
    active_futures_instrument =  [o["instrument_name"] for o in instrument_attributes_futures_all]
    
    if instrument_name not in active_futures_instrument:
        
        log.debug (f" inactive instrument_name {instrument_name}")
        
    
        

async def clean_up_closed_futures_because_has_delivered_(
    instrument_name, 
    transaction,
    delivered_transaction
    )-> None:
    
    log.warning(f"instrument_name {instrument_name}")
    log.warning(f"transaction {transaction}")
    try:
        trade_id_sqlite= int(transaction["trade_id"])
    
    except:
        trade_id_sqlite=(transaction["trade_id"])
    
    timestamp= transaction["timestamp"]
    
    closed_label=f"futureSpread-closed-{timestamp}"
    
    transaction.update({"instrument_name":instrument_name})
    transaction.update({"timestamp":timestamp})
    transaction.update({"price":transaction["price"]})
    transaction.update({"amount":transaction["amount"]})
    transaction.update({"label":transaction["label"]})
    transaction.update({"trade_id":trade_id_sqlite})
    transaction.update({"order_id":transaction["order_id"]})

    #log.warning(f"transaction {transaction}")
    await insert_tables("my_trades_closed_json", transaction)
    
    await deleting_row("my_trades_all_json",
                    "databases/trading.sqlite3",
                    "trade_id",
                    "=",
                    trade_id_sqlite,
                )

    delivered_transaction= delivered_transaction[0]
    
    timestamp_from_transaction_log= delivered_transaction["timestamp"] 

    try:
        price_from_transaction_log= delivered_transaction["price"] 
    
    except:
        price_from_transaction_log= delivered_transaction["index_price"] 
        
    closing_transaction= transaction
    closing_transaction.update({"label":closed_label})
    closing_transaction.update({"amount":(closing_transaction["amount"])*-1})
    closing_transaction.update({"price":price_from_transaction_log})
    closing_transaction.update({"trade_id":None})
    closing_transaction.update({"order_id":None})
    closing_transaction.update({"timestamp":timestamp_from_transaction_log})

    await insert_tables("my_trades_closed_json", closing_transaction)

    