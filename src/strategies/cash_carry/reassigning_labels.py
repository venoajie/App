# # -*- coding: utf-8 -*-

# built ins
import asyncio
import operator 
from collections import defaultdict
# installed

from loguru import logger as log
# user defined formula
from db_management.sqlite_management import (
    update_status_data,)
from loguru import logger as log
from utilities.string_modification import (
    remove_redundant_elements)
from strategies.basic_strategy import (
    get_label_integer,)
from strategies.cash_carry.combo_auto import(
    check_if_minimum_waiting_time_has_passed)

def sorting_list(
    listing: list,
    item_reference: str = "price",
    is_reversed: bool=True
    ) -> list:
    """
    https://sparkbyexamples.com/python/sort-list-of-dictionaries-by-value-in-python/

    Args:
        listing (list): _description_
        item_reference (str, optional): _description_. Defaults to "price".
        is_reversed (bool, optional): _description_. Defaults to True.
                                    True = from_highest_to_lowest
                                    False = from_lowest_to_highest

    Returns:
        list: _description_
    """

    return sorted(
        listing, 
        key=operator.itemgetter(item_reference), 
        reverse = is_reversed)

def get_unpaired_transaction(
    my_trades_currency_strategy: list
    ) -> list:
    """
    """
    unpaired_transactions_all =  [o for o in my_trades_currency_strategy if sum(o["amount"]) != 0]
    
    unpaired_transactions_futures =  sorting_list(
        [o for o in unpaired_transactions_all if "PERPETUAL" not in o["instrument_name"] ],
        "price",
        True
        )
    
    unpaired_transactions_perpetual =  sorting_list(
        [o for o in unpaired_transactions_all if "PERPETUAL" in o["instrument_name"] ],
        "price",
        False
        )
    
    log.error (f"unpaired_transactions_futures {unpaired_transactions_futures}")
    
    return dict(
        unpaired_transactions_futures = unpaired_transactions_futures,
        unpaired_transactions_perpetual = unpaired_transactions_perpetual) 
    
    
async def combo_modify_label_unpaired_transaction(
    unpaired_transactions_futures: list,
    unpaired_transactions_perpetual: list
    ) -> None:
    """
    """
    
    for transaction_future in unpaired_transactions_futures:
        log.debug (f"transaction_future {transaction_future}")
        size_future = transaction_future ["amount"]
        price_future = transaction_future ["price"]
        perpetual_with_same_size = sorting_list(
            [o  for o in unpaired_transactions_perpetual \
                if o["amount"] == size_future \
                    and o["price"] < price_future],
            "price",
            True
            )
        log.error (f"perpetual_with_same_size {perpetual_with_same_size}")
        if perpetual_with_same_size:
            break
    
    
def get_single_transaction(
    my_trades_currency: list,    
    strategy: str,
    ) -> list:
    """
    
    
    """
    
    my_trades_currency_active_with_no_blanks = [] if my_trades_currency == []\
        else [o for o in my_trades_currency
              if o["label"] is  not None]
    
    my_trades_currency_strategy = [o for o in my_trades_currency_active_with_no_blanks \
        if strategy in o["label"]\
                and "closed" not in o["label"]]
    
    if my_trades_currency_strategy:
        
        my_trades_label = remove_redundant_elements(
                [(o["label"]) for o in my_trades_currency_strategy])
        
        result = []
        
        for label in my_trades_label:
            
            label_integer = get_label_integer (label)
            
            transaction_under_label_integer = [o for o in my_trades_currency_strategy\
                if label_integer in o["label"]]
            
            transaction_under_label_integer_len = len(transaction_under_label_integer)
            
            if transaction_under_label_integer_len == 1:
                
                result.append (transaction_under_label_integer[0])
                
        return result

async def updating_db_with_new_label(
    trade_db_table: str,
    archive_db_table: str,
    trade_id: str,
    filter: str,
    new_label: str 
    ) -> None:
    """
    
    
    """
                
    await update_status_data(
        archive_db_table,
        "label",
        filter,
        trade_id,
        new_label,
        "="
        )
    
    await update_status_data(
        trade_db_table,
        "label",
        filter,
        trade_id,
        new_label,
        "="
        )
    

async def pairing_single_label(
    strategy_attributes: list,
    trade_db_table: str,
    archive_db_table: str,
    my_trades_currency_active: list,
    server_time: int 
    ) -> None:
    """
    
    
    """
    
    paired_success = False
    
    strategy = "futureSpread"     

    single_label_transaction = get_single_transaction(
        my_trades_currency_active,
        strategy)
    
    my_trades_amount = remove_redundant_elements([abs(o["amount"]) for o in single_label_transaction])

    strategy_params =  strategy_params= [o for o in strategy_attributes \
                                                if o["strategy_label"] == strategy][0]  
        
    for amount in my_trades_amount:
        
        my_trades_with_the_same_amount = [o for o in single_label_transaction\
            if amount == abs(o["amount"])]
        
        my_trades_with_the_same_amount_label_perpetual = [o for o in my_trades_with_the_same_amount\
            if "PERPETUAL" in o["instrument_name"]]
        
        my_trades_with_the_same_amount_label_non_perpetual = [o for o in my_trades_with_the_same_amount\
            if "PERPETUAL" not in o["instrument_name"]]
        
        my_trades_future_sorted = sorting_list(
                my_trades_with_the_same_amount_label_non_perpetual,"price",
                True)
        
        if my_trades_future_sorted:
            future_trade = my_trades_future_sorted[0]
            price_future = future_trade["price"]
            side_future = future_trade["side"]
            
            my_trades_perpetual_with_lower_price = [o for o in my_trades_with_the_same_amount_label_perpetual \
                if o["price"]< price_future ]
            
            my_trades_perpetual_with_lower_price_sorted = sorting_list(
                my_trades_perpetual_with_lower_price,"price",
                False)
                                                                            
            if side_future == "sell"\
                and my_trades_perpetual_with_lower_price_sorted:                                

                ONE_SECOND = 1000
                ONE_MINUTE = ONE_SECOND * 60
                
                perpetual_trade = my_trades_perpetual_with_lower_price_sorted[0]

                waiting_minute_before_cancel= strategy_params["waiting_minute_before_cancel"] * ONE_MINUTE
                
                timestamp_perpetual: int = perpetual_trade["timestamp"]
            
                waiting_time_for_perpetual_order: bool = check_if_minimum_waiting_time_has_passed(
                        waiting_minute_before_cancel,
                        timestamp_perpetual,
                        server_time,
                    )

                timestamp_future: int = future_trade["timestamp"]
            
                waiting_time_for_future_order: bool = check_if_minimum_waiting_time_has_passed(
                        waiting_minute_before_cancel,
                        timestamp_future,
                        server_time,
                    )
                
                paired_success = waiting_time_for_perpetual_order and waiting_time_for_future_order
                
                side_perpetual = perpetual_trade["side"]
                
                if paired_success \
                    and side_perpetual == "buy":
                        
                    filter = "trade_id"
                    trade_id = perpetual_trade[filter]
                    new_label = future_trade["label"]
                    
                    if False:
                        await updating_db_with_new_label(
                        trade_db_table,
                        archive_db_table,
                        trade_id,
                        filter,
                        new_label
                        )
                    
                    log.warning (future_trade)
                    log.debug (perpetual_trade)
                    log.debug (new_label)
                    
                    break
        
    return paired_success
