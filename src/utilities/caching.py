# -*- coding: utf-8 -*-

import asyncio
from transaction_management.deribit.api_requests import (
    get_tickers,)
from utilities.pickling import (
    read_data,)
from utilities.system_tools import (
    provide_path_for_file)


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


# Using the LRUCache decorator function with a maximum cache size of 3
def combining_ticker_data(instruments_name):
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/]
    https://medium.com/@jodielovesmaths/memoization-in-python-using-cache-36b676cb21ef
    data caching
    https://medium.com/@ryan_forrester_/python-return-statement-complete-guide-138c80bcfdc7

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    result=[]
    for instrument_name in instruments_name:
                        
        result_instrument= reading_from_pkl_data(
                "ticker",
                instrument_name
                )

        if result_instrument:
            result_instrument = result_instrument[0]

        else:
            result_instrument = get_tickers (instrument_name)
        result.append (result_instrument)

    return result



def update_cached_ticker(
    instrument_name: str,
    ticker: list,
    data_orders: dict,
    )-> None:
    """_summary_
    https://stackoverflow.com/questions/73064997/update-values-in-a-list-of-dictionaries

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """
    
        
    instrument_ticker: list = [o for o in ticker if instrument_name in o["instrument_name"]]
    
    if instrument_ticker:
        
        for item in data_orders:

            if "stats" not in item and "instrument_name" not in item and "type" not in item:
                [o for o in ticker 
                 if instrument_name in o["instrument_name"]][0][item] = data_orders[item]

            if "stats"  in item:
                
                data_orders_stat = data_orders[item]
                
                for item in data_orders_stat:
                    [o for o in ticker 
                     if instrument_name in o["instrument_name"]][0]["stats"][item] = data_orders_stat[item]
    
    else:
        from loguru import logger as log
    
        log.warning (f" {ticker}")
        log.debug(f" {instrument_name}")
        
        log.critical (f"instrument_ticker before {instrument_ticker}")
        #combining_order_data(currencies)
        log.debug (f"instrument_ticker after []-not ok {instrument_ticker}")
    
# Using the LRUCache decorator function with a maximum cache size of 3
async def combining_order_data(
    private_data: object,
    currencies: list
    )-> list:
    
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/]
    https://medium.com/@jodielovesmaths/memoization-in-python-using-cache-36b676cb21ef
    data caching
    https://medium.com/@ryan_forrester_/python-return-statement-complete-guide-138c80bcfdc7

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    from utilities.pickling import replace_data
    
    result=[]
    for currency in currencies:
        
        sub_accounts = await private_data.get_subaccounts_details (currency)
        
        my_path_sub_account = provide_path_for_file(
            "sub_accounts",
            currency
            )
        
        replace_data(
            my_path_sub_account, 
            sub_accounts
            )
                
        if sub_accounts:

            sub_account = sub_accounts[0]
        
            sub_account_orders = sub_account["open_orders"]
            
            if sub_account_orders:
                
                for order in sub_account_orders:
                    
                    result.append (order)

    return result

async def update_cached_orders(
    orders_all,
    queue: dict):
    """_summary_
    https://stackoverflow.com/questions/73064997/update-values-in-a-list-of-dictionaries

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """
    
    data_orders= await queue.get()
    
    print(f"data_orders {data_orders}")
    
    if data_orders:
        
        orders = data_orders["orders"]
        
        trades = data_orders["trades"]
        
        if orders:
            
            if trades :
                
                for trade in trades:

                    order_id = trade["order_id"]
                    
                    selected_order = [o for o in orders_all 
                                    if order_id in o["order_id"]]
                    
                    if selected_order:
                                            
                        orders_all.remove(selected_order[0])
                    
            if orders:
            
                for order in orders:
                    
                    print(f"cached order {order}")
                    
                    order_state= order["order_state"]    
                    
                    if order_state == "cancelled" or order_state == "filled":
                    
                        order_id = order["order_id"]
                        
                        selected_order = [o for o in orders_all 
                                        if order_id in o["order_id"]]
                        
                        print(f"caching selected_order {selected_order}")
                        
                        if selected_order:
                                                
                            orders_all.remove(selected_order[0])
                        
                    else:
                    
                        orders_all.append(order)
                    
        await queue.put(orders_all)
        await queue.task_done()