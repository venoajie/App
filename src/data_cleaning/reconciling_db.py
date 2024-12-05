# -*- coding: utf-8 -*-
"""_summary_
"""
# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formula
from db_management.sqlite_management import(
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables,)
from utilities.string_modification import(
    extract_integers_from_text,
    extract_currency_from_text,
    get_unique_elements, 
    remove_redundant_elements,
    sorting_list)
from utilities.system_tools import (
    sleep_and_restart,)
    
def get_sub_account_size_per_instrument(
    instrument_name: str,
    sub_account: list,
    ) -> float:
    """ """
        
    sub_account_instrument = [o for o in sub_account ["positions"] \
        if o["instrument_name"] == instrument_name ]
    
    sub_account_size_instrument = [o["size"] for o in sub_account_instrument ]
    
    sub_account_size_instrument = 0 if sub_account_size_instrument == [] \
        else sub_account_size_instrument [0]

    return 0 if not sub_account_size_instrument else sub_account_size_instrument
    

def get_my_trades_size_per_instrument(
    instrument_name: str,
    my_trades_currency: list,
    ) -> float:
    """ """

    my_trades_instrument = 0 if not my_trades_currency \
        else [o for o in my_trades_currency \
        if instrument_name in o["instrument_name"] ]  
        
    log.info (f"my_trades_instrument {my_trades_instrument}")    
                      
    sum_my_trades_instrument = 0 if not my_trades_instrument \
        else sum([o["amount"] for o in my_trades_instrument])
        
    return  0 \
        if not sum_my_trades_instrument \
            else sum_my_trades_instrument
    

def get_transaction_log_position_per_instrument(
    instrument_name: str,
    from_transaction_log: list,
    ) -> float:
    """ """
    
    from_transaction_log_instrument =([o for o in from_transaction_log \
        if o["instrument_name"] == instrument_name])    
    
    #timestamp could be double-> come from combo transaction. hence, trade_id is used to distinguish
    # other possibilities (instrument name beyond those in config):
    #from_transaction_log_instrument_example = [{'instrument_name': 'ETH_USDC', 'position': None, 'timestamp': 1730159747908, 'trade_id': 'ETH_USDC-3685325', 'user_seq': #1730159747919678, 'balance': 3987.0432}]
    
    try:

        last_time_stamp_log = [] if from_transaction_log_instrument == []\
            else (max([(o["user_seq"]) for o in from_transaction_log_instrument ]))        

        current_position_log = 0 if not from_transaction_log_instrument \
            else [o["position"] for o in from_transaction_log_instrument \
                if  last_time_stamp_log == o["user_seq"]][0]
                    # just in case, trade id = None(because of settlement)
    except:
            
        examples_from_transaction_log_instrument = [
            {'instrument_name': 'BTC-18OCT24', 'position': 0, 'timestamp': 1729238400029, 'trade_id': None},
            {'instrument_name': 'BTC-18OCT24', 'position': 0, 'timestamp': 1729231480754, 'trade_id': '321441856'}, 
            {'instrument_name': 'BTC-18OCT24', 'position': -100, 'timestamp': 1728904931445, 'trade_id': '320831413'}
            ]
        
        last_time_stamp_log = [] if from_transaction_log_instrument == []\
            else str(max([extract_integers_from_text(o["trade_id"]) for o in from_transaction_log_instrument ]))

        #log.error(f"last_time_stamp_log {last_time_stamp_log}")
        current_position_log = 0 if not from_transaction_log_instrument \
            else [o["position"] for o in from_transaction_log_instrument \
                if  str(last_time_stamp_log) in o["trade_id"]][0]

    return 0 if not current_position_log else current_position_log
    
    
def is_transaction_log_and_sub_account_size_reconciled_each_other(
    instrument_name: str,
    from_transaction_log: list,
    sub_account: list,
    ) -> bool:
    """ """
    
    if sub_account :
 
        current_position_log = get_transaction_log_position_per_instrument(
            instrument_name,
            from_transaction_log,
            )
        
        sub_account_size_instrument = get_sub_account_size_per_instrument(
            instrument_name,
            sub_account
            )
    
    reconciled = current_position_log == sub_account_size_instrument
    
    if not reconciled:
        log.critical(f"{instrument_name} reconciled {reconciled} sub_account_size_instrument {sub_account_size_instrument} current_position_log {current_position_log}")

    return reconciled
    
    
def is_my_trades_active_archived_reconciled_each_other(
    instrument_name: str,
    my_trades_active: list,
    my_trades_archived: list,
    ) -> bool:
    """ """
    
    my_trades_active_size_instrument = get_my_trades_size_per_instrument(
        instrument_name,
        my_trades_active,
        )
    
    my_trades_archived_size_instrument = get_my_trades_size_per_instrument(
        instrument_name,
        my_trades_archived,
        )
    
    
    reconciled = my_trades_archived_size_instrument == my_trades_active_size_instrument
        
    if not reconciled:
        log.critical(f"{instrument_name} reconciled {reconciled} my_trades_active_size_instrument {my_trades_active_size_instrument} my_trades_archived_size_instrument {my_trades_archived_size_instrument}")
       
    return reconciled


def is_my_trades_and_sub_account_size_reconciled_each_other(
    instrument_name: str,
    my_trades_currency: list,
    sub_account: list,
    ) -> bool:
    """ """
    
    if sub_account :
 
        my_trades_size_instrument = get_my_trades_size_per_instrument(
            instrument_name,
            my_trades_currency,
            )
        
        sub_account_size_instrument = get_sub_account_size_per_instrument(
            instrument_name,
            sub_account
            )
    
    reconciled = my_trades_size_instrument == sub_account_size_instrument
            
    if not reconciled:
        log.critical(f"{instrument_name} reconciled {reconciled} sub_account_size_instrument {sub_account_size_instrument} my_trades_size_instrument {my_trades_size_instrument}")

    return reconciled
    
    
async def my_trades_active_archived_not_reconciled_each_other(
    instrument_name: str,
    trade_db_table: str,
    archive_db_table: str
    ) -> None:
    
    column_trade: str= "instrument_name","data","trade_id","timestamp"
    
    my_trades_instrument_name_active = await get_query(trade_db_table, 
                instrument_name, 
                "all", 
                "all", 
                column_trade)
    
    my_trades_instrument_name_closed = await get_query("my_trades_closed_json", 
                instrument_name, 
                "all", 
                "all", 
                column_trade)
    
    my_trades_instrument_name_archive = await get_query(archive_db_table, 
                instrument_name, 
                "all", 
                "all", 
                column_trade)
                
    my_trades_archive_instrument_sorted = sorting_list(
        my_trades_instrument_name_archive,
        "timestamp",
        False
        )
    
    my_trades_archive_instrument_data = [ o["data"] for o in my_trades_archive_instrument_sorted ]

    if not my_trades_instrument_name_active \
        and not my_trades_instrument_name_closed:
        
        for transaction in my_trades_archive_instrument_data:
        
            log.warning (f"transaction {transaction} ")
            
            if transaction:
                await insert_tables(
                trade_db_table,
                transaction
                )  
    else:
                                        
        from_sqlite_closed_trade_id = [o["trade_id"] for o in my_trades_instrument_name_closed]
    
        from_sqlite_open_trade_id = [o["trade_id"] for o in my_trades_instrument_name_active]  
                                                
        from_exchange_trade_id = [o["trade_id"] for o in my_trades_instrument_name_archive]

        combined_trade_closed_open = from_sqlite_open_trade_id + from_sqlite_closed_trade_id

        unrecorded_trade_id = get_unique_elements(from_exchange_trade_id, combined_trade_closed_open)
        
        for trade_id in unrecorded_trade_id:
            
            transaction = [o for o in my_trades_instrument_name_archive\
                if trade_id in o["trade_id"]]
        
            log.debug (f"transaction {transaction} ")
            
            await insert_tables(
                trade_db_table,
                transaction
                )  

def is_size_sub_account_and_my_trades_reconciled(
    position_without_combo: list,
    sum_my_trades_currency_all: list,
    instrument_name: str,
    ) -> bool:
    """
    """
    
    try:

        sub_account_size_instrument = ([(o["size"]) for o in position_without_combo \
            if instrument_name in o["instrument_name"]])
                
        sub_account_size_instrument = 0 \
            if sub_account_size_instrument == []\
                else sub_account_size_instrument [0]
        
        my_trades_size_instrument = [o["amount"] for o in sum_my_trades_currency_all\
            if instrument_name in o["instrument_name"]]

        sum_my_trades_size_instrument = 0 \
            if my_trades_size_instrument == []\
                else sum(my_trades_size_instrument)
                
        if sub_account_size_instrument != sum_my_trades_size_instrument:
            log.critical (f"sum_my_trades_size_instrument {sum_my_trades_size_instrument}  sub_account_size_instrument {sub_account_size_instrument}")
                
        return sub_account_size_instrument == sum_my_trades_size_instrument
                        
    except Exception as error:
        log.warning(error)
                


def check_whether_order_db_reconciled_each_other(
    sub_account: list,
    instrument_name: str,
    orders_currency: list
    ) -> None:
    """ """
    
    if sub_account :
        
        sub_account_orders = sub_account["open_orders"]
        
        sub_account_instrument = [o for o in sub_account_orders \
            if o["instrument_name"] == instrument_name ]
                
        len_sub_account_instrument = 0 if not sub_account_instrument \
            else len([o["amount"] for o in sub_account_instrument])
            
        orders_instrument = [o for o in orders_currency \
            if instrument_name in o["instrument_name"] ]
        
        len_orders_instrument = 0 if not orders_instrument \
            else len([o["amount"] for o in orders_instrument])    
        
        result = len_orders_instrument == len_sub_account_instrument
        #log.debug (f"result {result} ")
        
        if not result:
            log.critical(f"len_order equal {result} len_sub_account_instrument {len_sub_account_instrument} len_orders_instrument {len_orders_instrument}")
        # comparing and return the result
        return  result

    else :        
        return  False
    
    
    
async def reconciling_orders(
    modify_order_and_db: object,
    sub_account: list,
    orders_currency: list,
    direction: str
    ) -> None:
    """
    direction: 
            "from_sub_account_to_order_db"
            "from_order_db_to_sub_account"
    """
    
    try:
        
        if direction == "from_order_db_to_sub_account":
            orders_instrument_name = remove_redundant_elements([o["instrument_name"] for o in orders_currency  ])
        
        if direction == "from_sub_account_to_order_db":

            sub_account_orders = sub_account["open_orders"]
            
            orders_instrument_name = remove_redundant_elements([o["instrument_name"] for o in sub_account_orders  ])
                    
        for instrument_name in orders_instrument_name:
            
            currency = extract_currency_from_text (instrument_name)

            len_order_is_reconciled_each_other =  check_whether_order_db_reconciled_each_other (
                sub_account,
                instrument_name,
                orders_currency)
            
            if not len_order_is_reconciled_each_other:
                                
                orders_instrument_name = [o for o in orders_currency \
                    if instrument_name in o["instrument_name"]]
                
                for order in orders_instrument_name:
                    await modify_order_and_db.cancel_by_order_id(order["order_id"])
                
                await modify_order_and_db. resupply_sub_accountdb(currency)
                
                await sleep_and_restart ()
                
    except Exception as error:
        log.warning(error)

