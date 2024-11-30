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
    insert_tables,
    deleting_row,)
from strategies.basic_strategy import(
    get_label_integer,)
from utilities.string_modification import(
    extract_currency_from_text,
    extract_integers_aggregation_from_text,
    get_unique_elements, 
    parsing_label,
    parsing_sqlite_json_output,
    remove_redundant_elements,
    )


def get_label_main(
    result: list, 
    strategy_label: str
    ) -> list:
    """ """

    return [o for o in result \
        if parsing_label(strategy_label)["main"]\
                    == parsing_label(o["label"])["main"]
                ]
    
    
async def saving_traded_orders (
    trade: str,
    table: str,
    order_db_table: str = "orders_all_json"
    ) -> None:
    
    """_summary_

    Args:
        trades (_type_): _description_
        orders (_type_): _description_
    """


    instrument_name = trade["instrument_name"]
    
    label= trade["label"]

    # insert clean trading transaction
    if "-FS-" not in instrument_name:
        await insert_tables(
            table, 
            trade
            )
    
    if "my_trades_all_json" in table:
        filter_trade="order_id"
        
        order_id = trade[f"{filter_trade}"]
        
        if   "closed" in label:
                                    
                await clean_up_closed_transactions(
                    instrument_name,
                    table
                    )
                    
        await deleting_row (
            order_db_table,
            "databases/trading.sqlite3",
            filter_trade,
            "=",
            order_id,
            )
        
async def get_unrecorded_trade_id(instrument_name: str) -> dict:
    
    currency = extract_currency_from_text(instrument_name)       
    
    column_list: str="data", "trade_id"
    
    from_sqlite_open= await get_query("my_trades_all_json", 
                                      instrument_name, 
                                      "all", 
                                      "all", 
                                      column_list)                                       

    from_sqlite_closed = await get_query("my_trades_closed_json", 
                                         instrument_name, 
                                         "all", 
                                         "all", 
                                         column_list)    

    #column_list: str="order_id", "trade_id","label","amount","id","data", 
    from_sqlite_all = await get_query(f"my_trades_all_{currency.lower()}_json", 
                                      instrument_name, 
                                      "all", 
                                      "all", 
                                      column_list,
                                      #40,
                                      #"id"
                                      )                                       

    from_sqlite_closed_trade_id = [o["trade_id"] for o in from_sqlite_closed]

    from_sqlite_open_trade_id = [o["trade_id"] for o in from_sqlite_open]  
                                            
    from_exchange_trade_id = [o["trade_id"] for o in from_sqlite_all]

    combined_trade_closed_open = from_sqlite_open_trade_id + from_sqlite_closed_trade_id

    unrecorded_trade_id = get_unique_elements(from_exchange_trade_id, 
                                              combined_trade_closed_open)
    
    log.debug(f"unrecorded_trade_id {unrecorded_trade_id}")

    return  [] if not from_sqlite_all \
        else [o["data"] for o in from_sqlite_all\
                    if o["trade_id"] in unrecorded_trade_id]

    
def get_custom_label(transaction: list) -> str:

    side= transaction["direction"]
    side_label= "Short" if side== "sell" else "Long"
    
    try:
        last_update= transaction["timestamp"]
    except:
        try:
            last_update= transaction["last_update_timestamp"]
        except:
            last_update= transaction["creation_timestamp"]
    
    return (f"custom{side_label.title()}-open-{last_update}")

async def refill_db (
    instrument_name,
    archive_db_table,
    ) -> list:
    

    column_trade: str= "id", "instrument_name","data", "label","trade_id"
                                    
    my_trades_currency_archive: list= await get_query(archive_db_table, 
                                                instrument_name, 
                                                "all", 
                                                "all", 
                                                column_trade)
    
    my_trades_currency_active_with_blanks = [o for o in (my_trades_currency_archive)\
                        if o["label"] is None]
    
    my_trades_archive_instrument_id = ([ o["id"] for o in my_trades_currency_active_with_blanks ])
    log.error (f"my_trades_currency_active_with_blanks {my_trades_currency_active_with_blanks}")
    log.error (f"my_trades_archive_instrument_id {my_trades_archive_instrument_id}")
    
    if my_trades_archive_instrument_id:
        for id in my_trades_archive_instrument_id:
            transaction = parsing_sqlite_json_output(
                [o["data"] for o in my_trades_currency_active_with_blanks \
                    if id == o["id"]])[0]

            label_open: str = get_custom_label(transaction)
            transaction.update({"label": label_open})
            log.debug (transaction)
            
            where_filter ="id"
                
            await deleting_row (
                archive_db_table,
                "databases/trading.sqlite3",
                where_filter,
                "=",
                id,
            )
            
            await insert_tables(
                    archive_db_table, 
                    transaction
                )
       

def get_unrecorded_trade_transactions(
    direction: str,
    my_trades_instrument_name: list,
    from_transaction_log_instrument: list) -> dict:
    
    """_summary_
    direction: "from_trans_log_to_my_trade" -> transaction log more update
    direction: "from_my_trade_to_trans_log" -> my_trade more update

    Returns:
        _type_: _description_
    """
     
    from_transaction_log_instrument_trade_id = sorted([o["trade_id"] for o in from_transaction_log_instrument] )
    
    #log.error (f"my_trades_instrument_name {my_trades_instrument_name}")
    if direction == "from_trans_log_to_my_trade":

        if my_trades_instrument_name:
            my_trades_instrument_name_trade_id = sorted([o["trade_id"] for o in my_trades_instrument_name])
            
            if my_trades_instrument_name_trade_id:
                unrecorded_trade_id = get_unique_elements(from_transaction_log_instrument_trade_id, 
                                                    my_trades_instrument_name_trade_id)
        else:
            unrecorded_trade_id = from_transaction_log_instrument_trade_id
        
        return unrecorded_trade_id
    
    if direction == "from_my_trade_to_trans_log":

        if my_trades_instrument_name:
            my_trades_instrument_name_trade_id = [o["trade_id"] for o in my_trades_instrument_name]
            
            if my_trades_instrument_name_trade_id:
                unrecorded_trade_id = get_unique_elements(my_trades_instrument_name_trade_id,
                                                          from_transaction_log_instrument_trade_id, 
                                                          )
                        
        else:
            unrecorded_trade_id = []
        
        log.debug(f"unrecorded_trade_from_transaction_log {unrecorded_trade_id}")

        return unrecorded_trade_id

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
                

async def clean_up_closed_futures_because_has_delivered(
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

    
def get_transactions_with_closed_label(transactions_all: list) -> list:
    """ """

    #log.error(f"transactions_all {transactions_all}")
    return [] if(transactions_all is None or transactions_all == []) \
        else [o for o in transactions_all if "closed" in o["label"]]

def transactions_under_label_int(
    label_integer: int, 
    transactions_all: list
    ) -> str:
    """ """
    
    transactions = [o for o in transactions_all if str(label_integer) in o["label"]]
    
    return dict(closed_transactions= transactions,
                summing_closed_transaction= sum([ o["amount"] for o in transactions]))

def get_closed_open_transactions_under_same_label_int(
    transactions_all: list, label: str
) -> list:
    """ """
    label_integer = get_label_integer(label)

    return [o for o in transactions_all if str(label_integer) in o["label"]]


async def distribute_closed_transactions(
    trade_table,
    closed_transaction: dict, 
    where_filter: str,
    ) -> None:
    """
    """
    trade_id = closed_transaction [where_filter]

    #insert closed transaction to db for closed transaction
    
    await deleting_row(
        "my_trades_closed_json",
        "databases/trading.sqlite3",
        where_filter,
        "=",
        trade_id,
    )
    
    await insert_tables(
        "my_trades_closed_json", 
        closed_transaction
        )

    #delete respective transaction from active db
    await deleting_row(
        trade_table,
        "databases/trading.sqlite3",
        where_filter,
        "=",
        trade_id,
    )
    
    
async def closing_orphan_order(
    label: str, 
    label_integer: int,
    trade_table: str,
    where_filter: str,
    transactions_under_label_main: list,
    ) -> None:
    """
    """
                                    

    closed_transaction_size = abs([o["amount"] for o in transactions_under_label_main if label in o["label"]][0])

    log.info(F" closed_transaction_size label {closed_transaction_size}")                                            
            
    open_label_with_same_size_as_closed_label_int = [
        o for o in transactions_under_label_main \
            if "open" in o["label"] \
                and closed_transaction_size == abs(o["amount"])
                ]

    #log.warning(F"open_label_with_same_size_as_closed_label {open_label_with_same_size_as_closed_label_int}")

    if open_label_with_same_size_as_closed_label_int:
        
        open_label_with_the_same_label_int = [
            o  for o in open_label_with_same_size_as_closed_label_int\
                if label_integer in o["label"]]
        
        log.warning(F"label_integer {label_integer}")
        
        if False and open_label_with_the_same_label_int:
            log.error(F"open_label_with_the_same_label_int {open_label_with_the_same_label_int}")
            
                    
            trade_id = closed_transaction [where_filter]

            #insert closed transaction to db for closed transaction
            await insert_tables("my_trades_closed_json", 
                                closed_transaction)

            #delete respective transaction form active db
            await deleting_row(
                trade_table,
                "databases/trading.sqlite3",
                where_filter,
                "=",
                trade_id,
            )
        

async def closing_one_to_one(
    transaction_closed_under_the_same_label_int: dict, 
    where_filter: str,
    trade_table: str
    ) -> None:
    """
    """
    
    for transaction in transaction_closed_under_the_same_label_int:
    
        await distribute_closed_transactions(
            trade_table,
            transaction, 
            where_filter,
            )
        
async def closing_one_to_many_single_open_order(
    open_label: dict, 
    transaction_closed_under_the_same_label_int,
    where_filter: str,
    trade_table: str
    ) -> None:             
            
    open_label_size = abs(open_label["amount"])
    log.warning(F"open_label_size {open_label_size}")
    
    closed_label_with_same_size_as_open_label = [
        o for o in transaction_closed_under_the_same_label_int\
            if "closed" in o["label"] \
                and abs(o["amount"]) == open_label_size]
    
    log.warning(F"closed_label_with_same_size_as_open_label{closed_label_with_same_size_as_open_label}")
    
    if closed_label_with_same_size_as_open_label:
            
        closed_label_with_same_size_as_open_label_int = str(extract_integers_aggregation_from_text("trade_id",
                                                                                                min,
                                                                                                closed_label_with_same_size_as_open_label))
        log.warning(F"closed_label_with_same_size_as_open_label_int {closed_label_with_same_size_as_open_label_int}")
            
        closed_label_ready_to_close = ([o for o in closed_label_with_same_size_as_open_label \
            if closed_label_with_same_size_as_open_label_int in o["trade_id"] ])[0]
        
        log.debug (F"closed_label_ready_to_close{closed_label_ready_to_close}")
        
        #closed open order
        await distribute_closed_transactions(
            trade_table,
            open_label, 
            where_filter,
            )
        
        #closed one of closing order
        await distribute_closed_transactions(
            trade_table,
            closed_label_ready_to_close, 
            where_filter,
            )
        
async def closing_one_to_many(
    open_label,
    transaction_closed_under_the_same_label_int: dict, 
    where_filter: str,
    trade_table: str
    ) -> None:
    """
    """
                    
    len_open_label_size = len(open_label)
    
    if len_open_label_size == 1:
        log.info(F"len_open_label_size 1 {len_open_label_size}")
        
        await closing_one_to_many_single_open_order(
            open_label[0],
            transaction_closed_under_the_same_label_int,
            where_filter,
            trade_table
            )
    
    else:
        for transaction in open_label:
            log.warning(F"len_open_label_size > 1 {len_open_label_size}")
                
            await closing_one_to_many_single_open_order(
                transaction,
                transaction_closed_under_the_same_label_int,
                where_filter,
                trade_table
                )
                    
                    
async def clean_up_closed_transactions(
    currency, 
    trade_table,
    transaction_all: list = None
    ) -> None:
    """
    closed transactions: buy and sell in the same label id = 0. When flagged:
    1. remove them from db for open transactions/my_trades_all_json
    2. delete them from active trade db
    """

    #prepare basic parameters for table query

    #log.critical(f" {"clean_up_closed_transactions".upper()} {instrument_name} START")
    
    where_filter = f"trade_id"

    if transaction_all is None:
        column_list: str= "instrument_name","label", "amount", where_filter
        
        #querying tables
        transaction_all: list = await get_query(
            trade_table,
            currency, 
            "all",
            "all",
            column_list,
            )   
        transaction_all = [o for o in transaction_all\
                                        if o["label"] is not None]        

    else:      
        
        if transaction_all:
                        
            transaction_all: list = [o for o in transaction_all \
            if currency in o["instrument_name"]]

    # filtered transactions with closing labels
    if transaction_all:
        
        transaction_with_closed_labels = get_transactions_with_closed_label(transaction_all)
        
        #log.debug(f"transaction_with_closed_labels {transaction_with_closed_labels}")
                               
        labels_only = remove_redundant_elements([o["label"] for o in transaction_with_closed_labels])

        if transaction_with_closed_labels:

            for label in labels_only:
                
                transactions_under_label_main = get_label_main(
                    transaction_all,
                    label
                    )
                
                label_integer = get_label_integer(label)
                
                closed_transactions_all= transactions_under_label_int(
                    label_integer,
                    transactions_under_label_main
                    )

                size_to_close = closed_transactions_all["summing_closed_transaction"]
                
                transaction_closed_under_the_same_label_int = closed_transactions_all["closed_transactions"]
                
                #log.error(f"closed_transactions_all {closed_transactions_all}")

                if size_to_close == 0:
                    
                    instrument_name_under_the_same_label_int = [o["instrument_name"] for o in transaction_closed_under_the_same_label_int]
                    
                    for instrument_name in instrument_name_under_the_same_label_int:
                        
                        instrument_transactions = ([ o for  o in transaction_closed_under_the_same_label_int\
                            if instrument_name in o["instrument_name"]])
                        
                        sum_instrument_transactions = sum([o["amount"] for o in instrument_transactions])
                        
                        if sum_instrument_transactions == 0:
                            log.info(F" instrument_transactions {instrument_transactions}")
                            
                            await closing_one_to_one(
                                instrument_transactions,
                                where_filter,
                                trade_table
                                )
                    
                if False and size_to_close != 0:
                    
                    open_label =  ([o for o in transaction_closed_under_the_same_label_int\
                        if "open" in o["label"]])
                                        
                    if open_label:
                         
                        log.warning(F" closing_one_to_many {open_label}")
                        
                        await closing_one_to_many(
                            open_label,
                            transaction_closed_under_the_same_label_int,
                            where_filter,
                            trade_table)
                    
                    #orphan label
                    else:
                                    
                        log.error(F" closing_orphan_order {label}")
                        
                        await closing_orphan_order(
                            label,
                            label_integer,
                            trade_table,
                            where_filter,
                            transactions_under_label_main
                            )
    
                        
    #log.critical(f" clean_up_closed_transactions {instrument_name} DONE")

