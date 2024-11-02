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
    deleting_row,
    executing_query_with_return,
    querying_arithmetic_operator,
    querying_duplicated_transactions)
from strategies.basic_strategy import(
    check_db_consistencies,
    get_label_integer,)
from strategies.config_strategies import(
    max_rows,
    paramaters_to_balancing_transactions)
from transaction_management.deribit.telegram_bot import(
    telegram_bot_sendtext,)
from utilities.system_tools import(
    sleep_and_restart,)
from utilities.string_modification import(
    extract_currency_from_text,
    extract_integers_aggregation_from_text,
    extract_integers_from_text,
    get_unique_elements, 
    parsing_label,
    remove_redundant_elements,)


def get_label_main(
    result: list, 
    strategy_label: str
    ) -> list:
    """ """

    return [o for o in result \
        if parsing_label(strategy_label)["main"]\
                    == parsing_label(o["label"])["main"]
                ]

async def reconciling_sub_account_and_db_open_orders(
    instrument_name: str,
    order_db_table: str,
    orders_currency: list,
    sub_account: list
    ) -> None:

    where_filter = f"order_id"
        
    sub_account_orders = sub_account["open_orders"]
    
    sub_account_orders_instrument = [o for o in sub_account_orders if instrument_name in o["instrument_name"]]
    
    #log.error(f"sub_account_orders_instrument {sub_account_orders_instrument}")

    sub_account_orders_instrument_id = [] if sub_account_orders_instrument == [] \
        else [o["order_id"] for o in sub_account_orders_instrument]

    log.debug(f"instrument_name {instrument_name}")
    db_orders_instrument = [o for o in orders_currency \
        if instrument_name in o["instrument_name"]]
    log.debug(f"db_orders_instrument {db_orders_instrument}")
    log.warning(f"sub_account_orders_instrument {sub_account_orders_instrument}")

    db_orders_instrument_id = [] if db_orders_instrument == [] \
        else [o["order_id"] for o in db_orders_instrument]
        
    # sub account contain outstanding order
    if sub_account_orders_instrument:
        
        filter_trade="order_id"
        
        # no outstanding order in db. refill with data from sub account
        if not db_orders_instrument:
            for order in sub_account_orders_instrument:
                
                await insert_tables(order_db_table, order)
                
        # both contain orders, but different id
        else:
            unrecorded_order_id = get_unique_elements(db_orders_instrument_id, 
                                                      sub_account_orders_instrument_id)
            
            log.info(f"unrecorded_order_id {unrecorded_order_id}")
            
            if unrecorded_order_id:
                
                for order_id in unrecorded_order_id:
                    
                    order = [o for o in sub_account_orders_instrument if order_id in ["order_id"]]
                    
                    if order_id:
                        
                        try:
                            order = order [0]
                                                        
                            order_state= order["order_state"]
                            
                            if order_state == "cancelled" \
                                or order_state == "filled":
                                    
                                await deleting_row(order_db_table,
                                                    "databases/trading.sqlite3",
                                                    filter_trade,
                                                    "=",
                                                    order_id,
                                                )
                        
                            if order_state == "open":
                                
                                await insert_tables(order_db_table, 
                                                    order)
            
                        except:
                            await telegram_bot_sendtext(f"order {order}")
    # sub account did not contain order from respective instrument
    else:
        # if db contain orders, delete them
        if db_orders_instrument:
            
            for open_order_id in db_orders_instrument_id:
                        
                await deleting_row("orders_all_json",
                                    "databases/trading.sqlite3",
                                    where_filter,
                                    "=",
                                    open_order_id,
                                )


async def get_unrecorded_trade_and_order_id(instrument_name: str) -> dict:
    
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

async def get_unrecorded_trade_from_transaction_log(
    my_trades_instrument_name: list,
    from_transaction_log_instrument: list) -> dict:
     
    my_trades_instrument_name_trade_id = [o["trade_id"] for o in my_trades_instrument_name]
    from_transaction_log_instrument_trade_id = [o["trade_id"] for o in from_transaction_log_instrument]  
                       
    if my_trades_instrument_name_trade_id:
        unrecorded_trade_id = get_unique_elements(from_transaction_log_instrument_trade_id, 
                                              my_trades_instrument_name_trade_id)
                   
    else:
        unrecorded_trade_id = from_transaction_log_instrument_trade_id
    
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
            if o["instrument_name"] == instrument_name ]
        len_orders_instrument = 0 if not orders_instrument \
            else len([o["amount"] for o in orders_instrument])    
        
        result = len_orders_instrument == len_sub_account_instrument
        
        if not result:
            log.critical(f"len_order equal {result} len_sub_account_instrument {len_sub_account_instrument} len_orders_instrument {len_orders_instrument}")
        # comparing and return the result
        return  result

    else :        
        return  False

def check_whether_size_db_reconciled_each_other(
    sub_account,
    instrument_name,
    my_trades_currency,
    from_transaction_log) -> None:
    """ """
    
    if sub_account :
        
        sub_account_size_instrument = [o["size"] for o in sub_account ["positions"] \
            if o["instrument_name"] == instrument_name ]
        sub_account_size_instrument = 0 if sub_account_size_instrument == [] \
            else sub_account_size_instrument [0]

        from_transaction_log_instrument =([o for o in from_transaction_log \
            if o["instrument_name"] == instrument_name])
        
        #timestamp could be double-> come from combo transaction. hence, trade_id is used to distinguish
        try:

            last_time_stamp_log = [] if from_transaction_log_instrument == []\
                else (max([(o["user_seq"]) for o in from_transaction_log_instrument ]))
                

            current_position_log = 0 if from_transaction_log_instrument == []\
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
    
            current_position_log = 0 if from_transaction_log_instrument == []\
                else [o["position"] for o in from_transaction_log_instrument \
                    if  last_time_stamp_log in o["trade_id"]][0]
    
        my_trades_instrument = [o for o in my_trades_currency \
            if o["instrument_name"] == instrument_name]        
        sum_my_trades_instrument = 0 if not my_trades_instrument \
            else sum([o["amount"] for o in my_trades_instrument])
        
            
        # comparing the result
        sum_trade_from_log_and_db_is_equal = current_position_log == sum_my_trades_instrument == sub_account_size_instrument
        
        if not sum_trade_from_log_and_db_is_equal:
            log.critical(f"sum_from_log_and_trade_is_equal {sum_trade_from_log_and_db_is_equal} sum_my_trades_currency {sum_my_trades_instrument}  sub_account_size_instrument {sub_account_size_instrument} current_position_log {current_position_log}")
        # combining result
        return sum_trade_from_log_and_db_is_equal

    else :        
        return False
                    
def check_if_label_open_still_in_active_transaction(
    from_sqlite_open: list, 
    instrument_name: str, 
    label: str
    ) -> bool:
    """_summary_
    
    concern: there are highly possibities of one label for multiple instruments for transactions under future spread. 
    Hence, the testing should be specified for an instrument

    Args:
        from_sqlite_open(list): _description_
        instrument_name(str): _description_
        label(str): _description_

    Returns:
        bool: _description_
    """
    integer_label= extract_integers_from_text(label)
    
    log.warning(f"from_sqlite_open {from_sqlite_open}")
    log.info(f"integer_label {integer_label}")
    
    trades_from_sqlite_open = [o for o in from_sqlite_open \
        if integer_label == extract_integers_from_text(o["label"]) \
            and "open" in o["label"] \
                and instrument_name == o["instrument_name"] ] 
    
    log.debug(f"trades_from_sqlite_open {trades_from_sqlite_open}")
    
    if trades_from_sqlite_open !=[]:

        # get sum of open label only
        sum_from_open_label_only= sum([o["amount"] for o in trades_from_sqlite_open])
        log.warning(f"sum_from_open_label_only {sum_from_open_label_only}")
        
        # get net sum of label
        sum_net_trades_from_open_and_closed= sum([o["amount"] for o in from_sqlite_open\
            if integer_label == extract_integers_from_text(o["label"]) \
                and instrument_name == o["instrument_name"]])
        
        log.warning(f"sum_net_trades_from_open_and_closed {sum_net_trades_from_open_and_closed}")
        
        sum_label = sum_from_open_label_only >= sum_net_trades_from_open_and_closed
        log.warning(f"sum_label {sum_label}")
        
    return False if trades_from_sqlite_open==[] else sum_label


def get_label_from_respected_id(
    trades_from_exchange, 
    unrecorded_id, 
    marker
    ) -> str:
    #log.info(f"trades_from_exchange {trades_from_exchange}")
    #log.info(f"unrecorded_id {unrecorded_id} marker {marker}")
    
    label= [o["label"] for o in trades_from_exchange if o[marker] == unrecorded_id][0]
    
    #log.info(f"label {label}")
    return label

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


async def remove_duplicated_elements() -> None:
    """ 
    
        # label/order id may be duplicated(consider an order id/label was 
        # executed couple of times due to lack of liquidity)
        # There is only one trade_id
       
    """
    
    label_checked=["my_trades_all_json", "my_trades_closed_json"]
    
    where_filter = f"trade_id"
    
    for label in label_checked:
        duplicated_elements_all = await querying_duplicated_transactions(label,where_filter)

        duplicated_elements = 0 if duplicated_elements_all == 0 else [o[where_filter] for o in duplicated_elements_all]
        
        log.info(f"duplicated_elements {duplicated_elements}")

        if duplicated_elements != 0:

            for trade_id in duplicated_elements:
                
                await deleting_row(
                    label,
                    "databases/trading.sqlite3",
                    where_filter,
                    "=",
                    trade_id,
                )
                await sleep_and_restart()

    
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
            if "closed" in o["label"] and abs(o["amount"]) == open_label_size]
    
    log.warning(F"closed_label_with_same_size_as_open_label{closed_label_with_same_size_as_open_label}")
    
    if closed_label_with_same_size_as_open_label:
            
        closed_label_with_same_size_as_open_label_int = str(extract_integers_aggregation_from_text("trade_id",
                                                                                                min,
                                                                                                closed_label_with_same_size_as_open_label))
        log.warning(F"closed_label_with_same_size_as_open_label_int {closed_label_with_same_size_as_open_label_int}")
            
        closed_label_ready_to_close = ([o for o in closed_label_with_same_size_as_open_label \
            if closed_label_with_same_size_as_open_label_int in o["trade_id"] ])[0]
        log.warning(F"closed_label_ready_to_close{closed_label_ready_to_close}")
        
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
    instrument_name, 
    trade_table,
    transaction_all: list = None
    ) -> None:
    """
    closed transactions: buy and sell in the same label id = 0. When flagged:
    1. remove them from db for open transactions/my_trades_all_json
    2. delete them from active trade db
    """

    #prepare basic parameters for table query

    log.error(f" clean_up_closed_transactions {instrument_name} START")
    
    where_filter = f"trade_id"

    column_list: str= "instrument_name","label", "amount", where_filter
        
    if transaction_all is None:
        
        #querying tables
        transaction_all: list = await get_query(
            trade_table,
            instrument_name, 
            "all",
            "all",
            column_list,
            )           

    else:      
        
        if transaction_all:
                        
            transaction_all: list = [o for o in transaction_all \
            if instrument_name in o["instrument_name"]]

    # filtered transactions with closing labels
    if transaction_all:
        
        transaction_with_closed_labels = get_transactions_with_closed_label(transaction_all)
                               
        labels_only = remove_redundant_elements([o["label"] for o in transaction_with_closed_labels])

        #log.error(f"closing transactions {labels_only}")

        if transaction_with_closed_labels:

            for label in labels_only:
                
                transactions_under_label_main = get_label_main(transaction_all,  
                                                               label)
                log.error(f"label {label}")
                #log.error(f"transactions_under_label_main {transactions_under_label_main}")
                
                label_integer = get_label_integer(label)
                
                closed_transactions_all= transactions_under_label_int(label_integer, 
                                                                      transactions_under_label_main)

                size_to_close = closed_transactions_all["summing_closed_transaction"]
                
                transaction_closed_under_the_same_label_int = closed_transactions_all["closed_transactions"]

                if size_to_close == 0:
                    
                    await closing_one_to_one(
                        transaction_closed_under_the_same_label_int,
                        where_filter,
                        trade_table
                        )
                    
                if size_to_close != 0:
                    
                    open_label =  ([o for o in transaction_closed_under_the_same_label_int\
                        if "open" in o["label"]])
                                        
                    if open_label:
                         
                        await closing_one_to_many(
                            open_label,
                            transaction_closed_under_the_same_label_int,
                            where_filter,
                            trade_table)
                    
                    #orphan label
                    else:
                                    
                        log.error(F" orphan label {label}")
                        
                        await closing_orphan_order(
                            label,
                            label_integer,
                            trade_table,
                            where_filter,
                            transactions_under_label_main
                            )
    
                        
    log.error(f" clean_up_closed_transactions {instrument_name} DONE")


async def count_and_delete_ohlc_rows():

    log.info("count_and_delete_ohlc_rows-START")
    tables = ["market_analytics_json", 
              "supporting_items_json",
              "ohlc1_eth_perp_json", 
              "ohlc1_btc_perp_json", 
              "ohlc15_eth_perp_json", 
              "ohlc15_btc_perp_json", 
              "ohlc30_eth_perp_json", 
              "ohlc60_eth_perp_json",
              "ohlc3_eth_perp_json", 
              "ohlc3_btc_perp_json", 
              "ohlc5_eth_perp_json", 
              "ohlc5_btc_perp_json",  
              "supporting_items_json"]
    
    database: str = "databases/trading.sqlite3"  

    for table in tables:

        rows_threshold= max_rows(table)
        
        if "supporting_items_json" in table:
            where_filter = f"id"
            
        else:
            where_filter = f"tick"
        
        count_rows_query = querying_arithmetic_operator(where_filter, 
                                                        "COUNT", 
                                                        table)

        rows = await executing_query_with_return(count_rows_query)
        
        rows = rows[0]["COUNT(tick)"] if where_filter=="tick" else rows[0]["COUNT(id)"]
            
        if rows > rows_threshold:
                  
            first_tick_query = querying_arithmetic_operator(where_filter, 
                                                            "MIN",
                                                            table)
            
            first_tick_fr_sqlite = await executing_query_with_return(first_tick_query)
            
            if where_filter=="tick":
                first_tick = first_tick_fr_sqlite[0]["MIN(tick)"] 
            
            if where_filter=="id":
                first_tick = first_tick_fr_sqlite[0]["MIN(id)"]

            #log. error(f"table {table} where_filter {where_filter} first_tick_fr_sqlite {first_tick_fr_sqlite}")
            await deleting_row(table,
                               database,
                               where_filter,
                               "=",
                               first_tick)
            
    log.info("count_and_delete_ohlc_rows-DONE")
