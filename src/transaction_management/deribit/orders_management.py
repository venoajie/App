# -*- coding: utf-8 -*-

# installed
from loguru import logger as log

# user defined formula
from db_management.sqlite_management import(
    deleting_row,
    insert_tables)
from websocket_management.cleaning_up_transactions import(
    clean_up_closed_transactions,)

def telegram_bot_sendtext(
    bot_message: str,
    purpose: str = "general_error"
    ) -> None:
    
    from utilities import telegram_app

    return telegram_app.telegram_bot_sendtext(
        bot_message,
        purpose)
    
    
async def saving_traded_orders(
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

    # record trading transaction
    if "-FS-" not in instrument_name:
        await insert_tables(
                table, 
                trade
                )
        
    filter_trade="order_id"
            
    order_id = trade[f"{filter_trade}"]
        
    # remove respective transaction from order db            
    await deleting_row (
        order_db_table,
        "databases/trading.sqlite3",
        filter_trade,
        "=",
        order_id,
                )
    
    # just in case manual transactions without label
    try:
        
        label= trade["label"]
        
        # check if the transaction was a closing transaction and need to cleaned up
        if "my_trades_all_json" in table:
            
            if   "closed" in label:
                                        
                    await clean_up_closed_transactions(
                        instrument_name,
                        table
                        )
            
    except:
        pass # no need to do anything
        

async def saving_order_based_on_state(
    order_table: str,
    order: dict
    ) -> None:
    
    """_summary_

    Args:
        trades (_type_): _description_
        orders (_type_): _description_
        
    Examples:
        
        original=  {'jsonrpc': '2.0', 'id': 1002, 'result': {'trades': [], 'order': {'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit', 'creation_timestamp': 1728090482863, 'order_state': 'open', 'reject_post_only': False, 'contracts': 5.0, 'average_price': 0.0, 'reduce_only': False, 'last_update_timestamp': 1728090482863, 'filled_amount': 0.0, 'post_only': True, 'replaced': False, 'mmp': False, 'order_id': 'ETH-49960097702', 'web': False, 'api': True, 'instrument_name': 'ETH-PERPETUAL', 'max_show': 5.0, 'time_in_force': 'good_til_cancelled', 'direction': 'sell', 'amount': 5.0, 'price': 2424.05, 'label': 'hedgingSpot-open-1728090482812'}}, 'usIn': 1728090482862653, 'usOut': 1728090482864640, 'usDiff': 1987, 'testnet': False}
        cancelled=  {'jsonrpc': '2.0', 'id': 1002, 'result': {'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit', 'creation_timestamp': 1728090482863, 'order_state': 'cancelled', 'reject_post_only': False, 'contracts': 5.0, 'average_price': 0.0, 'reduce_only': False, 'last_update_timestamp': 1728090483773, 'filled_amount': 0.0, 'post_only': True, 'replaced': False, 'mmp': False, 'cancel_reason': 'user_request', 'order_id': 'ETH-49960097702', 'web': False, 'api': True, 'instrument_name': 'ETH-PERPETUAL', 'max_show': 5.0, 'time_in_force': 'good_til_cancelled', 'direction': 'sell', 'amount': 5.0, 'price': 2424.05, 'label': 'hedgingSpot-open-1728090482812'}, 'usIn': 1728090483773107, 'usOut': 1728090483774372, 'usDiff': 1265, 'testnet': False}
        
    """

    filter_trade="order_id"

    order_id = order[f"{filter_trade}"]
    
    order_state= order["order_state"]
    
    if order_state == "cancelled" or order_state == "filled":
        
        await deleting_row (
            order_table,
            "databases/trading.sqlite3",
            filter_trade,
            "=",
            order_id,
            )
        
    if order_state == "open":
        await insert_tables(
            order_table, 
            order
            )
            
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

        
def labelling_unlabelled_order(order: dict) -> None:

    from strategies.basic_strategy import (
        get_transaction_side,
        )
    
    side= get_transaction_side(order)
    order.update({"everything_is_consistent": True})
    order.update({"order_allowed": True})
    order.update({"entry_price": order["price"]})
    order.update({"size": order["amount"]})
    order.update({"type": "limit"})
    order.update({"side": side})
    
    label_open: str = get_custom_label(order)
    order.update({"label": label_open})
    
    return order

