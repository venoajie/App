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
        
    

async def saving_orders(
    order_table: str,
    order: dict
    ) -> None:
    
    """_summary_

    Args:
        trades (_type_): _description_
        orders (_type_): _description_
    """

    filter_trade="order_id"

    order_id = order[f"{filter_trade}"]
    
    order_state= order["order_state"]
    
    if order_state == "cancelled" or order_state == "filled":
        
        original=  {'jsonrpc': '2.0', 'id': 1002, 'result': {'trades': [], 'order': {'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit', 'creation_timestamp': 1728090482863, 'order_state': 'open', 'reject_post_only': False, 'contracts': 5.0, 'average_price': 0.0, 'reduce_only': False, 'last_update_timestamp': 1728090482863, 'filled_amount': 0.0, 'post_only': True, 'replaced': False, 'mmp': False, 'order_id': 'ETH-49960097702', 'web': False, 'api': True, 'instrument_name': 'ETH-PERPETUAL', 'max_show': 5.0, 'time_in_force': 'good_til_cancelled', 'direction': 'sell', 'amount': 5.0, 'price': 2424.05, 'label': 'hedgingSpot-open-1728090482812'}}, 'usIn': 1728090482862653, 'usOut': 1728090482864640, 'usDiff': 1987, 'testnet': False}
        cancelled=  {'jsonrpc': '2.0', 'id': 1002, 'result': {'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit', 'creation_timestamp': 1728090482863, 'order_state': 'cancelled', 'reject_post_only': False, 'contracts': 5.0, 'average_price': 0.0, 'reduce_only': False, 'last_update_timestamp': 1728090483773, 'filled_amount': 0.0, 'post_only': True, 'replaced': False, 'mmp': False, 'cancel_reason': 'user_request', 'order_id': 'ETH-49960097702', 'web': False, 'api': True, 'instrument_name': 'ETH-PERPETUAL', 'max_show': 5.0, 'time_in_force': 'good_til_cancelled', 'direction': 'sell', 'amount': 5.0, 'price': 2424.05, 'label': 'hedgingSpot-open-1728090482812'}, 'usIn': 1728090483773107, 'usOut': 1728090483774372, 'usDiff': 1265, 'testnet': False}
        
        await deleting_row (
            order_table,
            "databases/trading.sqlite3",
            filter_trade,
            "=",
            order_id,
            )
        #await update_status_data(order_table, 
        #                     "order_state", 
        #                     filter_trade, 
        #                     order_id, 
        #                     "cancelled")

    if order_state == "open":
        await insert_tables(
            order_table, 
            order
            )