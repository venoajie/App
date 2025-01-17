#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
from multiprocessing.queues import Queue

# installed
from loguru import logger as log
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import (telegram_bot_sendtext,)
from transaction_management.deribit.api_requests import (
    SendApiRequest)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,)
from transaction_management.deribit.orders_management import (saving_orders,)
from utilities.system_tools import (parse_error_message)

    
async def saving_and_relabelling_orders(
    sub_account_id,
    config_app: list,
    queue: Queue
    ):
    
    """
    """
    
    log.critical ("saving_and_relabelling_orders START")
    
    try:
        private_data: str = SendApiRequest (sub_account_id)
        
        modify_order_and_db: object = ModifyOrderDb(sub_account_id)
        
        #currencies= random.sample(currencies_spot,len(currencies_spot))
        
        strategy_attributes = config_app["strategies"]
        
        strategy_attributes_active = [o for o in strategy_attributes \
            if o["is_active"]==True]
                        
        # get strategies that have not short/long attributes in the label 
        non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
            if o["non_checked_for_size_label_consistency"]==True]
        
        cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
            if o["cancellable"]==True]
        
        relevant_tables = config_app["relevant_tables"][0]
        
        order_db_table= relevant_tables["orders_table"]        
                   
        while True:
                 
            message: str = await queue.get()
            
            message_channel: str = message["channel"]
            
            data_orders: dict = message["data"] 
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency.lower()
                            
            if "user.changes.any" in message_channel:
                        
                await saving_orders(
                    modify_order_and_db,
                    private_data,
                    cancellable_strategies,
                    non_checked_strategies,
                    data_orders,
                    order_db_table,
                    currency_lower
                        )
                
                await modify_order_and_db.resupply_sub_accountdb(currency)
    
    except Exception as error:
        
        parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )

    except Exception as error:
        
        parse_error_message(error)  
        
        await telegram_bot_sendtext (
            error,
            "general_error"
            )
