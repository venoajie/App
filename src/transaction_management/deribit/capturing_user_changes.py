#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
from multiprocessing.queues import Queue

import uvloop

# installed
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import SendApiRequest
from transaction_management.deribit.managing_deribit import ModifyOrderDb
from transaction_management.deribit.orders_management import saving_orders
from utilities.system_tools import parse_error_message


async def saving_and_relabelling_orders(sub_account_id, config_app: list, queue: Queue):
    """ """

    log.critical("saving_and_relabelling_orders START")

    try:
        private_data: str = SendApiRequest(sub_account_id)

        modify_order_and_db: object = ModifyOrderDb(sub_account_id)

        # currencies= random.sample(currencies_spot,len(currencies_spot))

        strategy_attributes = config_app["strategies"]

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        # get strategies that have not short/long attributes in the label
        non_checked_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["non_checked_for_size_label_consistency"] == True
        ]

        cancellable_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["cancellable"] == True
        ]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]
        
        current_open_orders = []
        
        checking_time = 0
        
        no_transaction = True
        
        while True:
            
            log.critical(queue)
            
            while not queue.empty():

                message: str = await queue.get()

                message_channel: str = message["channel"]

                data_orders: dict = message["data"]
                
                orders_from_data_producers = message["orders_all"]
                
                server_time = message["latest_timestamp"]
                
                CHECKING_TIME_THRESHOLD = 1000

                delta_time = server_time - checking_time
                
                currency: str = message["currency"]

                currency_lower: str = currency.lower()
                
                log.critical (message_channel)        
                
                if "user.changes.any" in message_channel:
                    
                    log.critical (message)

                    await saving_orders(
                        modify_order_and_db,
                        private_data,
                        cancellable_strategies,
                        non_checked_strategies,
                        data_orders,
                        order_db_table,
                        currency_lower,
                    )

                    await modify_order_and_db.resupply_sub_accountdb(currency)

                if delta_time > CHECKING_TIME_THRESHOLD:
                    
                    if len(orders_from_data_producers) != len(current_open_orders):
                        
                        delta_time = server_time
                        
                        for order in orders_from_data_producers:
                            order_in_current_open_orders = [o for o in current_open_orders]
                            
                            no_transaction = False
                        
                        if current_open_orders:
                            
                            for order in current_open_orders:
                                order_in_orders_from_data_producers = [o for o in orders_from_data_producers]
                                no_transaction = False
                    
                    else:
                        delta_time = server_time
                
                queue.task_done()
            
    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(error, "general_error")

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(error, "general_error")
