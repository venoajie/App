# -*- coding: utf-8 -*-

# built ins
import asyncio
from loguru import logger as log

# user defined formulas
from db_management import sqlite_management as db_mgt
from messaging import telegram_bot as tlgrm
from transaction_management.deribit import (
    api_requests,
    cancelling_active_orders,
)
from utilities import (
    pickling,
    string_modification as str_mod,
    system_tools,
    time_modification as time_mod,
)


async def initial_procedures(
    private_data: object,
    config_app: list,
) -> None:

    try:

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]

        strategy_attributes = config_app["strategies"]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        cancellable_strategies = [
            o["strategy_label"] for o in strategy_attributes if o["cancellable"] == True
        ]

        # get ALL traded currencies in deribit
        get_currencies_all = await api_requests.get_currencies()

        all_exc_currencies = [o["currency"] for o in get_currencies_all["result"]]

        server_time = time_mod.get_now_unix_time()

        ONE_SECOND = 1000

        one_minute = ONE_SECOND * 60

        five_days_ago = server_time - (one_minute * 60 * 24 * 5)

        my_path_cur = system_tools.provide_path_for_file("currencies")

        pickling.replace_data(
            my_path_cur,
            all_exc_currencies,
        )

        for currency in all_exc_currencies:

            instruments = await api_requests.get_instruments(currency)

            my_path_instruments = system_tools.provide_path_for_file(
                "instruments", currency
            )

            pickling.replace_data(
                my_path_instruments,
                instruments,
            )

        for currency in currencies:

            currency_lower = currency.lower()

            archive_db_table = f"my_trades_all_{currency_lower}_json"

            query_trades_active_basic = f"SELECT instrument_name, user_seq, timestamp, trade_id  FROM  {archive_db_table}"

            query_trades_active_where = f"WHERE instrument_name LIKE '%{currency}%'"

            query_trades = f"{query_trades_active_basic} {query_trades_active_where}"

            await cancelling_active_orders.cancel_the_cancellables(
                private_data,
                order_db_table,
                currency,
                cancellable_strategies,
            )

            my_trades_currency = await db_mgt.executing_query_with_return(query_trades)

            if my_trades_currency == []:

                await refill_db(
                    private_data,
                    archive_db_table,
                    currency,
                    five_days_ago,
                )

    except Exception as error:

        system_tools.parse_error_message(f"starter initial_procedures {error}")

        await tlgrm.telegram_bot_sendtext(
            f"starter initial_procedures {error}", "general_error"
        )


async def refill_db(
    private_data: object,
    archive_db_table: str,
    currency: str,
    five_days_ago: int,
) -> None:

    transaction_log = await private_data.get_transaction_log(
        currency,
        five_days_ago,
        1000,
        "trade",
    )

    for transaction in transaction_log:
        result = {}

        if "sell" in transaction["side"]:
            direction = "sell"

        if "buy" in transaction["side"]:
            direction = "buy"

        result.update({"trade_id": transaction["trade_id"]})
        result.update({"user_seq": transaction["user_seq"]})
        result.update({"side": transaction["side"]})
        result.update({"timestamp": transaction["timestamp"]})
        result.update({"position": transaction["position"]})
        result.update({"amount": transaction["amount"]})
        result.update({"order_id": transaction["order_id"]})
        result.update({"price": transaction["price"]})
        result.update({"instrument_name": transaction["instrument_name"]})
        result.update({"label": None})
        result.update({"direction": direction})

        await db_mgt.insert_tables(
            archive_db_table,
            result,
        )


async def initial_data(
    private_data: object,
    currencies: list,
    redis_channels: dict,
) -> None:

    try:

        portfolio_channel: str = redis_channels["portfolio"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]

        query_trades = f"SELECT * FROM  v_trading_all_active"

        my_trades_active_from_db = await db_mgt.executing_query_with_return(
            query_trades
        )

        result_template = str_mod.message_template()

        # get portfolio from exchg
        portfolio_from_exchg = await private_data.get_subaccounts()

        # sub_account_combining
        sub_accounts = [
            await private_data.get_subaccounts_details(o) for o in currencies
        ]
        
        sub_account_combined = sub_account_combining(
                sub_accounts,
                sub_account_cached_channel,
                result_template,
            )
        
        log.error(f"sub_account_combined {sub_account_combined}")

        my_trades_active_all=my_trades_active_combining(
                my_trades_active_from_db,
                my_trades_channel,
                result_template,
            )
        
        portfolio_all=portfolio_combining(
                portfolio_from_exchg,
                portfolio_channel,
                result_template,
            )
        
        combined_result = dict(
            sub_account_combined=sub_account_combined,
            my_trades_active_all=my_trades_active_all,
            portfolio_all=portfolio_all,
        )

        log.debug(combined_result)
        
        return combined_result

    except Exception as error:

        system_tools.parse_error_message(f"starter refill db {error}")

        await tlgrm.telegram_bot_sendtext(f"starter refill db-{error}", "general_error")


def portfolio_combining(
    portfolio_all: list,
    portfolio_channel: str,
    result_template: dict,
) -> dict:

    portfolio = [o["portfolio"] for o in portfolio_all if o["type"] == "subaccount"][0]

    result_template["params"].update({"data": portfolio})
    result_template["params"].update({"channel": portfolio_channel})

    return result_template


def my_trades_active_combining(
    my_trades_active_all: list,
    my_trades_channel: str,
    result_template: dict,
) -> dict:

    result_template["params"].update({"data": my_trades_active_all})
    result_template["params"].update({"channel": my_trades_channel})

    return result_template


def sub_account_combining(
    sub_accounts: list,
    sub_account_cached_channel: str,
    result_template: dict,
) -> dict:

    orders_cached = []
    positions_cached = []
    
    try:
        
        for sub_account in sub_accounts:
            
            # result = await private_data.get_subaccounts_details(currency)

            sub_account = sub_account[0]
            
            sub_account_orders = sub_account["open_orders"]
            
            if sub_account_orders:

                for order in sub_account_orders:

                    orders_cached.append(order)            
            
            sub_account_positions = sub_account["positions"]

            if sub_account_positions:

                for position in sub_account_positions:

                    positions_cached.append(position)

        sub_account = dict(
            orders_cached=orders_cached,
            positions_cached=positions_cached,
        )
        
        result_template["params"].update({"data": sub_account})
        result_template["params"].update({"channel": sub_account_cached_channel})
        
        log.warning(f"result_template {result_template}")

        return result_template
    
    except:

        sub_account = dict(
            orders_cached=[],
            positions_cached=[],
        )

        result_template["params"].update({"data": sub_account})
        result_template["params"].update({"channel": sub_account_cached_channel})

        return result_template
