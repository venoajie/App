# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import uvloop
import orjson
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from db_management.redis_client import saving_and_publishing_result
from db_management.sqlite_management import executing_query_with_return
from messaging.telegram_bot import telegram_bot_sendtext
from strategies.hedging.hedging_spot import (
    HedgingSpot,
    modify_hedging_instrument,
)
from utilities.number_modification import get_closest_value
from utilities.pickling import read_data
from utilities.string_modification import (
    parsing_label,
    remove_redundant_elements,
)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)


async def hedging_spot(
    client_redis: object,
    config_app: list,
    futures_instruments,
    redis_channels: list,
    strategy_attributes: list,
) -> None:
    """ """

    strategy = "hedgingSpot"

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        # get strategies that have not short/long attributes in the label
        non_checked_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["non_checked_for_size_label_consistency"] == True
        ]

        contribute_to_hedging_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["contribute_to_hedging"] == True
        ]

        instrument_attributes_futures_all = futures_instruments["active_futures"]

        relevant_tables = config_app["relevant_tables"][0]

        trade_db_table = relevant_tables["my_trades_table"]

        # get redis channels
        order_receiving_channel: str = redis_channels["order_receiving"]
        market_analytics_channel: str = redis_channels["market_analytics_update"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]
        sending_order_channel: str = redis_channels["order_rest"]
        order_allowed_channel: str = redis_channels["order_is_allowed"]
        positions_update_channel: str = redis_channels["position_cache_updating"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]

        # prepare channels placeholders
        channels = [
            market_analytics_channel,
            order_receiving_channel,
            ticker_cached_channel,
            portfolio_channel,
            my_trades_channel,
            order_allowed_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        cached_orders = []

        cached_ticker_all = []

        not_cancel = True

        market_condition_all = []

        portfolio_all = []

        query_trades = f"SELECT * FROM  v_trading_all_active"

        order_allowed = False

        while not_cancel:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if order_allowed_channel in message_channel:

                        log.warning (f"order_allowed {order_allowed}")

                        order_allowed = message_byte_data * order_allowed

                        log.critical (f"order_allowed {order_allowed}")

                    if market_analytics_channel in message_channel:

                        market_condition_all = message_byte_data

                    if portfolio_channel in message_channel:

                        portfolio_all = message_byte_data["cached_portfolio"]

                    if my_trades_channel in message_channel:

                        my_trades_active_all = await executing_query_with_return(
                            query_trades
                        )

                    if order_receiving_channel in message_channel:

                        cached_orders = message_byte_data["cached_orders"]

                    if (order_allowed
                        and ticker_cached_channel in message_channel
                        and market_condition_all
                        and portfolio_all
                        and strategy in active_strategies
                    ):


                        cached_ticker_all = message_byte_data["data"]

                        server_time = message_byte_data["server_time"]

                        currency, currency_upper = (
                            message_byte_data["currency"],
                            message_byte_data["currency_upper"],
                        )

                        currency_lower: str = currency

                        instrument_name_perpetual = f"{currency_upper}-PERPETUAL"

                        archive_db_table: str = f"my_trades_all_{currency_lower}_json"

                        # get portfolio data
                        portfolio = [
                            o for o in portfolio_all if currency_upper in o["currency"]
                        ][0]

                        equity: float = portfolio["equity"]

                        ticker_perpetual_instrument_name = [
                            o
                            for o in cached_ticker_all
                            if instrument_name_perpetual in o["instrument_name"]
                        ][0]

                        index_price = get_index(ticker_perpetual_instrument_name)

                        # sub_account_orders = sub_account["open_orders"]

                        market_condition = [
                            o
                            for o in market_condition_all
                            if instrument_name_perpetual in o["instrument_name"]
                        ][0]

                        my_trades_currency_all_transactions: list = (
                            []
                            if not my_trades_active_all
                            else [
                                o
                                for o in my_trades_active_all
                                if currency_upper in o["instrument_name"]
                            ]
                        )

                        my_trades_currency_all: list = (
                            []
                            if my_trades_currency_all_transactions == 0
                            else [
                                o
                                for o in my_trades_currency_all_transactions
                                if o["instrument_name"]
                                in [
                                    o["instrument_name"]
                                    for o in instrument_attributes_futures_all
                                ]
                            ]
                        )

                        orders_currency = (
                            []
                            if not cached_orders
                            else [
                                o
                                for o in cached_orders
                                if currency_upper in o["instrument_name"]
                            ]
                        )

                        len_cleaned_orders = len(orders_currency)

                        if index_price is not None and equity > 0:
                            my_trades_currency: list = [
                                o
                                for o in my_trades_currency_all
                                if o["label"] is not None
                            ]

                            my_trades_currency_contribute_to_hedging = [
                                o
                                for o in my_trades_currency
                                if (parsing_label(o["label"])["main"])
                                in contribute_to_hedging_strategies
                            ]

                            my_trades_currency_contribute_to_hedging_sum = (
                                0
                                if not my_trades_currency_contribute_to_hedging
                                else sum(
                                    [
                                        o["amount"]
                                        for o in my_trades_currency_contribute_to_hedging
                                    ]
                                )
                            )

                            ONE_PCT = 1 / 100

                            INSTRUMENT_EXPIRATION_THRESHOLD = 60 * 8  # 8 hours

                            ONE_SECOND = 1000

                            ONE_MINUTE = ONE_SECOND * 60

                            notional: float = compute_notional_value(
                                index_price, equity
                            )

                            strategy_params = [
                                o
                                for o in strategy_attributes
                                if o["strategy_label"] == strategy
                            ][0]

                            my_trades_currency_strategy = [
                                o
                                for o in my_trades_currency
                                if strategy in (o["label"])
                            ]

                            orders_currency_strategy = (
                                []
                                if not orders_currency
                                else [
                                    o
                                    for o in orders_currency
                                    if strategy in (o["label"])
                                ]
                            )

                            # log.info(f" {currency} {len(orders_currency_strategy)}  {(orders_currency_strategy)} ")

                            instrument_attributes_futures_for_hedging = [
                                o
                                for o in futures_instruments["active_futures"]
                                if o["settlement_period"] != "month"
                                and o["kind"] == "future"
                            ]

                            strong_bearish = market_condition["strong_bearish"]

                            bearish = market_condition["bearish"]

                            max_position: int = notional * -1

                            instrument_ticker = await modify_hedging_instrument(
                                strong_bearish,
                                bearish,
                                instrument_attributes_futures_for_hedging,
                                cached_ticker_all,
                                ticker_perpetual_instrument_name,
                                currency_upper,
                            )

                            instrument_name = instrument_ticker["instrument_name"]

                            instrument_time_left = (
                                max(
                                    [
                                        o["expiration_timestamp"]
                                        for o in instrument_attributes_futures_all
                                        if o["instrument_name"] == instrument_name
                                    ]
                                )
                                - server_time
                            ) / ONE_MINUTE

                            instrument_time_left_exceed_threshold = (
                                instrument_time_left > INSTRUMENT_EXPIRATION_THRESHOLD
                            )

                            hedging = HedgingSpot(
                                strategy,
                                strategy_params,
                                max_position,
                                my_trades_currency_strategy,
                                market_condition,
                                index_price,
                                my_trades_currency_all,
                            )

                            # something was wrong because perpetuals were actively traded. cancell  orders
                            if (
                                order_allowed
                                and instrument_time_left_exceed_threshold
                                and len_cleaned_orders < 50
                            ):
                                async with client_redis.pipeline() as pipe:

                                    best_ask_prc: float = instrument_ticker[
                                        "best_ask_price"
                                    ]

                                    send_order: dict = (
                                        await hedging.is_send_open_order_allowed(
                                            non_checked_strategies,
                                            instrument_name,
                                            instrument_attributes_futures_for_hedging,
                                            orders_currency_strategy,
                                            best_ask_prc,
                                            archive_db_table,
                                            trade_db_table,
                                        )
                                    )

                                    if send_order["order_allowed"]:

                                        await saving_and_publishing_result(
                                            pipe,
                                            sending_order_channel,
                                            None,
                                            None,
                                            send_order,
                                        )

                                        # not_order = False

                                        break

                                    status_transaction = [
                                        "open",
                                        "closed",
                                    ]

                                    if len_cleaned_orders < 50:

                                        # log.error (f"{orders_currency_strategy} ")

                                        for status in status_transaction:

                                            my_trades_currency_strategy_status = [
                                                o
                                                for o in my_trades_currency_strategy
                                                if status in (o["label"])
                                            ]

                                            orders_currency_strategy_label_contra_status = [
                                                o
                                                for o in orders_currency_strategy
                                                if status not in o["label"]
                                            ]

                                            # log.error (f"{status} ")

                                            if my_trades_currency_strategy_status:

                                                transaction_instrument_name = remove_redundant_elements(
                                                    [
                                                        o["instrument_name"]
                                                        for o in my_trades_currency_strategy_status
                                                    ]
                                                )

                                                for (
                                                    instrument_name
                                                ) in transaction_instrument_name:

                                                    instrument_ticker: list = [
                                                        o
                                                        for o in cached_ticker_all
                                                        if instrument_name
                                                        in o["instrument_name"]
                                                    ]

                                                    if instrument_ticker:

                                                        instrument_ticker = (
                                                            instrument_ticker[0]
                                                        )

                                                        get_prices_in_label_transaction_main = [
                                                            o["price"]
                                                            for o in my_trades_currency_strategy_status
                                                            if instrument_name
                                                            in o["instrument_name"]
                                                        ]

                                                        log.error(
                                                            f"my_trades_currency_contribute_to_hedging_sum {my_trades_currency_contribute_to_hedging_sum}"
                                                        )

                                                        if (
                                                            status == "open"
                                                            and my_trades_currency_contribute_to_hedging_sum
                                                            <= 0
                                                        ):

                                                            best_bid_prc: (
                                                                float
                                                            ) = instrument_ticker[
                                                                "best_bid_price"
                                                            ]

                                                            closest_price = get_closest_value(
                                                                get_prices_in_label_transaction_main,
                                                                best_bid_prc,
                                                            )

                                                            nearest_transaction_to_index = [
                                                                o
                                                                for o in my_trades_currency_strategy_status
                                                                if o["price"]
                                                                == closest_price
                                                            ]

                                                            send_closing_order: (
                                                                dict
                                                            ) = await hedging.is_send_exit_order_allowed(
                                                                orders_currency_strategy_label_contra_status,
                                                                best_bid_prc,
                                                                nearest_transaction_to_index,
                                                                # orders_currency_strategy
                                                            )

                                                            if send_order[
                                                                "order_allowed"
                                                            ]:

                                                                await saving_and_publishing_result(
                                                                    pipe,
                                                                    sending_order_channel,
                                                                    None,
                                                                    None,
                                                                    send_order,
                                                                )

                                                                # not_order = False

                                                                break

                                                        if status == "closed":

                                                            best_ask_prc: (
                                                                float
                                                            ) = instrument_ticker[
                                                                "best_ask_price"
                                                            ]

                                                            closest_price = get_closest_value(
                                                                get_prices_in_label_transaction_main,
                                                                best_ask_prc,
                                                            )

                                                            nearest_transaction_to_index = [
                                                                o
                                                                for o in my_trades_currency_strategy_status
                                                                if o["price"]
                                                                == closest_price
                                                            ]

                                                            send_closing_order: (
                                                                dict
                                                            ) = await hedging.send_contra_order_for_orphaned_closed_transctions(
                                                                orders_currency_strategy_label_contra_status,
                                                                best_ask_prc,
                                                                nearest_transaction_to_index,
                                                                # orders_currency_strategy
                                                            )

                                                            if send_order[
                                                                "order_allowed"
                                                            ]:

                                                                await saving_and_publishing_result(
                                                                    pipe,
                                                                    sending_order_channel,
                                                                    None,
                                                                    None,
                                                                    send_order,
                                                                )

                                                                # not_order = False
                                                                # )

                                                                break

            except Exception as error:

                parse_error_message(error)

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(f"app hedging spot {error}")

        await telegram_bot_sendtext(f"app hedging spot-{error}", "general_error")


def reading_from_pkl_data(end_point, currency, status: str = None) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, currency, status)
    return read_data(path)


def compute_notional_value(index_price: float, equity: float) -> float:
    """ """
    return index_price * equity


def get_index(ticker: dict) -> float:

    try:

        index_price = ticker["index_price"]

    except:

        index_price = []

    if index_price == []:
        index_price = ticker["estimated_delivery_price"]

    return index_price
