# -*- coding: utf-8 -*-

# built ins
import asyncio
from random import sample

import orjson
import uvloop

# installed
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from data_cleaning.reconciling_db import is_size_sub_account_and_my_trades_reconciled
from db_management.sqlite_management import executing_query_with_return
from messaging.telegram_bot import telegram_bot_sendtext
from strategies.basic_strategy import get_label_integer
from strategies.cash_carry.combo_auto import (
    ComboAuto,
    check_if_minimum_waiting_time_has_passed,
)
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from transaction_management.deribit.processing_orders import processing_orders
from utilities.pickling import read_data
from utilities.string_modification import (
    remove_double_brackets_in_list,
    remove_redundant_elements,
)

from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)


async def future_spreads(
    private_data: object,
    modify_order_and_db: object,
    client_redis: object,
    config_app: list,
) -> None:
    """ """

    strategy = "futureSpread"

    try:

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get tradable currencies
        currencies = ([o["spot"] for o in tradable_config_app])[0]

        strategy_attributes = config_app["strategies"]

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_futures_instruments(
            currencies,
            settlement_periods,
        )

        instrument_attributes_futures_all = futures_instruments["active_futures"]

        instrument_attributes_combo_all = futures_instruments["active_combo"]

        instruments_name = futures_instruments["instruments_name"]

        server_time = 0

        pubsub: object = client_redis.pubsub()

        CHANNEL_NAME: str = "notification"

        not_cancel = True

        await pubsub.subscribe(CHANNEL_NAME)

        while not_cancel:

            try:

                message = await pubsub.get_message()

                if message and message["type"] == "message":

                    message = orjson.loads(message["data"])["message"]

                    log.debug(message["sequence"])

                    cached_orders, ticker_all = (
                        message["cached_orders"],
                        message["ticker_all"],
                    )

                    chart_trade, server_time = (
                        message["chart_trade"],
                        message["server_time"],
                    )

                    currency: str = message["currency"]

                    currency_lower: str = currency

                    currency_upper: str = currency.upper()

                    instrument_name_perpetual = f"{currency_upper}-PERPETUAL"

                    if not chart_trade and server_time != 0:

                        # get portfolio data
                        portfolio = reading_from_pkl_data("portfolio", currency)[0]

                        equity: float = portfolio["equity"]

                        ticker_perpetual_instrument_name = [
                            o
                            for o in ticker_all
                            if instrument_name_perpetual in o["instrument_name"]
                        ][0]

                        index_price = get_index(ticker_perpetual_instrument_name)

                        sub_account = reading_from_pkl_data("sub_accounts", currency)

                        sub_account = sub_account[0]

                        # sub_account_orders = sub_account["open_orders"]

                        market_condition = message["market_condition"]

                        if sub_account:

                            query_trades = (
                                f"SELECT * FROM  v_{currency_lower}_trading_active"
                            )

                            my_trades_currency_all_transactions: list = (
                                await executing_query_with_return(query_trades)
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

                            position = [o for o in sub_account["positions"]]

                            log.debug(f"cached_orders {cached_orders}")

                            log.warning(f"orders_currency {orders_currency}")

                            position_without_combo = [
                                o
                                for o in position
                                if f"{currency_upper}-FS" not in o["instrument_name"]
                            ]

                            size_perpetuals_reconciled = (
                                is_size_sub_account_and_my_trades_reconciled(
                                    position_without_combo,
                                    my_trades_currency_all,
                                    instrument_name_perpetual,
                                )
                            )

                            if index_price is not None and equity > 0:

                                my_trades_currency: list = [
                                    o
                                    for o in my_trades_currency_all
                                    if o["label"] is not None
                                ]

                                ONE_PCT = 1 / 100

                                THRESHOLD_DELTA_TIME_SECONDS = 120

                                THRESHOLD_MARKET_CONDITIONS_COMBO = 0.1 * ONE_PCT

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

                                log.info(
                                    f"orders_currency_strategy {len (orders_currency_strategy)}"
                                )

                                if (
                                    strategy in active_strategies
                                    and size_perpetuals_reconciled
                                ):

                                    extra = 3  # waiting minute before reorder  15 min

                                    BASIC_TICKS_FOR_AVERAGE_MOVEMENT: int = (
                                        strategy_params[
                                            "waiting_minute_before_relabelling"
                                        ]
                                        + extra
                                    )

                                    AVERAGE_MOVEMENT: float = 0.15 / 100

                                    monthly_target_profit = strategy_params[
                                        "monthly_profit_pct"
                                    ]

                                    max_order_currency = 2

                                    random_instruments_name = sample(
                                        (
                                            [
                                                o
                                                for o in instruments_name
                                                if "-FS-" not in o
                                                and currency_upper in o
                                            ]
                                        ),
                                        max_order_currency,
                                    )

                                    combo_auto = ComboAuto(
                                        strategy,
                                        strategy_params,
                                        orders_currency_strategy,
                                        server_time,
                                        market_condition,
                                        my_trades_currency_strategy,
                                        ticker_perpetual_instrument_name,
                                    )

                                    my_trades_currency_strategy_labels: list = [
                                        o["label"] for o in my_trades_currency_strategy
                                    ]

                                    # send combo orders
                                    future_control = []
                                    for (
                                        instrument_attributes_combo
                                    ) in instrument_attributes_combo_all:

                                        try:
                                            instrument_name_combo = (
                                                instrument_attributes_combo[
                                                    "instrument_name"
                                                ]
                                            )

                                        except:
                                            instrument_name_combo = None

                                        if (
                                            instrument_name_combo
                                            and currency_upper in instrument_name_combo
                                        ):

                                            instrument_name_future = (
                                                f"{currency_upper}-{instrument_name_combo[7:][:7]}"
                                            ).strip("_")

                                            instrument_time_left = (
                                                max(
                                                    [
                                                        o["expiration_timestamp"]
                                                        for o in instrument_attributes_futures_all
                                                        if o["instrument_name"]
                                                        == instrument_name_future
                                                    ]
                                                )
                                                - server_time
                                            ) / ONE_MINUTE

                                            instrument_time_left_exceed_threshold = (
                                                instrument_time_left
                                                > INSTRUMENT_EXPIRATION_THRESHOLD
                                            )

                                            size_future_reconciled = is_size_sub_account_and_my_trades_reconciled(
                                                position_without_combo,
                                                my_trades_currency_all,
                                                instrument_name_future,
                                            )

                                            ticker_combo = [
                                                o
                                                for o in ticker_all
                                                if instrument_name_combo
                                                in o["instrument_name"]
                                            ]

                                            ticker_future = [
                                                o
                                                for o in ticker_all
                                                if instrument_name_future
                                                in o["instrument_name"]
                                            ]

                                            future_control = future_control.append(
                                                    instrument_name_future
                                                )
                                            

                                            log.debug(
                                                f"future_control {future_control} instrument_name_combo {instrument_name_combo} instrument_name_future {instrument_name_future}"
                                            )

                                            instrument_name_future_not_in_control = [
                                                o
                                                for o in future_control
                                                if instrument_name_future not in o
                                            ]

                                            log.debug(
                                                f"instrument_name_future_not_in_control {instrument_name_future_not_in_control}"
                                            )

                                            if (
                                                instrument_name_future_not_in_control
                                                and len_cleaned_orders < 50
                                                and ticker_future
                                                and ticker_combo
                                            ):
                                                # and not reduce_only \

                                                ticker_combo, ticker_future = (
                                                    ticker_combo[0],
                                                    ticker_future[0],
                                                )

                                                if (
                                                    instrument_time_left_exceed_threshold
                                                    and instrument_name_future
                                                    in random_instruments_name
                                                    and size_future_reconciled
                                                ):

                                                    send_order: dict = await combo_auto.is_send_open_order_constructing_manual_combo_allowed(
                                                        ticker_future,
                                                        instrument_attributes_futures_all,
                                                        notional,
                                                        monthly_target_profit,
                                                        AVERAGE_MOVEMENT,
                                                        BASIC_TICKS_FOR_AVERAGE_MOVEMENT,
                                                        min(
                                                            1,
                                                            max_order_currency,
                                                        ),
                                                        market_condition,
                                                    )

                                                    if send_order["order_allowed"]:

                                                        await processing_orders(
                                                            modify_order_and_db,
                                                            config_app,
                                                            send_order,
                                                        )

                                                        not_order = False

                                                        break

                                    # get labels from active trades
                                    labels = remove_redundant_elements(
                                        my_trades_currency_strategy_labels
                                    )

                                    filter = "label"

                                    #! closing active trades
                                    for label in labels:

                                        label_integer: int = get_label_integer(label)
                                        selected_transaction = [
                                            o
                                            for o in my_trades_currency_strategy
                                            if str(label_integer) in o["label"]
                                        ]

                                        selected_transaction_amount = [
                                            o["amount"] for o in selected_transaction
                                        ]
                                        sum_selected_transaction = sum(
                                            selected_transaction_amount
                                        )
                                        len_selected_transaction = len(
                                            selected_transaction_amount
                                        )

                                        #! closing combo auto trading
                                        if "Auto" in label and len_cleaned_orders < 50:

                                            if sum_selected_transaction == 0:

                                                abnormal_transaction = [
                                                    o
                                                    for o in selected_transaction
                                                    if "closed" in o["label"]
                                                ]

                                                if not abnormal_transaction:
                                                    send_order: dict = await combo_auto.is_send_exit_order_allowed_combo_auto(
                                                        label,
                                                        instrument_attributes_combo_all,
                                                        THRESHOLD_MARKET_CONDITIONS_COMBO,
                                                    )

                                                    if send_order["order_allowed"]:

                                                        await processing_orders(
                                                            modify_order_and_db,
                                                            config_app,
                                                            send_order,
                                                        )

                                                        not_order = False

                                                        break

                                                else:
                                                    log.critical(
                                                        f"abnormal_transaction {abnormal_transaction}"
                                                    )

                                                    break

                                        else:

                                            #! closing unpaired transactions
                                            log.critical(
                                                f"sum_selected_transaction {sum_selected_transaction}"
                                            )
                                            if sum_selected_transaction != 0:

                                                if (
                                                    len_selected_transaction == 1
                                                    and "closed" not in label
                                                ):

                                                    send_order = []

                                                    if size_perpetuals_reconciled:

                                                        for (
                                                            transaction
                                                        ) in selected_transaction:

                                                            waiting_minute_before_ordering = (
                                                                strategy_params[
                                                                    "waiting_minute_before_cancel"
                                                                ]
                                                                * ONE_MINUTE
                                                            )

                                                            timestamp: int = (
                                                                transaction["timestamp"]
                                                            )

                                                            waiting_time_for_selected_transaction: (
                                                                bool
                                                            ) = (
                                                                check_if_minimum_waiting_time_has_passed(
                                                                    waiting_minute_before_ordering,
                                                                    timestamp,
                                                                    server_time,
                                                                )
                                                                * 2
                                                            )

                                                            instrument_name = (
                                                                transaction[
                                                                    "instrument_name"
                                                                ]
                                                            )

                                                            ticker_transaction = [
                                                                o
                                                                for o in ticker_all
                                                                if instrument_name
                                                                in o["instrument_name"]
                                                            ]

                                                            if (
                                                                ticker_transaction
                                                                and len_cleaned_orders
                                                                < 50
                                                            ):

                                                                TP_THRESHOLD = (
                                                                    THRESHOLD_MARKET_CONDITIONS_COMBO
                                                                    * 5
                                                                )

                                                                send_order: dict = await combo_auto.is_send_contra_order_for_unpaired_transaction_allowed(
                                                                    ticker_transaction[
                                                                        0
                                                                    ],
                                                                    instrument_attributes_futures_all,
                                                                    TP_THRESHOLD,
                                                                    transaction,
                                                                    waiting_time_for_selected_transaction,
                                                                    random_instruments_name,
                                                                )

                                                                if send_order[
                                                                    "order_allowed"
                                                                ]:

                                                                    await processing_orders(
                                                                        modify_order_and_db,
                                                                        config_app,
                                                                        send_order,
                                                                    )

                                                                    not_order = False

                                                                    break

            except Exception as error:

                parse_error_message(error)

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        await telegram_bot_sendtext(
            f"app future spreads - {error}",
            "general_error",
        )

        parse_error_message(error)


def get_settlement_period(strategy_attributes) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


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
