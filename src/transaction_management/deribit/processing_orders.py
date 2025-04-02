# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import orjson
from loguru import logger as log

# user defined formula
from db_management import redis_client, sqlite_management as db_mgt
from messaging import subscribing_to_channels, telegram_bot as tlgrm
from strategies import basic_strategy
from transaction_management.deribit import cancelling_active_orders as cancel_order
from utilities import caching, string_modification as str_mod, system_tools as tools


async def processing_orders(
    private_data: object,
    client_redis: object,
    cancellable_strategies: list,
    currencies: list,
    initial_data_subaccount: dict,
    order_db_table: str,
    redis_channels: list,
    strategy_attributes: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # subscribe to channels
        await subscribing_to_channels.redis_channels(
            pubsub,
            redis_channels,
            "processing_orders",
        )

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        # get strategies that have not short/long attributes in the label
        non_checked_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["non_checked_for_size_label_consistency"] == True
        ]

        # get redis channels
        order_rest_channel: str = redis_channels["order_rest"]
        my_trade_receiving_channel: str = redis_channels["my_trade_receiving"]
        order_update_channel: str = redis_channels["order_cache_updating"]
        portfolio_channel: str = redis_channels["portfolio"]
        sqlite_updating_channel: str = redis_channels["sqlite_record_updating"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]

        not_cancel = True

        query_trades = f"SELECT * FROM  v_trading_all_active"

        sub_account_cached_params = initial_data_subaccount["params"]

        sub_account_cached = sub_account_cached_params["data"]

        orders_cached = sub_account_cached["orders_cached"]

        positions_cached = sub_account_cached["positions_cached"]

        ordered = []

        from loguru import logger as log

        while not_cancel:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    params = message_byte_data["params"]

                    data = params["data"]

                    message_channel = params["channel"]

                    if my_trade_receiving_channel in message_channel:

                        log.critical(message_channel)
                        log.error(data)

                        for trade in data:

                            currency_lower: str = trade["fee_currency"].lower()

                            archive_db_table = f"my_trades_all_{currency_lower}_json"

                            await cancel_order.cancel_the_cancellables(
                                private_data,
                                order_db_table,
                                currency_lower,
                                cancellable_strategies,
                            )

                            await saving_traded_orders(
                                trade,
                                archive_db_table,
                                order_db_table,
                            )

                    if order_rest_channel in message_channel:

                        log.critical(message_channel)
                        log.warning(data)

                        if data["order_allowed"]:

                            await if_order_is_true(
                                private_data,
                                non_checked_strategies,
                                data,
                                ordered,
                            )

                    if order_update_channel in message_channel:

                        data = data["current_order"]
                        
                        log.critical(message_channel)
                        log.warning(data)
                        
                        if "oto_order_ids" in data:

                            await saving_oto_order(
                                private_data,
                                non_checked_strategies,
                                [data],
                                order_db_table,
                                ordered,
                            )

                        else:
                            
                            log.debug(data)

                            if "OTO" not in data["order_id"]:

                                label = data["label"]

                                order_id = data["order_id"]
                                order_state = data["order_state"]

                                # no label
                                if label == "":

                                    await cancelling_and_relabelling(
                                        private_data,
                                        non_checked_strategies,
                                        order_db_table,
                                        data,
                                        label,
                                        order_state,
                                        order_id,
                                        ordered,
                                    )

                                else:
                                    label_and_side_consistent = (
                                        basic_strategy.is_label_and_side_consistent(
                                            non_checked_strategies, data
                                        )
                                    )

                                    if label_and_side_consistent and label:

                                        # log.debug(f"order {order}")

                                        await saving_order_based_on_state(
                                            order_db_table,
                                            data,
                                        )

                                    # check if transaction has label. Provide one if not any
                                    if not label_and_side_consistent:

                                        if (
                                            order_state != "cancelled"
                                            or order_state != "filled"
                                        ):
                                            await cancel_order.cancel_by_order_id(
                                                private_data,
                                                order_db_table,
                                                order_id,
                                            )

                                            # log.error (f"order {order}")
                                            await db_mgt.insert_tables(
                                                order_db_table,
                                                data,
                                            )

                        for currency in currencies:

                            result = await private_data.get_subaccounts_details(
                                currency
                            )

                            await updating_sub_account(
                                client_redis,
                                orders_cached,
                                positions_cached,
                                query_trades,
                                result,
                                sub_account_cached_channel,
                                message_byte_data,
                            )

                    if (
                        sqlite_updating_channel in message_channel
                        or portfolio_channel in message_channel
                    ):
                        for currency in currencies:

                            result = await private_data.get_subaccounts_details(
                                currency
                            )

                            await updating_sub_account(
                                client_redis,
                                orders_cached,
                                positions_cached,
                                query_trades,
                                result,
                                sub_account_cached_channel,
                                message_byte_data,
                            )

            except Exception as error:

                tools.parse_error_message(error)

                await tlgrm.telegram_bot_sendtext(
                    f"cancelling active orders - {error}",
                    "general_error",
                )

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        tools.parse_error_message(f"procesing orders {error}")

        await tlgrm.telegram_bot_sendtext(
            f"processing order - {error}",
            "general_error",
        )


async def if_order_is_true(
    private_data: object,
    non_checked_strategies: list,
    order: dict,
    ordered: list,
) -> None:
    """ """

    if order["order_allowed"]:

        # get parameter orders
        try:
            params: dict = order["order_parameters"]
        except:
            params: dict = order

        label_and_side_consistent: bool = basic_strategy.is_label_and_side_consistent(
            non_checked_strategies,
            params,
        )

        if label_and_side_consistent:

            label = str_mod.parsing_label(params["label"])["main"]
            log.warning(f"params {params}")
            log.debug(f"label {label}")
            label_has_order_id = [o["order_id"] for o in ordered if label in o["label"]]
            log.debug(f"label_has_order_id {label_has_order_id}")

            if label_has_order_id:
                ordered.remove(params)

            else:
                if ordered == []:
                    send_limit_result = await private_data.send_limit_order(params)

                    return send_limit_result

                else:
                    ordered.append(params)

                    return []

            # await asyncio.sleep(10)
        else:

            return []
            # await asyncio.sleep(10)


async def cancelling_and_relabelling(
    private_data,
    non_checked_strategies,
    order_db_table,
    order,
    label,
    order_state,
    order_id,
    ordered,
) -> None:

    # log.debug(f"label {label} order_state {order_state} order {order}")

    # no label
    if label == "":

        # log.info(label == "")

        if "open" in order_state or "untriggered" in order_state:

            if "OTO" not in order["order_id"]:

                # log.error (f"order {order}")
                await db_mgt.insert_tables(
                    order_db_table,
                    order,
                )

                await cancel_order.cancel_by_order_id(
                    private_data,
                    order_db_table,
                    order_id,
                )

            order_attributes = labelling_unlabelled_order(order)
            # log.warning (f"order_attributes {order_attributes}")

            await if_order_is_true(
                private_data,
                non_checked_strategies,
                order_attributes,
                ordered,
            )


def labelling_unlabelled_order(order: dict) -> None:

    from strategies.basic_strategy import get_transaction_side

    type = order["order_type"]

    side = get_transaction_side(order)
    order.update({"everything_is_consistent": True})
    order.update({"order_allowed": True})
    order.update({"entry_price": order["price"]})
    order.update({"size": order["amount"]})
    order.update({"type": type})

    if type != "limit":  # limit has various state
        order.update({"trigger_price": order["trigger_price"]})
        order.update({"trigger": order["trigger"]})

    order.update({"side": side})

    label_open: str = get_custom_label(order)
    order.update({"label": label_open})

    return order


def labelling_unlabelled_order_oto(
    transaction_main: list,
    transaction_secondary: list,
) -> None:
    """

    orders_example= [
        {
            'oto_order_ids': ['OTO-80322590'], 'is_liquidation': False, 'risk_reducing': False,
            'order_type': 'limit', 'creation_timestamp': 1733172624209, 'order_state': 'open',
            'reject_post_only': False, 'contracts': 1.0, 'average_price': 0.0, 'reduce_only': False,
            'trigger_fill_condition': 'incremental', 'last_update_timestamp': 1733172624209,
            'filled_amount': 0.0, 'replaced': False, 'post_only': True, 'mmp': False, 'web': True,
            'api': False, 'instrument_name': 'BTC-PERPETUAL', 'max_show': 10.0, 'time_in_force': 'good_til_cancelled',
            'direction': 'buy', 'amount': 10.0, 'order_id': '81944428472', 'price': 90000.0, 'label': ''},
        {
            'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit',
            'creation_timestamp': 1733172624177, 'order_state': 'untriggered', 'average_price': 0.0,
            'reduce_only': False, 'trigger_fill_condition': 'incremental', 'last_update_timestamp': 1733172624177,
            'filled_amount': 0.0, 'is_secondary_oto': True, 'replaced': False, 'post_only': False, 'mmp': False,
            'web': True, 'api': False, 'instrument_name': 'BTC-PERPETUAL', 'max_show': 10.0,
            'time_in_force': 'good_til_cancelled', 'direction': 'sell', 'amount': 10.0,
            'order_id': 'OTO-80322590', 'price': 100000.0, 'label': ''}
        ]


    """

    label: str = get_custom_label_oto(transaction_main)

    label_open: str = label["open"]
    label_closed: str = label["closed"]
    instrument_name = transaction_main["instrument_name"]
    secondary_params = [
        {
            "amount": transaction_secondary["amount"],
            "direction": (transaction_secondary["direction"]),
            "type": "limit",
            "instrument_name": instrument_name,
            "label": label_closed,
            "price": transaction_secondary["price"],
            "time_in_force": "good_til_cancelled",
            "post_only": True,
        }
    ]

    params = {}
    params.update({"everything_is_consistent": True})
    params.update({"instrument_name": instrument_name})
    params.update({"type": "limit"})
    params.update({"entry_price": transaction_main["price"]})
    params.update({"size": transaction_main["amount"]})
    params.update({"label": label_open})
    params.update({"side": transaction_main["direction"]})
    params.update({"otoco_config": secondary_params})

    try:
        params.update({"linked_order_type": transaction_main["linked_order_type"]})

    except:
        pass  # default: one_triggers_other at API request

    return dict(
        order_allowed=True,
        order_parameters=params,
    )


def get_custom_label_oto(transaction: list) -> dict:

    side = transaction["direction"]
    side_label = "Short" if side == "sell" else "Long"

    try:
        last_update = transaction["timestamp"]
    except:
        try:
            last_update = transaction["last_update_timestamp"]
        except:
            last_update = transaction["creation_timestamp"]

    return dict(
        open=(f"custom{side_label.title()}-open-{last_update}"),
        closed=(f"custom{side_label.title()}-closed-{last_update}"),
    )


def get_custom_label(transaction: list) -> str:

    side = transaction["direction"]
    side_label = "Short" if side == "sell" else "Long"

    try:
        last_update = transaction["timestamp"]
    except:
        try:
            last_update = transaction["last_update_timestamp"]
        except:
            last_update = transaction["creation_timestamp"]

    return f"custom{side_label.title()}-open-{last_update}"


async def saving_order_based_on_state(
    order_table: str,
    order: dict,
    db: str = "sqlite",
) -> None:
    """
    db: "sqlite"
        "redis"

    _summary_

    Args:
        trades (_type_): _description_
        orders (_type_): _description_

    Examples:

        original=  {
            'jsonrpc': '2.0',
            'id': 1002,
            'result': {
                'trades': [],
                'order': {
                    'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit',
                    'creation_timestamp': 1728090482863, 'order_state': 'open', 'reject_post_only': False,
                    'contracts': 5.0, 'average_price': 0.0, 'reduce_only': False,
                    'last_update_timestamp': 1728090482863, 'filled_amount': 0.0, 'post_only': True,
                    'replaced': False, 'mmp': False, 'order_id': 'ETH-49960097702', 'web': False, 'api': True,
                    'instrument_name': 'ETH-PERPETUAL', 'max_show': 5.0, 'time_in_force': 'good_til_cancelled',
                    'direction': 'sell', 'amount': 5.0, 'price': 2424.05, 'label': 'hedgingSpot-open-1728090482812'
                    }
                    },
                    'usIn': 1728090482862653,
                    'usOut': 1728090482864640,
                    'usDiff': 1987,
                    'testnet': False
                    }

        cancelled=  {
            'jsonrpc': '2.0',
            'id': 1002,
            'result': {
                'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit',
                'creation_timestamp': 1728090482863, 'order_state': 'cancelled','reject_post_only': False,
                'contracts': 5.0, 'average_price': 0.0,'reduce_only': False,
                'last_update_timestamp': 1728090483773, 'filled_amount': 0.0,'post_only': True,
                'replaced': False, 'mmp': False, 'cancel_reason': 'user_request','order_id': 'ETH-49960097702',
                'web': False, 'api': True, 'instrument_name': 'ETH-PERPETUAL',
                'max_show': 5.0, 'time_in_force': 'good_til_cancelled', 'direction': 'sell', 'amount': 5.0,
                'price': 2424.05, 'label': 'hedgingSpot-open-1728090482812'},
                'usIn': 1728090483773107,
                'usOut': 1728090483774372,
                'usDiff': 1265,
                'testnet': False
                }

    """

    filter_trade = "order_id"

    order_id = order[f"{filter_trade}"]

    order_state = order["order_state"]

    if order_state == "cancelled" or order_state == "filled":

        await db_mgt.deleting_row(
            order_table,
            "databases/trading.sqlite3",
            filter_trade,
            "=",
            order_id,
        )

    if order_state == "open":

        # log.error (f"order {order}")
        await db_mgt.insert_tables(
            order_table,
            order,
        )


async def saving_traded_orders(
    trade_result: str,
    trade_table: str,
    order_db_table: str,
) -> None:
    """_summary_

    Args:
        trades (_type_): _description_
        orders (_type_): _description_
    """

    filter_trade = "order_id"

    order_id = trade_result[f"{filter_trade}"]

    # remove respective transaction from order db
    await db_mgt.deleting_row(
        order_db_table,
        "databases/trading.sqlite3",
        filter_trade,
        "=",
        order_id,
    )

    trade_to_db = trade_template()

    trade_to_db.update({"instrument_name": trade_result["instrument_name"]})
    trade_to_db.update({"amount": trade_result["amount"]})
    trade_to_db.update({"price": trade_result["price"]})
    trade_to_db.update({"direction": trade_result["direction"]})
    trade_to_db.update({"trade_id": trade_result["trade_id"]})
    trade_to_db.update({"order_id": trade_result["order_id"]})
    trade_to_db.update({"timestamp": trade_result["timestamp"]})

    try:
        trade_to_db.update({"label": trade_result["label"]})

    except:

        pass

    await db_mgt.insert_tables(
        trade_table,
        trade_to_db,
    )


async def saving_oto_order(
    private_data: object,
    non_checked_strategies: list,
    orders: list,
    order_db_table: str,
    ordered: list,
) -> None:

    print(f"saving_oto_order {orders}")
    transaction_main = [o for o in orders if "OTO" not in o["order_id"]][0]

    transaction_main_oto = transaction_main["oto_order_ids"][0]

    kind = "future"
    type = "trigger_all"

    open_orders_from_exchange = await private_data.get_open_orders(kind, type)

    transaction_secondary = [
        o for o in open_orders_from_exchange if transaction_main_oto in o["order_id"]
    ]

    if transaction_secondary:

        transaction_secondary = transaction_secondary[0]

        # no label
        if (
            transaction_main["label"] == ""
            and "open" in transaction_main["order_state"]
        ):

            # log.error (f"transaction_main {transaction_main}")
            await db_mgt.insert_tables(
                order_db_table,
                transaction_main,
            )

            order_attributes = labelling_unlabelled_order_oto(
                transaction_main, transaction_secondary
            )

            await cancel_order.cancel_by_order_id(
                private_data,
                order_db_table,
                transaction_main["order_id"],
            )

            await if_order_is_true(
                private_data,
                non_checked_strategies,
                order_attributes,
                ordered,
            )

        else:
            
            # log.error (f"transaction_main {transaction_main}")
            await db_mgt.insert_tables(
                order_db_table,
                transaction_main,
            )


async def updating_sub_account(
    client_redis: object,
    orders_cached: list,
    positions_cached: list,
    query_trades: str,
    subaccounts_details_result: list,
    sub_account_cached_channel: str,
    message_byte_data: dict,
) -> None:

    if subaccounts_details_result:

        open_orders = [o["open_orders"] for o in subaccounts_details_result]

        if open_orders:
            caching.update_cached_orders(
                orders_cached,
                open_orders[0],
                "rest",
            )
        positions = [o["positions"] for o in subaccounts_details_result]

        if positions:
            caching.positions_updating_cached(
                positions_cached,
                positions[0],
                "rest",
            )

    my_trades_active_all = await db_mgt.executing_query_with_return(query_trades)

    data = dict(
        positions=positions_cached,
        open_orders=orders_cached,
        my_trades=my_trades_active_all,
    )

    message_byte_data["params"].update({"channel": sub_account_cached_channel})
    message_byte_data["params"].update({"data": data})

    await redis_client.publishing_result(
        client_redis,
        sub_account_cached_channel,
        message_byte_data,
    )


def is_order_has_executed(
    ordered: list,
    orders_from_exchange: list,
) -> bool:

    order_has_executed = [
        o for o in ordered if o["label"] in [o["label"] for o in orders_from_exchange]
    ]

    if order_has_executed:
        ordered = []

    return True if order_has_executed else False


def trade_template() -> str:

    """
    combining result from websocket (user changes/trade)
        and rest API (get_transaction_log)

    """
    return dict(
        instrument_name=None,
        #                 label=None, commented as label header is needed as identification for no label
        amount=None,
        price=None,
        side=None,
        direction=None,
        position=None,
        currency=None,
        timestamp=None,
        trade_id=None,
        order_id=None,
        user_seq=None,
    )
