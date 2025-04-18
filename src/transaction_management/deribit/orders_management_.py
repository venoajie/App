# -*- coding: utf-8 -*-

# user defined formula
from db_management.sqlite_management import deleting_row, insert_tables
from strategies.basic_strategy import is_label_and_side_consistent

# from loguru import logger as log
from transaction_management.deribit.processing_orders import if_order_is_true
from transaction_management.deribit.cancelling_active_orders import (
    cancel_by_order_id,
    cancel_the_cancellables,
)


async def saving_traded_orders(
    trade: str,
    trade_table: str,
    order_db_table: str,
) -> None:
    """_summary_

    Args:
        trades (_type_): _description_
        orders (_type_): _description_
    """

    filter_trade = "order_id"

    order_id = trade[f"{filter_trade}"]

    # remove respective transaction from order db
    await deleting_row(
        order_db_table,
        "databases/trading.sqlite3",
        filter_trade,
        "=",
        order_id,
    )

    # record trading transaction
    # log.error (f"trade {trade}")
    await insert_tables(
        trade_table,
        trade,
    )


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

        await deleting_row(
            order_table,
            "databases/trading.sqlite3",
            filter_trade,
            "=",
            order_id,
        )

    if order_state == "open":

        # log.error (f"order {order}")
        await insert_tables(
            order_table,
            order,
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


async def saving_oto_order(
    private_data,
    non_checked_strategies,
    orders,
    order_db_table,
) -> None:

    len_oto_order_ids = len(orders[0]["oto_order_ids"])

    transaction_main = [o for o in orders if "OTO" not in o["order_id"]][0]

    if len_oto_order_ids == 1:
        pass

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
            await insert_tables(
                order_db_table,
                transaction_main,
            )

            order_attributes = labelling_unlabelled_order_oto(
                transaction_main, transaction_secondary
            )

            await cancel_by_order_id(
                private_data,
                order_db_table,
                transaction_main["order_id"],
            )

            await if_order_is_true(
                private_data,
                non_checked_strategies,
                order_attributes,
            )

        else:
            # log.error (f"transaction_main {transaction_main}")
            await insert_tables(
                order_db_table,
                transaction_main,
            )


async def saving_orders(
    private_data: object,
    cancellable_strategies: list,
    non_checked_strategies: list,
    data: dict,
    order_db_table: list,
    currency_lower: str,
    save_only: bool = True,
) -> None:

    trades = data["trades"]

    orders = data["orders"]

    if orders:

        # log.info (f"orders {orders}")

        if trades:

            archive_db_table = f"my_trades_all_{currency_lower}_json"

            await cancel_the_cancellables(
                private_data,
                order_db_table,
                currency_lower,
                cancellable_strategies,
            )

            for trade in trades:
                await saving_traded_orders(
                    trade,
                    archive_db_table,
                    order_db_table,
                )

        else:

            if "oto_order_ids" in (orders[0]):

                await saving_oto_order(
                    private_data,
                    non_checked_strategies,
                    orders,
                    order_db_table,
                )

            else:

                for order in orders:

                    # log.debug(f"order {order}")

                    if "OTO" not in order["order_id"]:

                        label = order["label"]

                        order_id = order["order_id"]
                        order_state = order["order_state"]

                        # no label
                        if label == "":

                            await cancelling_and_relabelling(
                                private_data,
                                non_checked_strategies,
                                order_db_table,
                                order,
                                label,
                                order_state,
                                order_id,
                            )

                        else:
                            label_and_side_consistent = is_label_and_side_consistent(
                                non_checked_strategies, order
                            )

                            if label_and_side_consistent and label:

                                # log.debug(f"order {order}")

                                await saving_order_based_on_state(
                                    order_db_table,
                                    order,
                                )

                            # check if transaction has label. Provide one if not any
                            if not label_and_side_consistent:

                                if (
                                    order_state != "cancelled"
                                    or order_state != "filled"
                                ):
                                    await cancel_by_order_id(
                                        private_data,
                                        order_db_table,
                                        order_id,
                                    )

                                    # log.error (f"order {order}")
                                    await insert_tables(
                                        order_db_table,
                                        order,
                                    )


async def cancelling_and_relabelling(
    private_data,
    non_checked_strategies,
    order_db_table,
    order,
    label,
    order_state,
    order_id,
) -> None:

    # log.debug(f"label {label} order_state {order_state} order {order}")

    # no label
    if label == "":

        # log.info(label == "")

        if "open" in order_state or "untriggered" in order_state:

            if "OTO" not in order["order_id"]:

                # log.error (f"order {order}")
                await insert_tables(
                    order_db_table,
                    order,
                )

                await cancel_by_order_id(
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
            )
