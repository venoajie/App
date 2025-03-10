# -*- coding: utf-8 -*-
import os

import pytest

# installed
import tomli

from strategies.basic_strategy import (
    are_size_and_order_appropriate,
    check_if_next_closing_size_will_not_exceed_the_original,
    compute_profit_usd,
    is_label_and_side_consistent,
    positions_and_orders,
    profit_usd_has_exceed_target,
    proforma_size,
    sum_order_under_closed_label_int,
)
from utilities.system_tools import provide_path_for_file


def get_config(file_name: str) -> list:
    """ """
    config_path = provide_path_for_file(file_name)

    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read = tomli.load(handle)
                return read
    except:
        return []


# parsing config file
# config_app = get_config("config_strategies.toml")

# strategy_attributes = config_app["strategies"]

# get strategies that have not short/long attributes in the label
# non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes \
#    if o["non_checked_for_size_label_consistency"]==True]

non_checked_strategies = ["futureSpread", "comboAuto", "custom"]

params1 = {"label": "hedgingSpot-open-100", "side": "sell"}
params2 = {"label": "customShort-open-100", "side": "sell"}
params3 = {"label": "comboAuto-open-100", "side": "sell"}
params4 = {"label": "comboAuto-open-100", "side": "buy"}
params5 = {"label": "futureSpread-open-100", "side": "buy"}
params6 = {"label": "futureSpread-open-100", "side": "sell"}
params7 = {"label": "customLong-open-100", "side": "buy"}
params8 = {"label": "customLong-open-100", "side": "sell"}
params9 = {"label": "hedgingSpot-open-100", "side": "buy"}


@pytest.mark.parametrize(
    "non_checked_strategies, params, expected",
    [
        (non_checked_strategies, params1, True),
        (non_checked_strategies, params2, True),
        (non_checked_strategies, params3, True),
        (non_checked_strategies, params4, True),
        (non_checked_strategies, params5, True),
        (non_checked_strategies, params6, True),
        (non_checked_strategies, params7, True),
        (non_checked_strategies, params8, True),
        (non_checked_strategies, params9, False),
    ],
)
def test_is_label_and_side_consistent(non_checked_strategies, params, expected):

    result = is_label_and_side_consistent(
        non_checked_strategies,
        params,
    )

    assert result == expected


@pytest.mark.parametrize(
    "closed_orders_label_strategy, label_integer_open, expected",
    [
        ([], [], 0),
    ],
)
def test_sum_order_under_closed_label_int(
    closed_orders_label_strategy, label_integer_open, expected
):

    result = sum_order_under_closed_label_int(
        closed_orders_label_strategy,
        label_integer_open,
    )

    assert result == expected


@pytest.mark.parametrize(
    "basic_size, net_size, next_size, expected",
    [
        (-1, -1, 1, True),
        (-10, 0, 10, True),
        (10, 0, -10, True),
        (-10, 6, 4, True),
        (-10, 4, 6, True),
        (-10, 6, -1, False),
        (-10, -6, -16, False),
        (10, 6, 16, False),
        (-10, 11, 1, False),
        (10, -11, -1, False),
    ],
)
def test_check_if_closing_size_will_not_exceed_the_original(
    basic_size, net_size, next_size, expected
):

    result = check_if_next_closing_size_will_not_exceed_the_original(
        basic_size,
        net_size,
        next_size,
    )

    assert result == expected


@pytest.mark.parametrize(
    "current_size_or_open_position, current_orders_size, expected",
    [
        (-100, -10, -110),
        (100, -10, 90),
    ],
)
def test_positions_and_orders(
    current_size_or_open_position, current_orders_size, expected
):

    result = positions_and_orders(
        current_size_or_open_position,
        current_orders_size,
    )

    assert result == expected


@pytest.mark.parametrize(
    "current_size_or_open_position, current_orders_size, next_orders_size, expected",
    [
        (-100, -10, -10, -120),
        (100, -10, 10, 100),
    ],
)
def test_proforma_size_add(
    current_size_or_open_position,
    current_orders_size,
    next_orders_size,
    expected,
):

    result = proforma_size(
        current_size_or_open_position,
        current_orders_size,
        next_orders_size,
    )

    assert result == expected


@pytest.mark.parametrize(
    "purpose, current_size_or_open_position, current_orders_size, next_orders_size, max_position, expected",
    [
        ("add_position", -100, -10, -10, -100, False),
        ("add_position", 0, 10, 10, 100, True),
        ("add_position", 0, 0, -10, -100, True),
        ("reduce_position", 0, -10, 10, 100, False),
        ("reduce_position", 0, -10, 10, -100, False),
        ("reduce_position", -50, 0, -10, None, False),
        ("reduce_position", -50, 0, 10, None, True),
        ("reduce_position", -100, 0, 10, 100, True),
    ],
)
def test_are_size_and_order_appropriate(
    purpose,
    current_size_or_open_position,
    current_orders_size,
    next_orders_size,
    max_position,
    expected,
):

    result = are_size_and_order_appropriate(
        purpose,
        current_size_or_open_position,
        current_orders_size,
        next_orders_size,
        max_position,
    )

    assert result == expected


@pytest.mark.parametrize(
    "transaction_price, currrent_price, size, side, expected",
    [
        (100_000, 99_950, -50, "sell", 0.025),
        (100_000, 99_899, -50, "sell", 0.0505),
        (100_000, 100_050, -50, "sell", -0.025),
        (100_000, 99_950, 50, "buy", -0.025),
        (100_000, 100_050, 50, "buy", 0.025),
    ],
)
def test_compute_profit(transaction_price, currrent_price, size, side, expected):

    result = compute_profit_usd(transaction_price, currrent_price, size, side)

    assert result == expected


@pytest.mark.parametrize(
    "target_profit, transaction_price, currrent_price, size, side, expected",
    [
        (0.1 / 100, 100_000, 99_950, -50, "sell", False),
        (0.1 / 100, 100_000, 99_899, -50, "sell", True),
        (0.1 / 100, 100_000, 100_050, -50, "sell", False),
        (0.1 / 100, 100_000, 99_950, 50, "buy", False),
        (0.1 / 100, 99_950, 100_051, 50, "buy", True),
        (0.1 / 100, 100_000, 100_050, 50, "buy", False),
    ],
)
def test_profit_usd_has_exceed_target(
    target_profit, transaction_price, currrent_price, size, side, expected
):

    result = profit_usd_has_exceed_target(
        target_profit, transaction_price, currrent_price, size, side
    )

    assert result == expected
