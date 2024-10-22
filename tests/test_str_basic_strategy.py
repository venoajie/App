# -*- coding: utf-8 -*-
import pytest


from strategies.basic_strategy import (
    are_size_and_order_appropriate,
    check_if_next_closing_size_will_not_exceed_the_original,
    is_label_and_side_consistent,
    positions_and_orders,
    proforma_size,
    sum_order_under_closed_label_int)

non_checked_strategies = ["futureSpread",
                          "comboAuto",]
      
params1 = {"label": "hedgingSpot-open-100",
           "side": "sell"}
params2 = {"label": "customShort-open-100",
           "side": "sell"}
params3 = {"label": "comboAuto-open-100",
           "side": "sell"}
params4 = {"label": "comboAuto-open-100",
           "side": "buy"}
params5 = {"label": "futureSpread-open-100",
           "side": "buy"}
params6 = {"label": "futureSpread-open-100",
           "side": "sell"}
params7 = {"label": "customLong-open-100",
           "side": "buy"}
params8 = {"label": "customLong-open-100",
           "side": "sell"}
params9 = {"label": "hedgingSpot-open-100",
           "side": "buy"}
@pytest.mark.parametrize("non_checked_strategies, params, expected", [
    ( non_checked_strategies, params1, True),
    ( non_checked_strategies, params2, True),
    ( non_checked_strategies, params3, True),
    ( non_checked_strategies, params4, True),
    ( non_checked_strategies, params5, True),
    ( non_checked_strategies, params6, True),
    ( non_checked_strategies, params7, True),
    ( non_checked_strategies, params8, False),
    ( non_checked_strategies, params9, False),
    ])
def test_is_label_and_side_consistent (non_checked_strategies, 
                                        params,
                                        expected):
    
    result = is_label_and_side_consistent (non_checked_strategies, 
                                              params,)

    assert result == expected
    
    
@pytest.mark.parametrize("closed_orders_label_strategy, label_integer_open, expected", [
    ( [], [], 0),
    ])
def test_sum_order_under_closed_label_int (closed_orders_label_strategy, 
                                           label_integer_open,
                                           expected):
    
    result = sum_order_under_closed_label_int (closed_orders_label_strategy, 
                                              label_integer_open,)

    assert result == expected


@pytest.mark.parametrize("basic_size, net_size, next_size, expected", [
    ( -1, -1, 1, True),
    ( -10, 0, 10, True),
    ( 10, 0, -10, True),
    ( -10, 6, 4, True),
    ( -10, 4, 6, True),
    ( -10, 6, -1, False),
    ( -10, -6, -16, False),
    ( 10, 6, 16, False),
    ( -10, 11, 1, False),
    ( 10, -11,- 1, False),
    ])
def test_check_if_closing_size_will_not_exceed_the_original(basic_size, 
                                                        net_size,
                                                        next_size,
                                                        expected):

    result = check_if_next_closing_size_will_not_exceed_the_original(basic_size, 
                                                            net_size,
                                                            next_size,)

    assert result == expected


@pytest.mark.parametrize("current_size_or_open_position, current_orders_size, expected", [
    (-100, -10, -110),
    (100, -10, 90),])
def test_positions_and_orders(current_size_or_open_position, 
                                      current_orders_size,
                                      expected):
    
    result = positions_and_orders(current_size_or_open_position, 
                                      current_orders_size,)

    assert result == expected


@pytest.mark.parametrize("current_size_or_open_position, current_orders_size, next_orders_size, expected", [
    (-100, -10, -10, -120),
    (100, -10, 10, 100),])
def test_proforma_size_add(current_size_or_open_position, 
                           current_orders_size,
                           next_orders_size,
                           expected):

    result = proforma_size(current_size_or_open_position, 
                                      current_orders_size,
                                      next_orders_size,)

    assert result == expected

@pytest.mark.parametrize("purpose, current_size_or_open_position, current_orders_size, next_orders_size, max_position, expected", [
    ("add_position",-100, -10, -10, -100, False),
    ("add_position",0, 10, 10, 100, True),
    ("add_position",0, 0, -10, -100, True),
    ("reduce_position",0, -10, 10, 100, False),
    ("reduce_position",0, -10, 10, -100, False),
    ("reduce_position",-50, 0, -10, None, False),
    ("reduce_position",-50, 0, 10, None, True),
    ("reduce_position",-100, 0, 10, 100, True),])
def test_are_size_and_order_appropriate(purpose,
                                        current_size_or_open_position, 
                                        current_orders_size,
                                        next_orders_size,
                                        max_position,
                                        expected):

    result = are_size_and_order_appropriate(purpose,
                                             current_size_or_open_position, 
                                             current_orders_size, 
                                             next_orders_size,
                                             max_position)

    assert result == expected

