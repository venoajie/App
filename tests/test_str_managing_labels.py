# -*- coding: utf-8 -*-
import pytest

from strategies.managing_labels import sorting_list

my_trades_currency_strategy = [
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.25,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.4,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-open-1728859651166',
        'amount': -5.0,
        'price': 2465.65,
        'side': 'sell',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.45,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-open-1728806470451',
        'amount': -3.0,
        'price': 2466.6,
        'side': 'sell',
    },
]

from_lowest_to_highest = [
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-open-1728859651166',
        'amount': -5.0,
        'price': 2465.65,
        'side': 'sell',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.25,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.4,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.45,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-open-1728806470451',
        'amount': -3.0,
        'price': 2466.6,
        'side': 'sell',
    },
]


from_highest_to_lowest = [
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-open-1728806470451',
        'amount': -3.0,
        'price': 2466.6,
        'side': 'sell',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.45,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.4,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-closed-1728806470451',
        'amount': 3.0,
        'price': 2466.25,
        'side': 'buy',
    },
    {
        'instrument_name': 'ETH-PERPETUAL',
        'label': 'hedgingSpot-open-1728859651166',
        'amount': -5.0,
        'price': 2465.65,
        'side': 'sell',
    },
]


@pytest.mark.parametrize(
    'my_trades_currency_strategy, item_reference, is_reversed, expected',
    [
        (my_trades_currency_strategy, 'price', False, from_lowest_to_highest),
        (my_trades_currency_strategy, 'price', True, from_highest_to_lowest),
    ],
)
def test_sorting_list(
    my_trades_currency_strategy, item_reference, is_reversed, expected
):

    result = sorting_list(
        my_trades_currency_strategy, item_reference, is_reversed
    )

    assert result == expected
