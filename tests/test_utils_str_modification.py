# -*- coding: utf-8 -*-
import pytest

from utilities.string_modification import (
    extract_currency_from_text,
    extract_integers_aggregation_from_text,
    extract_integers_from_text,
    get_unique_elements,
)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("incremental_ticker.BTC-PERPETUAL", "btc"),
        ("incremental_ticker.BTC-4OCT24", "btc"),
        ("chart.trades.BTC-4OCT24", "btc"),
        ("chart.trades.BTC-PERPETUAL.1", "btc"),
        ("chart.trades.BTC-PERPETUAL.1D", "btc"),
        ("user.portfolio.eth", "eth"),
        ("user.changes.any.ETH.raw", "eth"),
    ],
)
def test_check_if_closing_size_will_exceed_the_original(text, expected):

    result = extract_currency_from_text(
        text,
    )

    assert result == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        (50412766709, 50412766709),
        ("50412766709", 50412766709),
        ("ETH-50412766709", 50412766709),
        ("hedgingSpot-closed-1729128970946", 1729128970946),
        ("ETH-219515135", 219515135),
        ("ETH-50412766709", 50412766709),
        ("every5mtestLong-1681617021717", 51681617021717),
    ],
)
def test_extract_integers_from_text(text, expected):

    result = extract_integers_from_text(text)

    assert result == expected


closed_label_with_same_size_as_open_label = [
    {
        "instrument_name": "ETH-PERPETUAL",
        "label": "hedgingSpot-closed-1729220139293",
        "amount": 2.0,
        "trade_id": "ETH-219435821",
    },
    {
        "instrument_name": "ETH-PERPETUAL",
        "label": "hedgingSpot-closed-1729220139293",
        "amount": 2.0,
        "trade_id": "ETH-219435815",
    },
    {
        "instrument_name": "ETH-PERPETUAL",
        "label": "hedgingSpot-closed-1729220139293",
        "amount": 2.0,
        "trade_id": "ETH-219435810",
    },
]


@pytest.mark.parametrize(
    "identifier, aggregator, text, expected",
    [
        (
            "trade_id",
            min,
            closed_label_with_same_size_as_open_label,
            219435810,
        ),
    ],
)
def test_extract_integers_aggregation_from_text(identifier, aggregator, text, expected):

    result = extract_integers_aggregation_from_text(identifier, aggregator, text)

    assert result == expected


@pytest.mark.parametrize(
    "data1, data2, expected",
    [
        ([4, 5, 6], [1, 2, 3, 4, 5], [6]),
        ([1, 2, 3, 4, 5], [4, 5, 6], [1, 2, 3]),
    ],
)
def test_get_unique_elements(data1, data2, expected):

    result = get_unique_elements(data1, data2)

    assert result == expected
