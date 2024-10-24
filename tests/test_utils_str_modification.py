# -*- coding: utf-8 -*-
import pytest

from utilities.string_modification import (
    extract_currency_from_text,
    extract_integers_from_text)


@pytest.mark.parametrize("text, expected", [
    ("incremental_ticker.BTC-PERPETUAL", "btc"),
    ("incremental_ticker.BTC-4OCT24", "btc"),
    ("chart.trades.BTC-4OCT24", "btc"),
    ("chart.trades.BTC-PERPETUAL.1", "btc"),
    ("chart.trades.BTC-PERPETUAL.1D", "btc"),
    ("user.portfolio.eth", "eth"),
    ("user.changes.any.ETH.raw", "eth"),
    ])
def test_check_if_closing_size_will_exceed_the_original(text,
                                                        expected):

    result = extract_currency_from_text(text,)

    assert result == expected


@pytest.mark.parametrize("text, expected", [
    (50412766709, 50412766709),
    ("50412766709", 50412766709),
    ("ETH-50412766709", 50412766709),
    ("hedgingSpot-closed-1729128970946", 1729128970946),
    ("ETH-219515135", 219515135),
    ("ETH-50412766709", 50412766709),
    ("every5mtestLong-1681617021717", 51681617021717),
    ])
def test_extract_integers_from_text(text,
                                    expected):

    result = extract_integers_from_text(text)

    assert result == expected


