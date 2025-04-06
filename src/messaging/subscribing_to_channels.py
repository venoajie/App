# -*- coding: utf-8 -*-

# built ins
import asyncio

from messaging.telegram_bot import telegram_bot_sendtext
from utilities.system_tools import parse_error_message


async def redis_channels(
    pubsub: object,
    redis_channels: list,
    purpose: str,
) -> None:
    """ """

    # get redis channels
    order_allowed_channel: str = redis_channels["order_is_allowed"]
    positions_update_channel: str = redis_channels["position_cache_updating"]
    ticker_cached_channel: str = redis_channels["ticker_cache_updating"]
    order_rest_channel: str = redis_channels["order_rest"]
    my_trade_receiving_channel: str = redis_channels["my_trade_receiving"]
    order_update_channel: str = redis_channels["order_cache_updating"]
    portfolio_channel: str = redis_channels["portfolio"]
    sqlite_updating_channel: str = redis_channels["sqlite_record_updating"]
    sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
    market_analytics_channel: str = redis_channels["market_analytics_update"]
    my_trades_channel: str = redis_channels["my_trades_cache_updating"]

    try:

        match purpose:

            case "reconciling_size":
                channels = [
                    my_trade_receiving_channel,
                    order_allowed_channel,
                    portfolio_channel,
                    positions_update_channel,
                    sub_account_cached_channel,
                    ticker_cached_channel,
                ]
            case "processing_orders":
                channels = [
                    my_trade_receiving_channel,
                    portfolio_channel,
                    order_rest_channel,
                    order_update_channel,
                    sqlite_updating_channel,
                    sub_account_cached_channel,
                ]
            case "hedging_spot" | "future_spread":
                channels = [
                    market_analytics_channel,
                    order_update_channel,
                    ticker_cached_channel,
                    portfolio_channel,
                    my_trades_channel,
                    order_allowed_channel,
                    sub_account_cached_channel,
                ]
            case "cancelling_active_orders":
                channels = [
                    market_analytics_channel,
                    order_update_channel,
                    ticker_cached_channel,
                    portfolio_channel,
                    my_trades_channel,
                    sub_account_cached_channel,
                ]

        [await pubsub.subscribe(o) for o in channels]

    except Exception as error:

        parse_error_message(f"subscribe redis {error}")

        await telegram_bot_sendtext(
            f"subscribe redis - {error}",
            "general_error",
        )
