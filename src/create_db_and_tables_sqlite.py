# # -*- coding: utf-8 -*-

"""
Provide table manipulation queries:
- create tables
- create index
- delete tables
"""
import asyncio

# user defined formula
from db_management import sql_executing_queries


def catch_error(
    error,
    idle: int = None,
) -> list:
    """ """
    from utilities import system_tools

    system_tools.catch_error_message(error, idle)


async def create_db_sqlite(
    db_name: str = "databases/trading",
    ext: str = "sqlite3",
) -> list:
    """ """

    await sql_executing_queries.create_dataBase_sqlite(
        db_name,
        ext,
    )


async def create_tbl_json_sqlite() -> list:
    """ """

    tables = [
        "my_trades_all_json",
        "orders_all_json",
        "portfolio_json",
        "account_summary_json",
        "market_analytics_json",
        "supporting_items_json",
        "ohlc1_eth_perp_json",
        "ohlc3_eth_perp_json",
        "ohlc5_eth_perp_json",
        "ohlc15_eth_perp_json",
        "ohlc30_eth_perp_json",
        "ohlc60_eth_perp_json",
        "ohlc4H_eth_perp_json",
        "ohlc1D_eth_perp_json",
        "ohlc1_btc_perp_json",
        "ohlc3_btc_perp_json",
        "ohlc5_btc_perp_json",
        "ohlc15_btc_perp_json",
        "ohlc30_btc_perp_json",
        "ohlc60_btc_perp_json",
        "ohlc4H_btc_perp_json",
        "ohlc1D_btc_perp_json",
        "transaction_log_json",
    ]

    for table in tables:
        await sql_executing_queries.create_tables_json_sqlite(table)


async def main() -> list:
    """ """
    await create_db_sqlite("databases/trading", "sqlite3")

    await create_tbl_json_sqlite()
    # query=await sqlite_management.querying_table('myTradesOpen', 'state', '=', 'filled')
    # print (query)


if __name__ == "__main__":

    try:
        asyncio.get_event_loop().run_until_complete(main())

    except (KeyboardInterrupt, SystemExit):
        catch_error(KeyboardInterrupt)

    except Exception as error:
        catch_error(error, 10)
