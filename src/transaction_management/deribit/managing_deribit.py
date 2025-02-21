# built ins
import asyncio

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

# user defined formula
from transaction_management.deribit.api_requests import SendApiRequest
from transaction_management.deribit.orders_management import saving_traded_orders


def get_first_tick_query(
    where_filter: str,
    transaction_log_trading: str,
    instrument_name: str,
    count: int = 1,
) -> str:

    return f"""SELECT MIN ({where_filter}) FROM {transaction_log_trading} WHERE instrument_name LIKE '%{instrument_name}%' ORDER  BY {where_filter} DESC
    LIMIT  {count+1}"""


def first_tick_fr_sqlite_if_database_still_empty(count: int) -> int:
    """ """

    from configuration.label_numbering import get_now_unix_time
    from strategies.config_strategies import (
        paramaters_to_balancing_transactions,
    )

    server_time = get_now_unix_time()

    balancing_params = paramaters_to_balancing_transactions()

    max_closed_transactions_downloaded_from_sqlite = balancing_params[
        "max_closed_transactions_downloaded_from_sqlite"
    ]

    count_at_first_download = max(count, max_closed_transactions_downloaded_from_sqlite)

    some_days_ago = 3600000 * count_at_first_download

    delta_some_days_ago = server_time - some_days_ago

    return delta_some_days_ago


def currency_inline_with_database_address(
    currency: str,
    database_address: str,
) -> bool:
    return currency.lower() in str(database_address)


@dataclass(unsafe_hash=True, slots=True)
class ModifyOrderDb(SendApiRequest):
    """ """

    private_data: object = fields

    def __post_init__(self):
        # Provide class object to access private get API
        self.private_data: str = SendApiRequest(self.sub_account_id)

    async def resupply_portfolio(
        self,
        currency,
    ) -> None:

        # fetch data from exchange
        sub_accounts = await self.private_data.get_subaccounts()

        portfolio = extract_portfolio_per_id_and_currency(
            self.sub_account_id,
            sub_accounts,
            currency,
        )

        await update_db_pkl(
            "portfolio",
            portfolio,
            currency,
        )

    async def update_trades_from_exchange(
        self,
        currency: str,
        archive_db_table,
        order_db_table,
        count: int = 5,
    ) -> None:
        """ """
        trades_from_exchange = await self.private_data.get_user_trades_by_currency(
            currency,
            count,
        )

        if trades_from_exchange:

            trades_from_exchange_without_futures_combo = [
                o
                for o in trades_from_exchange
                if f"{currency}-FS" not in o["instrument_name"]
            ]

            if trades_from_exchange_without_futures_combo:

                for trade in trades_from_exchange_without_futures_combo:

                    log.error(f"trades_from_exchange {trade}")

                    await saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table,
                    )
