# built ins
import asyncio

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

# user defined formula
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import SendApiRequest
from transaction_management.deribit.orders_management import saving_traded_orders
from utilities.pickling import replace_data
from utilities.system_tools import provide_path_for_file


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


async def update_db_pkl(
    path,
    data_orders,
    currency,
) -> None:

    my_path_portfolio = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(
        currency,
        my_path_portfolio,
    ):

        replace_data(
            my_path_portfolio,
            data_orders,
        )


def currency_inline_with_database_address(
    currency: str,
    database_address: str,
) -> bool:
    return currency.lower() in str(database_address)


def extract_portfolio_per_id_and_currency(
    sub_account_id: str,
    sub_accounts: list,
    currency: str,
) -> list:

    portfolio_all = [o for o in sub_accounts if str(o["id"]) in sub_account_id][0][
        "portfolio"
    ]

    return portfolio_all[f"{currency.lower()}"]


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

    async def send_triple_orders(
        self, 
        params,
        ) -> None:
        """
        triple orders:
            1 limit order
            1 SL market order
            1 TP limit order
        """

        main_side = params["side"]
        instrument = params["instrument_name"]
        main_label = params["label_numbered"]
        closed_label = params["label_closed_numbered"]
        size = params["size"]
        main_prc = params["entry_price"]
        sl_prc = params["cut_loss_usd"]
        tp_prc = params["take_profit_usd"]

        order_result = await self.send_order(
            main_side, instrument, size, main_label, main_prc
        )

        order_result_id = order_result["result"]["order"]["order_id"]

        if "error" in order_result:
            await self.get_cancel_order_byOrderId(order_result_id)
            await telegram_bot_sendtext("combo order failed")

        else:
            if main_side == "buy":
                closed_side = "sell"
                trigger_prc = tp_prc - 1

            if main_side == "sell":
                closed_side = "buy"
                trigger_prc = tp_prc + 1

            order_result = await self.send_order(
                closed_side,
                instrument,
                size,
                closed_label,
                None,
                "stop_market",
                sl_prc,
            )

            log.info(order_result)

            if "error" in order_result:
                await self.get_cancel_order_byOrderId(order_result_id)
                await telegram_bot_sendtext("combo order failed")

            order_result = await self.send_order(
                closed_side,
                instrument,
                size,
                closed_label,
                tp_prc,
                "take_limit",
                trigger_prc,
            )
            log.info(order_result)

            if "error" in order_result:
                await self.get_cancel_order_byOrderId(order_result_id)
                await telegram_bot_sendtext("combo order failed")
