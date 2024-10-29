

from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)

async def send_text():
    
    await telegram_bot_sendtext('size or open order is inconsistent', "general_error")