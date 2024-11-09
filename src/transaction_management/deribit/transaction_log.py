# -*- coding: utf-8 -*-

# user defined formula
from db_management.sqlite_management import(
    insert_tables,)
from utilities.string_modification import(
    remove_dict_elements,)


async def saving_transaction_log(
    transaction_log_trading, 
    transaction_log,
    ) -> None:
    
    """_summary_

    Args:
        trades (_type_): _description_
        orders (_type_): _description_
    """
    

    # processs if transactions log not empty
    if transaction_log:
        
        for transaction in transaction_log:
            
            # remove unnecessary element
            modified_dict = remove_dict_elements(
                transaction,
                "info"
                )
            
            # get Transaction type                        
            type_log = modified_dict ["type"]
            
            #type: trading
            if "trade" in type_log \
                or "delivery" in type_log:
            
                # save to trading db
                await insert_tables(
                    transaction_log_trading,
                    modified_dict
                    )
            
            #type: non trading
            else:
                
                # save to non-trading db
                table= f"transaction_log_json"
                await insert_tables(
                    table, 
                    modified_dict
                    )
