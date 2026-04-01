import snowflake.snowpark.functions as F
from snowflake.snowpark import DataFrame

from reveel_lib.shipment import (
    add_normalized_surcharge,
    get_modeled_price,  
    get_shipment,
)


SAFE_GLOBALS = {    
    "__builtins__": {},
    "F": F,
    "get_shipment": get_shipment,
    "add_normalized_surcharge": add_normalized_surcharge,
    "get_modeled_price": get_modeled_price,
}


def run_generated_code(code: str):
    output_df = eval(code, SAFE_GLOBALS)

    if not isinstance(output_df, DataFrame):
        raise ValueError("The output of the code is not a DataFrame.")

    output_df.show()
