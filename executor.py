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
    import importlib

    import reveel_lib.explain as explain_mod
    import reveel_lib.shipment as shipment_mod

    importlib.reload(explain_mod)
    importlib.reload(shipment_mod)
    g = {
        **SAFE_GLOBALS,
        "get_shipment": shipment_mod.get_shipment,
        "add_normalized_surcharge": shipment_mod.add_normalized_surcharge,
        "get_modeled_price": shipment_mod.get_modeled_price,
    }
    output_df = eval(code, g)

    if not isinstance(output_df, DataFrame):
        raise ValueError("The output of the code is not a DataFrame.")

    output_df.show(max_with=1000)
