import snowflake.snowpark.functions as F
from snowflake.snowpark import DataFrame

from snowflake.snowpark.context import get_active_session

session = get_active_session()


def get_shipment(tracking_number: str) -> DataFrame:
    return session.table("staging.charge.premodel").where(
        F.col("tracking_number") == tracking_number
    )
