import snowflake.snowpark.functions as F
from snowflake.snowpark import DataFrame

from snowflake.snowpark.context import get_active_session
import notebooks.utils.normalization as norm
import notebooks.utils.surcharges as sur


session = get_active_session()


def get_shipment(tracking_number: str) -> DataFrame:
    return session.table("staging.charge.premodel").where(
        F.col("tracking_number") == tracking_number
    )


def add_normalized_surcharge(df: DataFrame) -> DataFrame:
    if "cleaned_cd" not in df.columns:
        df = df.withColumn(
            "cleaned_cd",
            norm.invoice_surcharge_clean(
                F.col("charge_description"),
                F.col("carrier"),
            ),
        )

    if {
        "is_alaska",
        "is_hawaii",
        "is_intra_hawaii",
        "is_intra_alaska",
        "is_residential",
        "is_transportation_charge",
    } not in df.columns:
        df = sur.add_surcharge_flags(df)

    return norm.get_normalized_surcharge(df).distinct()
