import snowflake.snowpark.functions as F
from snowflake.snowpark import DataFrame

from snowflake.snowpark.context import get_active_session
import notebooks.utils.normalization as norm
import notebooks.utils.surcharges as sur
from notebooks.modeling_library import date_overrides, model
from notebooks.misc_utils import (
    join_charge_bands,
    join_executed_agreements,
    lower_columns,
)
import notebooks.utils.table_names as tn

from reveel_lib.utils import load_data_xforms


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


def get_modeled_price(
    tracking_number: str,
    agreement_id: str | None = None,
) -> DataFrame:
    df, _ = get_shipment(tracking_number).transform(load_data_xforms)

    if not agreement_id:
        df = add_active_agreement_info(df)
    else:
        df = df.withColumn("agreement_id", F.lit(agreement_id))

    if "surcharge_id" not in lower_columns(df):
        df = add_normalized_surcharge(df)

    return model(df, env="staging", is_force_lookup=True).select(
        "tracking_number", "charge_description", F.col("new_net").alias("modeled_price")
    )


def add_active_agreement_info(df: DataFrame) -> DataFrame:
    df.show()
    client_info = (
        session.table(tn.CLIENT_INFO)
        .where(F.col("agreement_id").isNotNull() & (~F.col("is_demo")))
        .drop("effective_date", "agreement_id", "earned_band")
    )

    add_client_info = df.join(
        client_info.withColumnRenamed("account_id", "account_number"),
        ["account_number", "carrier"],
        "inner",
        lsuffix="",
        rsuffix="_r",
    )
    add_client_info.show()

    with_agreement_ids = add_client_info.transform(join_executed_agreements).select(
        add_client_info["*"],
        "agreement_id",
        F.col("start_date").alias("agreement_effective_date"),
    )

    with_charge_bands = join_charge_bands(with_agreement_ids).select(
        with_agreement_ids["*"], "average_weekly"
    )

    with_date_cols = date_overrides(with_charge_bands)

    with_date_cols.show()

    return with_date_cols
