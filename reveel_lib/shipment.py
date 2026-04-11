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

from reveel_lib.explain import model_explain
from reveel_lib.utils import load_data_xforms


session = get_active_session()

# Columns to retain in explain mode (repricing / audit). Matched case-insensitively to
# whatever Snowpark has after model(); missing names are skipped.
model_explain_cols = [
    "tracking_number",
    "lead_shipment_number",
    "lead_shipment_id",
    "shipment_id",
    "agreement_id",
    "account_number",
    "carrier",
    "invoice_date",
    "payor",
    "surcharge_id",
    "surcharge_name",
    "charge_description",
    "is_transportation_charge",
    "is_fuel_calc_surcharge",
    "gross",
    "net_amount",
    "old_gross",
    "old_net_amount",
    "new_gross",
    "new_net",
    "discount_sum",
    "currency_sum",
    "full_discount_percent",
    "term_type",
    "term_service",
    "earned_type",
    "earned_band",
    "average_weekly",
    "discount_amount",
    "discount_units",
    "discount_zone",
    "discount_weight",
    "discount_exclude_hundredweight",
    # Term-side discount_is_flat_rate is not in term_keep_cols; use is_flat_rate on the charge.
    "discount_less_than_one",
    "rate_capped",
    "is_rate_capped",
    "minimum_charge",
    "min_def",
    "new_min",
    "min_discount",
    "pre_rate_cap_minimum_charge",
    "is_hit_min",
    "net_hitting_min",
    "unrealized_savings",
    "minimum_reduction",
    "net_subtotal",
    "trans_subtotal",
    "fuel_subtotal",
    "tp_subtotal",
    "other_subtotal",
    "updated_gross",
    "fuel_rate",
    "new_fuel_gross",
    "tp_rate",
    "new_tp_gross",
    "other_charges_net",
    "tc_net",
    "fuel_net",
    "zone",
    "import_export",
    "service",
    "new_service_type",
    "is_hwt",
    "is_multipiece",
    "is_flat_rate",
    "less_than_one",
    "billed_weight",
    "original_billed_weight",
    "modeled_billed_weight",
    "raw_modeled_billed_weight",
    "hwt_billed_weight",
    "dim_divisor",
    "dim_length",
    "dim_width",
    "dim_height",
    "volume",
    "num_of_trackings",
    "norm_container_type",
    "skip",
]


def _select_repricing_explain_columns(df: DataFrame) -> DataFrame:
    by_lower = {c.lower(): c for c in df.columns}
    present = [by_lower[n] for n in model_explain_cols if n in by_lower]
    return df.select(present) if present else df


def _unpack_load_result(result):
    return result[0] if isinstance(result, tuple) else result


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
    *,
    explain: bool = False,
) -> DataFrame:
    df = get_shipment(tracking_number)
    if "surcharge_id" not in lower_columns(df):
        df = add_normalized_surcharge(df)

    df = _unpack_load_result(load_data_xforms(df))

    if not agreement_id:
        df = add_active_agreement_info(df)
    else:
        df = df.withColumn("agreement_id", F.lit(agreement_id))

    with_model_price = model(df, env="staging", is_force_lookup=True)

    if explain:
        with_debug_cols = _select_repricing_explain_columns(with_model_price)
        return model_explain(with_debug_cols)
    return with_model_price.select(
        "tracking_number", "charge_description", F.col("new_net").alias("modeled_price")
    )


def add_active_agreement_info(df: DataFrame) -> DataFrame:
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

    with_agreement_ids = add_client_info.transform(join_executed_agreements).select(
        add_client_info["*"],
        "agreement_id",
        F.col("start_date").alias("agreement_effective_date"),
    )

    with_charge_bands = join_charge_bands(with_agreement_ids).select(
        with_agreement_ids["*"], "average_weekly"
    )

    return date_overrides(with_charge_bands)
