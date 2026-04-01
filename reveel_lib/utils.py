import snowflake.snowpark.functions as F
from snowflake.snowpark import DataFrame, Window
from snowflake.snowpark.types import DecimalType

import notebooks.utils.normalization as norm
import notebooks.utils.surcharges as sur
from notebooks.modeling_library import (
    col_or_zero,
    fedex_data_fixes,
    final_ship_date_filter,
    is_intl_multipiece,
)
from notebooks.shipment_tracking_info.utils import get_better_shipment_date
from notebooks.utils.currency import get_usd_spend
from notebooks.utils.optimize import write_tmp_table


def load_data_xforms(
    df: DataFrame,
    date_col: str = "invoice_date",
    start_date: str | None = None,
    end_date: str | None = None,
):
    filtered_data = (
        df.transform(get_usd_spend)
        .withColumns(
            {
                "lead_shipment_number": F.coalesce(
                    "lead_shipment_number", "tracking_number"
                ),
                "old_gross": F.col("gross").cast(DecimalType(38, 18)),
                "old_net_amount": F.col("net_amount").cast(DecimalType(38, 18)),
            }
        )
        .transform(write_tmp_table)
    )

    ups_data = filtered_data.where(F.col("carrier") == "ups")
    fedex_data = filtered_data.where(F.col("carrier") == "fedex")

    ups_data = get_better_shipment_date(ups_data)

    fedex_data = fedex_data.drop("upload_year", "upload_month", "upload_day")
    ups_data = ups_data.drop("upload_year", "upload_month", "upload_day")

    fedex_data = fedex_data.distinct()
    fedex_data = fedex_data_fixes(fedex_data)

    ups_data = ups_data.distinct().where(
        F.col("billed_weight").is_not_null()
        | ~F.col("is_transportation_charge")
        | is_intl_multipiece
        | F.col("is_flat_rate")
    )

    final_ups_data = ups_data.where(
        F.col("tracking_number").is_not_null()
        | F.col("lead_shipment_number").is_not_null()
    )
    ups_data_without_tracking_numbers = ups_data.where(
        F.col("tracking_number").is_null() & F.col("lead_shipment_number").is_null()
    )

    current_data = fedex_data.union_all_by_name(
        final_ups_data, allow_missing_columns=True
    ).withColumns(
        {
            "volume": F.ceil(col_or_zero("dim_length"))
            * F.ceil(col_or_zero("dim_width"))
            * F.ceil(col_or_zero("dim_height")).cast(DecimalType(38, 6)),
            "hwt_billed_weight": F.try_cast(
                F.col("hwt_billed_weight"), DecimalType(38, 18)
            ),
            "modeled_billed_weight": F.lit(None).cast(DecimalType(38, 18)),
            "num_of_trackings": F.size(
                F.collect_set("tracking_number").over(
                    Window.partitionBy("lead_shipment_id")
                )
            ),
            "new_service_type": F.coalesce(F.col("new_service_type"), F.lit("invalid")),
            "service": F.first_value(F.col("service"), True).over(
                Window.partitionBy("shipment_id").orderBy(
                    F.col("is_transportation_charge").desc_nulls_last()
                )
            ),
        }
    )

    current_data = current_data.where(
        ~F.col("is_transportation_charge")
        | (F.col("gross").isNotNull() & (F.col("gross") != 0))
        | (F.col("net_amount").isNotNull() & (F.col("net_amount") != 0))
        | F.col("is_negated_charge")
    )

    current_data = current_data.with_column(
        "new_service_type", F.coalesce(F.col("new_service_type"), F.lit("invalid"))
    )

    current_data = current_data.with_column(
        "new_service_type",
        F.when(
            F.col("is_hwt")
            & ~F.col("new_service_type").rlike(".*(Hundredweight|Freight).*"),
            F.concat(F.col("new_service_type"), F.lit(" Hundredweight")),
        ).otherwise(F.col("new_service_type")),
    )

    with_relookup_surcharges_identified = current_data.withColumns(
        {
            "is_oversize": F.boolor_agg(
                F.col("surcharge_id").eqNullSafe(F.lit(sur.surcharge_id_oversize_comm))
                | F.col("surcharge_id").eqNullSafe(F.lit(sur.surcharge_id_oversize_resi))
            ).over(Window.partitionBy("shipment_id", "lead_shipment_id")),
            "is_ahs_dim": F.boolor_agg(
                F.col("surcharge_id").eqNullSafe(F.lit(sur.surcharge_id_ahs_dim))
            ).over(Window.partitionBy("shipment_id", "lead_shipment_id")),
        }
    )

    with_rate_cap = with_relookup_surcharges_identified.with_column(
        "rate_capped", F.lit(False)
    )

    with_norm_container = with_rate_cap.transform(norm.add_normalized_packaging)

    if date_col == "shipment_date":
        return final_ship_date_filter(
            with_norm_container,
            ups_data_without_tracking_numbers,
            start_date,
            end_date,
        )
    else:
        return with_norm_container, ups_data_without_tracking_numbers
