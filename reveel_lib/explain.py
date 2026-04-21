"""
Explain payload shaped like `explain_schema` (nested structs).

Discount granularity
----------------------
`model()` collapses multiple agreement-term discount rows into `discount_sum` and
`currency_sum` via window sums in `calculate_percent_discounts` / `calculate_currency_discounts`
(see modeling_library). After that, **per-term base vs earned breakdown is not recoverable**
from the final dataframe alone.

Options without losing information:
  - Change modeling_library to retain intermediate columns (e.g. array of applied discounts,
    or keep `percent_discount` per term row through final_cleanup), or
  - Run a parallel explain query from Postgres `agreement_terms` for display-only (may not
  match application order), or
  - Accept **aggregated** explain lines (what we do below): one row for combined % discount,
  one for combined currency off, labeled as totals—not individual earned/base lines.
"""

from __future__ import annotations

import snowflake.snowpark.functions as F
from snowflake.snowpark import DataFrame
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Re-export schema for callers / tests
explain_schema = StructType(
    [
        StructField(
            "rate_construction",
            StructType(
                [
                    StructField("carrier", StringType(), True),
                    StructField("service", StringType(), True),
                    StructField("charge_description", StringType(), True),
                    StructField("package_type", StringType(), True),
                    StructField("zone", StringType(), True),
                    StructField("billed_weight", DoubleType(), True),
                    StructField("is_transportation_charge", BooleanType(), True),
                    StructField("surcharge_id", StringType(), True),
                    StructField("is_flat_rate", BooleanType(), True),
                    StructField("is_hwt", BooleanType(), True),
                    StructField("is_multipiece", BooleanType(), True),
                    StructField("base_rate_date", StringType(), True),
                    StructField("rate_source", StringType(), True),
                    StructField("gross", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "discounts",
            StructType(
                [
                    StructField(
                        "base",
                        ArrayType(
                            StructType(
                                [
                                    StructField("type", StringType(), True),
                                    StructField("value", DoubleType(), True),
                                    StructField("amount", DoubleType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                    StructField(
                        "min",
                        ArrayType(
                            StructType(
                                [
                                    StructField("type", StringType(), True),
                                    StructField("threshold", DoubleType(), True),
                                    StructField("value", DoubleType(), True),
                                    StructField("amount", DoubleType(), True),
                                    StructField("applied", BooleanType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("final_net", DoubleType(), True),
    ]
)


def _names(df: DataFrame) -> dict[str, str]:
    return {c.lower(): c for c in df.columns}


def _c(df: DataFrame, name: str):
    """Column if present, else NULL literal (typed as string for struct flexibility)."""
    n = _names(df)
    if name.lower() in n:
        return F.col(n[name.lower()])
    return F.lit(None)


def model_explain(df: DataFrame) -> DataFrame:
    """
    Map modeled charge row(s) to top-level columns: rate_construction, discounts, final_net,
    matching explain_schema conceptually. Uses aggregated discount lines when per-term detail
    is not available (see module docstring).
    """
    gross_for_pct = F.coalesce(
        _c(df, "updated_gross"), _c(df, "new_gross"), _c(df, "gross")
    )
    pct_amount = gross_for_pct * F.coalesce(
        _c(df, "discount_sum").cast(DoubleType()), F.lit(0.0)
    )

    base_arr = F.array_construct(
        F.struct(
            F.lit("aggregated_percent_discounts").alias("type"),
            F.round(
                F.coalesce(_c(df, "discount_sum").cast(DoubleType()), F.lit(0.0)),
                2,
            ).alias("value"),
            F.round(pct_amount.cast(DoubleType()), 2).alias("amount"),
        ),
        F.struct(
            F.lit("aggregated_currency_discounts").alias("type"),
            F.round(
                F.coalesce(_c(df, "currency_sum").cast(DoubleType()), F.lit(0.0)),
                2,
            ).alias("value"),
            F.round(
                F.coalesce(_c(df, "currency_sum").cast(DoubleType()), F.lit(0.0)),
                2,
            ).alias("amount"),
        ),
    )

    min_arr = F.array_construct(
        F.struct(
            F.lit("minimum_floor").alias("type"),
            F.round(
                F.coalesce(_c(df, "new_min").cast(DoubleType()), F.lit(0.0)),
                2,
            ).alias("threshold"),
            F.round(
                F.coalesce(_c(df, "min_discount").cast(DoubleType()), F.lit(0.0)),
                2,
            ).alias("value"),
            F.round(
                F.coalesce(_c(df, "minimum_charge").cast(DoubleType()), F.lit(0.0)),
                2,
            ).alias("amount"),
            F.coalesce(_c(df, "is_hit_min"), F.lit(0)).cast("boolean").alias("applied"),
        )
    )

    rate_construction = F.struct(
        _c(df, "carrier").cast(StringType()).alias("carrier"),
        F.coalesce(_c(df, "service"), _c(df, "new_service_type"))
        .cast(StringType())
        .alias("service"),
        _c(df, "charge_description").cast(StringType()).alias("charge_description"),
        _c(df, "norm_container_type").cast(StringType()).alias("package_type"),
        _c(df, "zone").cast(StringType()).alias("zone"),
        F.round(
            F.coalesce(
                _c(df, "billed_weight"),
                _c(df, "modeled_billed_weight"),
                _c(df, "original_billed_weight"),
            ).cast(DoubleType()),
            2,
        ).alias("billed_weight"),
        _c(df, "is_transportation_charge")
        .cast(BooleanType())
        .alias("is_transportation_charge"),
        _c(df, "surcharge_id").cast(StringType()).alias("surcharge_id"),
        _c(df, "is_flat_rate").cast(BooleanType()).alias("is_flat_rate"),
        _c(df, "is_hwt").cast(BooleanType()).alias("is_hwt"),
        _c(df, "is_multipiece").cast(BooleanType()).alias("is_multipiece"),
        _c(df, "base_rate_date").cast(StringType()).alias("base_rate_date"),
        F.lit("modeled_pipeline").alias("rate_source"),
        F.round(
            F.coalesce(_c(df, "gross"), _c(df, "old_gross")).cast(DoubleType()),
            2,
        ).alias("gross"),
    )

    discounts = F.struct(base_arr.alias("base"), min_arr.alias("min"))

    return df.select(
        rate_construction.alias("rate_construction"),
        discounts.alias("discounts"),
        F.round(
            F.coalesce(_c(df, "new_net").cast(DoubleType()), F.lit(0.0)),
            2,
        ).alias("final_net"),
    )
