from snowflake.snowpark.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
    ArrayType
)

explain_schema = StructType([
    StructField("rate_construction",
        StructType([
            StructField("carrier", StringType(), True),
            StructField("service", StringType(), True),
            StructField("package_type", StringType(), True),
            StructField("zone", DoubleType(), True),
            StructField("billed_weight", DoubleType(), True),

            StructField("is_transportation_charge", BooleanType(), True),
            StructField("surcharge_id", StringType(), True),

            StructField("is_flat_rate", BooleanType(), True),
            StructField("is_hwt", BooleanType(), True),
            StructField("is_multipiece", BooleanType(), True),

            StructField("base_rate_date", StringType(), True),
            StructField("rate_source", StringType(), True),

            StructField("gross", DoubleType(), True)
        ]),
        True
    ),

    StructField("discounts",
        StructType([
            StructField(
                "base",
                ArrayType(
                    StructType([
                        StructField("type", StringType(), True),
                        StructField("value", DoubleType(), True),
                        StructField("amount", DoubleType(), True)
                    ])
                ),
                True
            ),
            StructField(
                "min",
                ArrayType(
                    StructType([
                        StructField("type", StringType(), True),
                        StructField("threshold", DoubleType(), True),
                        StructField("value", DoubleType(), True),
                        StructField("amount", DoubleType(), True),
                        StructField("applied", BooleanType(), True)
                    ])
                ),
                True
            )
        ]),
        True
    ),

    StructField("final_net", DoubleType(), True)
])