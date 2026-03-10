from pyspark import pipelines as dp
from pyspark.sql.functions import count, expr
from sdp_python.transformations.config import (get_rules)

@dp.materialized_view(
    name="TEST_user_gold_sdp",
    comment="TEST: check that gold table only contains unique customer id",
    private=True
)
@dp.expect_or_fail("pk_must_be_unique", "duplicate = 1")
def TEST_user_gold_sdp():
    return (
        spark.read.table("user_gold_sdp")
        .groupBy("id")
        .agg(count("*").alias("duplicate"))
    )

# FLOW ONE: The "Clean" Production Table
# This flow uses the 'DROP' policy to ensure only valid data proceeds.
# Records failing ANY of these checks are discarded from this specific table.
@dp.table(
    name="user_gold_sdp",
    comment="Clean gold user data with invalid records dropped."
)
@dp.expect_all_or_drop(get_rules('user_gold_sdp'))
def user_gold_sdp():
    return spark.readStream("user_silver_sdp")


# FLOW TWO: The Quarantine Table
# This parallel flow captures rejected records for remediation.
@dp.table(
    name="user_gold_quarantine",
    comment="Quarantined records failing gold expectations."
)
def user_gold_quarantine():
    # Construct a filter that represents the failure of any expectation [1]
    # Logic: Capture records where NOT (All Conditions are Met)
    quarantine_filter = "NOT (age IS NOT NULL AND annual_income IS NOT NULL AND spending_core IS NOT NULL)"

    return spark.read_stream("user_silver_sdp") \
        .filter(quarantine_filter) \
        .withColumn("quarantine_reason", expr("CASE "
                                              "WHEN age IS NULL THEN 'Missing Age' "
                                              "WHEN annual_income IS NULL THEN 'Missing Income' "
                                              "WHEN spending_core IS NULL THEN 'Missing Score' "
                                              "ELSE 'Multiple Failures' END"))