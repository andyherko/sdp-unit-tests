from pyspark import pipelines as dp
from pyspark.sql.functions import expr
from sdp_python.transformations.config import (get_rules)

# FLOW ONE: The "Clean" Production Table
# This flow uses the 'DROP' policy to ensure only valid data proceeds.
# Records failing ANY of these checks are discarded from this specific table.
@dp.table(
    name="user_gold_sdp_adv",
    comment="Clean gold user data with invalid records dropped."
)
@dp.expect_all_or_drop(get_rules('user_gold_sdp_adv'))
def user_gold_sdp_adv():
    return spark.readStream.table("user_bronze_sdp")


# FLOW TWO: The Quarantine Table
# This parallel flow captures rejected records for remediation.
@dp.table(
    name="user_gold_quarantine",
    comment="Quarantined records failing gold expectations."
)
def user_gold_quarantine():
    # Construct a filter that represents the failure of any expectation
    # Logic: Capture records where NOT (All Conditions are Met)
    quarantine_filter = "NOT (id IS NOT NULL AND email IS NOT NULL AND lastname IS NOT NULL)"

    return spark.readStream.table("user_bronze_sdp") \
        .filter(quarantine_filter) \
        .withColumn("quarantine_reason", expr("CASE "
                                              "WHEN id IS NULL THEN 'Missing Id' "
                                              "WHEN email IS NULL THEN 'Missing email' "
                                              "WHEN lastname IS NULL THEN 'Missing lastname' "
                                              "ELSE 'Multiple Failures' END"))