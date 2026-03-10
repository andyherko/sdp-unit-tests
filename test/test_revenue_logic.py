import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.transformations.revenue_logic import calculate_net_revenue

@pytest.fixture(scope="session")
def spark():
    """
    Fixture to initialize a local SparkSession.
    Enables 'Shift-Left' testing on a local machine or CI agent.
    """
    return SparkSession.builder \
        .master("local[*]") \
        .appName("SDP-Unit-Testing") \
        .getOrCreate()

def test_calculate_net_revenue(spark):
    source_data = [
        ("order_1", 100.0, 10.0),
        ("order_2", 50.5, 5.05)
    ]
    source_df = spark.createDataFrame(source_data, ["order_id", "gross_revenue", "tax_amount"])

    expected_data = [
        ("order_1", 100.0, 10.0, 90.0),
        ("order_2", 50.5, 5.05, 45.45)
    ]
    expected_df = spark.createDataFrame(expected_data, ["order_id", "gross_revenue", "tax_amount", "net_revenue"])

    actual_df = calculate_net_revenue(source_df)

    assert_df_equality(
        actual_df, 
        expected_df, 
        ignore_row_order=True, 
        ignore_nullable=True
    )