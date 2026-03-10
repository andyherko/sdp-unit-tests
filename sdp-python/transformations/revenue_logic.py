from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def calculate_net_revenue(df: DataFrame) -> DataFrame:
    """
    Pure transformation function to calculate net revenue.
    Decoupled from SDP/DLT decorators for unit testing.
    """
    return df.withColumn(
        "net_revenue", 
        F.round(F.col("gross_revenue") - F.col("tax_amount"), 2)
    )