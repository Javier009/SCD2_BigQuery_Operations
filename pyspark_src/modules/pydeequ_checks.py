# deequ_checks.py
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pyspark.sql.functions import col

def run_quality_checks(spark, df):
    
    # Flat data frame for check
    df_flat = df.withColumn("customer_id", col("customer_info.customer_id"))

    check = (
      Check(spark, CheckLevel.Error, "Data quality")
      .hasSize(lambda sz: sz > 0, "non_empty")
      .isComplete("transaction_id", "transaction_id_not_null")
      .isComplete("customer_id", "customer_id_not_null")
    )

    product_check = (
      VerificationSuite(spark)
      .onData(df_flat)
      .addCheck(check)
      .run()
    )

    dq_check_df = VerificationResult.checkResultsAsDataFrame(spark, product_check)
    dq_check_df.show()
    
    if product_check.status != "Success":
        raise ValueError("Data Quality Checks Failed for Transactions Data")