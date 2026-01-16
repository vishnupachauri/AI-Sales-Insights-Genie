from pyspark.sql.functions import col, to_date, month, year, expr

catalog = "ai-sales-insights-genie"
schema = "input_data"
volume_path = f"/Volumes/{catalog}/{schema}/data"

def clean_cols(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.replace(" ", "_").replace("-", "_"))
    return df

# 1. Load Datasets
orders_df = spark.read.csv(f"{volume_path}/List of Orders.csv", header=True, inferSchema=True)
details_df = spark.read.csv(f"{volume_path}/Order Details.csv", header=True, inferSchema=True)
targets_df = spark.read.csv(f"{volume_path}/Sales target.csv", header=True, inferSchema=True)

# 2. Clean & Format
orders_df = clean_cols(orders_df).withColumn("Order_Date", to_date(col("Order_Date"), "dd-MM-yyyy"))
details_df = clean_cols(details_df)
targets_df = clean_cols(targets_df)

# 3. Create Master Sales Table (Join Orders + Details)
master_sales = details_df.join(orders_df, on="Order_ID", how="inner")

# 4. Save to Unity Catalog as Delta Tables
master_sales.write.mode("overwrite").saveAsTable(f"`{catalog}`.{schema}.fact_sales")
targets_df.write.mode("overwrite").saveAsTable(f"`{catalog}`.{schema}.fact_targets")

print(f"Tables created in {catalog}.{schema}")
