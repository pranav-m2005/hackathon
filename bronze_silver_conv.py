# ============================================================
# BRONZE -> SILVER : CLEAN DATA, NO NULLS IN SILVER
# ============================================================
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SCHEMA = "workspace.hackathon"
def t(name):
    return f"{SCHEMA}.{name}"

NULL_WORDS = ["", "nan", "none", "null", "na", "n/a", "-", "?"]
AUDIT = ["row_id", "source_file", "ingested_at"]
summary = []


# ---------------- helper functions ----------------
def clean_strings(df):
    """Trim spaces; turn 'nan', 'NULL', '' etc. into real NULL."""
    for c, dtype in df.dtypes:
        if dtype == "string":
            col = F.trim(F.col(c))
            df = df.withColumn(c, F.when(F.lower(col).isin(NULL_WORDS), None).otherwise(col))
    return df

def nice_text(c):
    return F.initcap(F.regexp_replace(F.lower(F.col(c)), "[_-]+", " "))

def to_number(c):
    return F.expr(f"try_cast(regexp_replace({c}, '[^0-9.-]', '') AS DOUBLE)")

def to_int(c):
    return F.expr(f"try_cast(try_cast({c} AS DOUBLE) AS INT)")

def to_timestamp_any(c):
    return F.coalesce(
        F.expr(f"try_to_timestamp({c})"),
        F.expr(f"try_to_timestamp({c}, 'dd-MM-yyyy HH:mm')"),
        F.expr(f"try_to_timestamp({c}, 'dd/MM/yyyy HH:mm')"),
        F.expr(f"try_to_timestamp({c}, 'dd-MM-yyyy HH:mm:ss')"),
        F.expr(f"try_to_timestamp({c}, 'dd/MM/yyyy HH:mm:ss')"),
    )

def drop_empty_rows(df):
    """Remove rows where every data column is NULL (blank Excel rows)."""
    data_cols = [c for c in df.columns if c not in AUDIT]
    return df.dropna(how="all", subset=data_cols)

def remove_duplicates(df, keys):
    df = df.withColumn("_order", F.monotonically_increasing_id())
    w = Window.partitionBy(*keys).orderBy("_order")
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn", "_order"))

def build_silver(table, df_bronze, transform, required, fill_text, dup_keys):
    total = df_bronze.count()

    df = drop_empty_rows(clean_strings(df_bronze))
    after_empty = df.count()

    df = transform(df)

    df = df.dropna(how="any", subset=required)            # required missing -> remove
    after_required = df.count()

    df = df.fillna("Unknown", subset=fill_text)            # optional text missing -> 'Unknown'

    df = remove_duplicates(df, dup_keys)
    final = df.count()

    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(t(table)))

    summary.append((table, total,
                    total - after_empty,              # empty rows
                    after_empty - after_required,     # missing required
                    after_required - final,           # duplicates
                    final))
    print(f"{table}: {total} -> {final}")


# ============================================================
# 1. CUSTOMER
# ============================================================
def customer_transform(df):
    return (df.withColumn("customer_id", F.upper("customer_id"))
              .withColumn("customer_name", F.initcap("customer_name"))
              .withColumn("contact_number", F.regexp_replace("contact_number", r"\.0$", ""))
              .withColumn("contact_number", F.regexp_replace("contact_number", "[^0-9+]", ""))
              .withColumn("contact_number",
                          F.when(F.length("contact_number") >= 7, F.col("contact_number")))
              .withColumn("shipping_city", nice_text("shipping_city"))
              .withColumn("billing_city", nice_text("billing_city")))

build_silver("silver_customer", spark.table(t("bronze_customer")), customer_transform,
             required=["customer_id"],
             fill_text=["customer_name", "contact_number", "shipping_city", "billing_city"],
             dup_keys=["customer_id"])


# ============================================================
# 2. PRODUCT (transactions)
# ============================================================
def product_transform(df):
    return (df.withColumn("customer_id", F.upper("customer_id"))
              .withColumn("transaction_id", F.upper("transaction_id"))
              .withColumn("merchant_category", nice_text("merchant_category"))
              .withColumn("amount_aed", to_number("amount_aed"))
              .withColumn("items_count", to_int("items_count"))
              .withColumn("device_type", nice_text("device_type"))
              .withColumn("browser", nice_text("browser"))
              .withColumn("return_option", nice_text("return_option"))
              .filter(F.col("amount_aed").isNull() | (F.col("amount_aed") >= 0)))

build_silver("silver_product", spark.table(t("bronze_product")), product_transform,
             required=["row_id", "transaction_id", "customer_id", "amount_aed", "items_count"],
             fill_text=["merchant_category", "device_type", "browser", "return_option"],
             dup_keys=["transaction_id"])


# ============================================================
# 3. PAYMENT
# ============================================================
def payment_transform(df):
    return (df.withColumn("customer_id", F.upper("customer_id"))
              .withColumn("payment_method", nice_text("payment_method"))
              .withColumn("timestamp_utc", to_timestamp_any("timestamp_utc"))
              .withColumn("refund_option", nice_text("refund_option")))

build_silver("silver_payment", spark.table(t("bronze_payment")), payment_transform,
             required=["row_id", "customer_id", "timestamp_utc"],
             fill_text=["payment_method", "refund_option"],
             dup_keys=["customer_id", "payment_method", "timestamp_utc", "refund_option"])


# ============================================================
# 4. REPORT : what was removed and why
# ============================================================
display(spark.createDataFrame(summary,
        ["table", "bronze_rows", "empty_rows_removed", "missing_required_removed",
         "duplicates_removed", "silver_rows"]))


# ============================================================
# 5. VERIFY : NULL count per column in silver (should all be 0)
# ============================================================
for table in ["silver_customer", "silver_product", "silver_payment"]:
    df = spark.table(t(table))
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
    print(f"--- NULL check: {table} ---")
    display(nulls)