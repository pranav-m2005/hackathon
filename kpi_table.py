# ============================================================
# GOLD : KPI CARDS + DATA QUALITY (CLEANING RESULTS)
# Creates: gold_kpi_cards, gold_data_quality
# ============================================================
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

SCHEMA = "workspace.hackathon"
def t(name):
    return f"{SCHEMA}.{name}"

AUDIT = ["row_id", "source_file", "ingested_at"]

gc = spark.table(t("gold_customer"))
gp = spark.table(t("gold_product"))
gy = spark.table(t("gold_payment"))

kpis = []


# ---------------- helper functions ----------------
def add(group, name, value, unit="", display_text=None):
    """Add one KPI card."""
    value = round(float(value or 0), 2)
    if display_text is None:
        if unit == "AED":
            display_text = f"AED {value:,.2f}"
        elif unit == "%":
            display_text = f"{value:.2f}%"
        elif value == int(value):
            display_text = f"{int(value):,}"
        else:
            display_text = f"{value:,.2f}"
    kpis.append((len(kpis) + 1, group, name, value, display_text, unit))

def top(df, col, metric_col=None):
    """Most common / highest-revenue value of a column -> (value, metric, share %)."""
    metric = F.sum(metric_col) if metric_col else F.count("*")
    g = df.groupBy(col).agg(metric.alias("m"))
    total = g.agg(F.sum("m")).first()[0] or 0
    r = g.orderBy(F.desc("m")).first()
    if r is None or total == 0:
        return ("None", 0.0, 0.0)
    return (str(r[col]), float(r["m"]), float(r["m"]) * 100 / total)

def pct(a, b):
    return (a * 100 / b) if b else 0.0

def count_empty_rows(df):
    cond = F.lit(True)
    for c in df.columns:
        if c not in AUDIT:
            cond = cond & F.col(c).isNull()
    return df.filter(cond).count()


# ============================================================
# 1. SALES KPIs
# ============================================================
s = gp.agg(
    F.sum("amount_aed").alias("revenue"),
    F.count("*").alias("orders"),
    F.sum("items_count").alias("items"),
    F.max("amount_aed").alias("max_order"),
    F.min("amount_aed").alias("min_order"),
).first()

revenue, orders, items = float(s["revenue"] or 0), s["orders"], s["items"] or 0

add("Sales", "Total Revenue", revenue, "AED")
add("Sales", "Total Orders", orders)
add("Sales", "Average Order Value", revenue / orders if orders else 0, "AED")
add("Sales", "Total Items Sold", items)
add("Sales", "Average Items per Order", items / orders if orders else 0)
add("Sales", "Highest Order Value", s["max_order"], "AED")
add("Sales", "Lowest Order Value", s["min_order"], "AED")


# ============================================================
# 2. CUSTOMER KPIs
# ============================================================
total_customers = gc.count()
per_customer = gp.groupBy("customer_id").count()
buyers = per_customer.count()
repeat_buyers = per_customer.filter("count > 1").count()

add("Customers", "Total Customers", total_customers)
add("Customers", "Active Customers (bought at least once)", buyers)
add("Customers", "Active Customer Rate", pct(buyers, total_customers), "%")
add("Customers", "Repeat Customer Rate", pct(repeat_buyers, buyers), "%")
add("Customers", "Revenue per Customer", revenue / buyers if buyers else 0, "AED")

sales_city = gp.join(gc.select("customer_id", "shipping_city"), "customer_id")
city, city_rev, city_share = top(sales_city, "shipping_city", "amount_aed")
add("Customers", "Top City by Revenue", city_share, "%", f"{city} ({city_share:.1f}% of revenue)")
add("Customers", "Number of Cities", gc.select("shipping_city").distinct().count())


# ============================================================
# 3. PRODUCT KPIs
# ============================================================
cat, cat_rev, cat_share = top(gp, "merchant_category", "amount_aed")
add("Products", "Top Category by Revenue", cat_share, "%", f"{cat} ({cat_share:.1f}% of revenue)")
add("Products", "Number of Categories", gp.select("merchant_category").distinct().count())

dev, _, dev_share = top(gp, "device_type")
add("Products", "Top Device", dev_share, "%", f"{dev} ({dev_share:.1f}% of orders)")

brw, _, brw_share = top(gp, "browser")
add("Products", "Top Browser", brw_share, "%", f"{brw} ({brw_share:.1f}% of orders)")

ret, _, ret_share = top(gp, "return_option")
add("Products", "Most Common Return Option", ret_share, "%", f"{ret} ({ret_share:.1f}% of orders)")


# ============================================================
# 4. PAYMENT KPIs
# ============================================================
total_payments = gy.count()
add("Payments", "Total Payments", total_payments)

pm, _, pm_share = top(gy, "payment_method")
add("Payments", "Top Payment Method", pm_share, "%", f"{pm} ({pm_share:.1f}% of payments)")
add("Payments", "Number of Payment Methods", gy.select("payment_method").distinct().count())

rf, _, rf_share = top(gy, "refund_option")
add("Payments", "Most Common Refund Option", rf_share, "%", f"{rf} ({rf_share:.1f}% of payments)")

d = gy.agg(F.min("timestamp_utc").alias("first"), F.max("timestamp_utc").alias("last")).first()
if d["first"] and d["last"]:
    days = (d["last"] - d["first"]).days
    add("Payments", "Data Period (days)", days, "days",
        f"{d['first']:%d %b %Y} to {d['last']:%d %b %Y} ({days} days)")


# ============================================================
# 5. DATA QUALITY TABLE (results of cleaning)
# ============================================================
dq_rows = []
for entity in ["customer", "product", "payment"]:
    bronze = spark.table(t(f"bronze_{entity}"))
    bronze_total = bronze.count()
    empty = count_empty_rows(bronze)
    silver = spark.table(t(f"silver_{entity}")).count()
    gold = spark.table(t(f"gold_{entity}")).count()
    rejected = spark.table(t(f"gold_{entity}_rejected")).count()

    real_rows = bronze_total - empty
    removed_silver = real_rows - silver          # duplicates + missing required values

    dq_rows.append((
        entity, bronze_total, empty, real_rows,
        removed_silver, silver, rejected, gold,
        round(pct(gold, real_rows), 2),
    ))

dq_schema = StructType([
    StructField("table_name", StringType()),
    StructField("bronze_rows", LongType()),
    StructField("empty_rows_removed", LongType()),
    StructField("real_rows", LongType()),
    StructField("duplicates_or_missing_removed", LongType()),
    StructField("silver_rows", LongType()),
    StructField("rejected_in_gold", LongType()),
    StructField("gold_rows", LongType()),
    StructField("data_quality_score_pct", DoubleType()),
])

dq = (spark.createDataFrame(dq_rows, dq_schema)
      .withColumn("calculated_at", F.current_timestamp()))

dq.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_data_quality"))


# ============================================================
# 6. DATA QUALITY KPI CARDS (totals across all tables)
# ============================================================
tot = {k: sum(r[i] for r in dq_rows) for k, i in
       [("bronze", 1), ("empty", 2), ("real", 3), ("removed", 4), ("silver", 5), ("rejected", 6), ("gold", 7)]}

add("Data Quality", "Total Rows Ingested (Bronze)", tot["bronze"])
add("Data Quality", "Empty Rows Removed", tot["empty"])
add("Data Quality", "Duplicates / Missing Values Removed (Silver)", tot["removed"])
add("Data Quality", "Rows Rejected by Format Checks (Gold)", tot["rejected"])
add("Data Quality", "Clean Rows in Gold", tot["gold"])
add("Data Quality", "Overall Data Quality Score", pct(tot["gold"], tot["real"]), "%")


# ============================================================
# 7. SAVE KPI CARDS TABLE
# ============================================================
kpi_schema = StructType([
    StructField("sort_order", IntegerType()),
    StructField("kpi_group", StringType()),
    StructField("kpi_name", StringType()),
    StructField("kpi_value", DoubleType()),
    StructField("kpi_display", StringType()),
    StructField("unit", StringType()),
])

kpi_df = (spark.createDataFrame(kpis, kpi_schema)
          .withColumn("calculated_at", F.current_timestamp()))

kpi_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("gold_kpi_cards"))


# ============================================================
# 8. SHOW RESULTS
# ============================================================
print("===== KPI CARDS =====")
display(spark.table(t("gold_kpi_cards")).orderBy("sort_order"))

print("===== DATA QUALITY (CLEANING RESULTS) =====")
display(spark.table(t("gold_data_quality")))