# ============================================================
# SILVER -> GOLD : VALIDATE FORMATS
# pass  -> gold_customer / gold_product / gold_payment
# fail  -> gold_customer_rejected / ... (with reject_reason)
# ============================================================
from pyspark.sql import functions as F

SCHEMA = "workspace.hackathon"
def t(name):
    return f"{SCHEMA}.{name}"

CHECK_CUSTOMER_EXISTS = True   # product/payment must belong to a valid gold customer
summary = []

# ---------------- common patterns ----------------
ID_PATTERN   = r"^[A-Za-z0-9_-]+$"          # IDs: letters, digits, _ or -
NAME_PATTERN = r"^[A-Za-z][A-Za-z .'-]+$"   # names: letters, spaces, . ' -
TEXT_PATTERN = r"^[A-Za-z][A-Za-z0-9 &/.'-]*$"


# ---------------- helper functions ----------------
def normalise_phone(c):
    """Make phone a 10-digit number where possible.
       +91 9876543210  -> 9876543210
       09876543210     -> 9876543210
       +971 50 123 4567-> 0501234567
    """
    d = F.regexp_replace(F.col(c), "[^0-9]", "")
    return (F.when((F.length(d) == 12) & d.startswith("91"),  F.expr(f"right(regexp_replace({c}, '[^0-9]', ''), 10)"))
             .when((F.length(d) == 12) & d.startswith("971"), F.concat(F.lit("0"), F.expr(f"right(regexp_replace({c}, '[^0-9]', ''), 9)")))
             .when((F.length(d) == 11) & d.startswith("0"),   F.expr(f"right(regexp_replace({c}, '[^0-9]', ''), 10)"))
             .otherwise(d))

def validate(df, rules):
    """rules = list of (rule_name, condition_that_must_be_TRUE).
       Adds reject_reason listing every failed rule (NULL = row is good)."""
    failed = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules]
    reason = F.concat_ws(", ", *failed)
    return df.withColumn("reject_reason", F.when(reason == "", None).otherwise(reason))

def split_and_save(df, table):
    good = df.filter("reject_reason IS NULL").drop("reject_reason")
    bad  = df.filter("reject_reason IS NOT NULL")

    good.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(table))
    bad.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(f"{table}_rejected"))

    n_good = spark.table(t(table)).count()
    n_bad = spark.table(t(f"{table}_rejected")).count()
    summary.append((table, n_good + n_bad, n_good, n_bad))
    print(f"{table}: passed {n_good} | rejected {n_bad}")


# ============================================================
# 1. CUSTOMER
# ============================================================
c = (spark.table(t("silver_customer"))
     .withColumn("contact_number", normalise_phone("contact_number")))

c = validate(c, [
    ("bad_customer_id",   F.col("customer_id").rlike(ID_PATTERN)),
    ("bad_name",          F.col("customer_name").rlike(NAME_PATTERN) & (F.col("customer_name") != "Unknown")),
    ("phone_not_10_digits", F.col("contact_number").rlike(r"^[0-9]{10}$")),
    ("bad_shipping_city", F.col("shipping_city").rlike(NAME_PATTERN)),
    ("bad_billing_city",  F.col("billing_city").rlike(NAME_PATTERN)),
])
split_and_save(c, "gold_customer")

valid_customers = spark.table(t("gold_customer")).select("customer_id").withColumn("_ok", F.lit(True))


# ============================================================
# 2. PRODUCT
# ============================================================
p = (spark.table(t("silver_product"))
     .withColumn("amount_aed", F.round("amount_aed", 2)))

if CHECK_CUSTOMER_EXISTS:
    p = p.join(valid_customers, "customer_id", "left")

rules = [
    ("bad_transaction_id",   F.col("transaction_id").rlike(ID_PATTERN)),
    ("bad_customer_id",      F.col("customer_id").rlike(ID_PATTERN)),
    ("amount_not_positive",  F.col("amount_aed") > 0),
    ("items_count_invalid",  F.col("items_count").between(1, 1000)),
    ("bad_category",         F.col("merchant_category").rlike(TEXT_PATTERN)),
    ("bad_device_type",      F.col("device_type").rlike(TEXT_PATTERN)),
    ("bad_browser",          F.col("browser").rlike(TEXT_PATTERN)),
    ("bad_return_option",    F.col("return_option").rlike(TEXT_PATTERN)),
]
if CHECK_CUSTOMER_EXISTS:
    rules.append(("customer_not_in_gold", F.col("_ok").isNotNull()))

p = validate(p, rules)
if CHECK_CUSTOMER_EXISTS:
    p = p.drop("_ok")
split_and_save(p, "gold_product")


# ============================================================
# 3. PAYMENT
# ============================================================
y = spark.table(t("silver_payment"))

if CHECK_CUSTOMER_EXISTS:
    y = y.join(valid_customers, "customer_id", "left")

rules = [
    ("bad_customer_id",     F.col("customer_id").rlike(ID_PATTERN)),
    ("bad_payment_method",  F.col("payment_method").rlike(TEXT_PATTERN)),
    ("bad_refund_option",   F.col("refund_option").rlike(TEXT_PATTERN)),
    ("date_too_old",        F.col("timestamp_utc") >= F.lit("2000-01-01").cast("timestamp")),
    ("date_in_future",      F.col("timestamp_utc") <= F.current_timestamp()),
]
if CHECK_CUSTOMER_EXISTS:
    rules.append(("customer_not_in_gold", F.col("_ok").isNotNull()))

y = validate(y, rules)
if CHECK_CUSTOMER_EXISTS:
    y = y.drop("_ok")
split_and_save(y, "gold_payment")


# ============================================================
# 4. REPORT
# ============================================================
display(spark.createDataFrame(summary, ["table", "silver_rows", "passed_to_gold", "rejected"]))

for table in ["gold_customer", "gold_product", "gold_payment"]:
    print(f"--- why rows were rejected: {table} ---")
    display(spark.table(t(f"{table}_rejected"))
            .groupBy("reject_reason").count().orderBy(F.desc("count")))


# ============================================================
# 5. FINAL CHECK ON GOLD (all should be 0)
# ============================================================
gc = spark.table(t("gold_customer"))
gp = spark.table(t("gold_product"))
gy = spark.table(t("gold_payment"))

checks = [
    ("customer: phone not 10 digits", gc.filter(~F.col("contact_number").rlike(r"^[0-9]{10}$")).count()),
    ("customer: duplicate customer_id", gc.count() - gc.select("customer_id").distinct().count()),
    ("product: duplicate transaction_id", gp.count() - gp.select("transaction_id").distinct().count()),
    ("product: amount <= 0", gp.filter("amount_aed <= 0").count()),
    ("payment: future dates", gy.filter(F.col("timestamp_utc") > F.current_timestamp()).count()),
]
for name, df in [("gold_customer", gc), ("gold_product", gp), ("gold_payment", gy)]:
    nulls = sum(df.filter(F.col(col).isNull()).count() for col in df.columns)
    checks.append((f"{name}: total NULL values", nulls))

display(spark.createDataFrame(checks, ["check", "failed_rows"]))