# ============================================================
# KPI REPORT : PRINT ONLY (no tables created)
# ============================================================
S = "workspace.hackathon"

def money(x): return f"AED {float(x or 0):,.2f}"
def num(x):   return f"{int(x or 0):,}"
def pct(a, b): return f"{(a * 100 / b) if b else 0:.2f}%"
def line(name, value): print(f"  {name:<40} {value}")


# ---------------- 1. SALES (1 query) ----------------
s = spark.sql(f"""
SELECT SUM(amount_aed)              AS revenue,
       COUNT(*)                     AS orders,
       SUM(items_count)             AS items,
       MAX(amount_aed)              AS max_order,
       MIN(amount_aed)              AS min_order,
       COUNT(DISTINCT customer_id)  AS buyers,
       COUNT(DISTINCT merchant_category) AS categories
FROM {S}.gold_product
""").first()

revenue = float(s.revenue or 0)
orders  = s.orders or 0


# ---------------- 2. CUSTOMERS (1 query) ----------------
c = spark.sql(f"""
SELECT (SELECT COUNT(*) FROM {S}.gold_customer)                         AS total_customers,
       (SELECT COUNT(DISTINCT shipping_city) FROM {S}.gold_customer)    AS cities,
       (SELECT COUNT(*) FROM (SELECT customer_id FROM {S}.gold_product
                              GROUP BY customer_id HAVING COUNT(*) > 1)) AS repeat_buyers
""").first()


# ---------------- 3. TOP VALUES (1 query) ----------------
tops = {r.kpi: r for r in spark.sql(f"""
WITH allv AS (
  SELECT 'city'     AS kpi, c.shipping_city AS val, CAST(SUM(p.amount_aed) AS DOUBLE) AS m
  FROM {S}.gold_product p JOIN {S}.gold_customer c USING (customer_id) GROUP BY c.shipping_city
  UNION ALL
  SELECT 'category', merchant_category, CAST(SUM(amount_aed) AS DOUBLE) FROM {S}.gold_product GROUP BY merchant_category
  UNION ALL
  SELECT 'device',   device_type,   CAST(COUNT(*) AS DOUBLE) FROM {S}.gold_product GROUP BY device_type
  UNION ALL
  SELECT 'browser',  browser,       CAST(COUNT(*) AS DOUBLE) FROM {S}.gold_product GROUP BY browser
  UNION ALL
  SELECT 'return',   return_option, CAST(COUNT(*) AS DOUBLE) FROM {S}.gold_product GROUP BY return_option
  UNION ALL
  SELECT 'payment',  payment_method, CAST(COUNT(*) AS DOUBLE) FROM {S}.gold_payment GROUP BY payment_method
  UNION ALL
  SELECT 'refund',   refund_option,  CAST(COUNT(*) AS DOUBLE) FROM {S}.gold_payment GROUP BY refund_option
)
SELECT kpi, val, m, share FROM (
  SELECT kpi, val, m,
         m * 100 / SUM(m) OVER (PARTITION BY kpi)            AS share,
         ROW_NUMBER() OVER (PARTITION BY kpi ORDER BY m DESC) AS rn
  FROM allv
) WHERE rn = 1
""").collect()}

def top(key, what):
    r = tops.get(key)
    return f"{r.val} ({r.share:.1f}% of {what})" if r else "n/a"


# ---------------- 4. PAYMENTS (1 query) ----------------
y = spark.sql(f"""
SELECT COUNT(*)                       AS payments,
       COUNT(DISTINCT payment_method) AS methods,
       MIN(timestamp_utc)             AS first_ts,
       MAX(timestamp_utc)             AS last_ts
FROM {S}.gold_payment
""").first()


# ---------------- 5. DATA QUALITY (1 query) ----------------
DATA_COLS = {
    "customer": ["customer_id", "customer_name", "contact_number", "shipping_city", "billing_city"],
    "product":  ["customer_id", "merchant_category", "amount_aed", "items_count",
                 "device_type", "browser", "transaction_id", "return_option"],
    "payment":  ["customer_id", "payment_method", "timestamp_utc", "refund_option"],
}

parts = []
for e, cols in DATA_COLS.items():
    empty_cond = " AND ".join(f"{col} IS NULL" for col in cols)
    parts.append(f"""
    SELECT '{e}' AS tbl,
      (SELECT COUNT(*) FROM {S}.bronze_{e})                     AS bronze,
      (SELECT COUNT(*) FROM {S}.bronze_{e} WHERE {empty_cond})  AS empty,
      (SELECT COUNT(*) FROM {S}.silver_{e})                     AS silver,
      (SELECT COUNT(*) FROM {S}.gold_{e}_rejected)              AS rejected,
      (SELECT COUNT(*) FROM {S}.gold_{e})                       AS gold""")

dq = spark.sql(" UNION ALL ".join(parts)).collect()


# ============================================================
# PRINT REPORT
# ============================================================
print("=" * 70)
print("                 RETAIL SALES KPI REPORT")
print("=" * 70)

print("\nSALES")
line("Total Revenue", money(revenue))
line("Total Orders", num(orders))
line("Average Order Value", money(revenue / orders if orders else 0))
line("Total Items Sold", num(s.items))
line("Average Items per Order", f"{(s.items or 0) / orders if orders else 0:.2f}")
line("Highest Order Value", money(s.max_order))
line("Lowest Order Value", money(s.min_order))

print("\nCUSTOMERS")
line("Total Customers", num(c.total_customers))
line("Active Customers (bought at least once)", num(s.buyers))
line("Active Customer Rate", pct(s.buyers, c.total_customers))
line("Repeat Customer Rate", pct(c.repeat_buyers, s.buyers))
line("Revenue per Customer", money(revenue / s.buyers if s.buyers else 0))
line("Number of Cities", num(c.cities))
line("Top City", top("city", "revenue"))

print("\nPRODUCTS")
line("Number of Categories", num(s.categories))
line("Top Category", top("category", "revenue"))
line("Top Device", top("device", "orders"))
line("Top Browser", top("browser", "orders"))
line("Most Common Return Option", top("return", "orders"))

print("\nPAYMENTS")
line("Total Payments", num(y.payments))
line("Number of Payment Methods", num(y.methods))
line("Top Payment Method", top("payment", "payments"))
line("Most Common Refund Option", top("refund", "payments"))
if y.first_ts and y.last_ts:
    line("Data Period", f"{y.first_ts:%d %b %Y} to {y.last_ts:%d %b %Y} "
                        f"({(y.last_ts - y.first_ts).days} days)")

print("\nDATA QUALITY (CLEANING RESULTS)")
print(f"  {'table':<10}{'bronze':>9}{'empty':>9}{'real':>9}{'silver':>9}"
      f"{'rejected':>10}{'gold':>9}{'score':>9}")
tot = dict(bronze=0, empty=0, silver=0, rejected=0, gold=0)
for r in dq:
    real = r.bronze - r.empty
    print(f"  {r.tbl:<10}{r.bronze:>9,}{r.empty:>9,}{real:>9,}{r.silver:>9,}"
          f"{r.rejected:>10,}{r.gold:>9,}{pct(r.gold, real):>9}")
    for k in tot:
        tot[k] += getattr(r, k)

real_total = tot["bronze"] - tot["empty"]
print()
line("Total Rows Ingested (Bronze)", num(tot["bronze"]))
line("Empty Rows Removed", num(tot["empty"]))
line("Duplicates / Missing Removed (Silver)", num(real_total - tot["silver"]))
line("Rejected by Format Checks (Gold)", num(tot["rejected"]))
line("Clean Rows in Gold", num(tot["gold"]))
line("Overall Data Quality Score", pct(tot["gold"], real_total))
print("=" * 70)