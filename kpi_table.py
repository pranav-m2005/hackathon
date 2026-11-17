# ============================================================
# RETAILIQ - 5 MANDATORY KPIs AS A TEXT REPORT
# Reads the gold tables, prints every KPI as text,
# and also saves the report as kpi_report.txt next to this file.
# ============================================================
import os
from datetime import datetime
import pandas as pd

S = "workspace.hackathon"

# ---------- settings: check them against the "DATA CHECK" section ----------
RETURN_VALUES = ["yes", "y", "true", "1", "returned", "return"]   # return_option values that mean "returned"
NON_DIGITAL   = ["cash", "cash on delivery", "cod", "unknown"]    # every other payment method counts as digital
TOP_N = 5
REPORT_FILE = os.path.join(os.getcwd(), "kpi_report.txt")


# ---------- print helpers (everything printed is also saved to the file) ----------
out = []
def p(text=""):
    print(text)
    out.append(str(text))

def title(t):
    p()
    p("=" * 78)
    p(t)
    p("=" * 78)

def show(df):
    p(df.to_string(index=False) if len(df) else "  (no data)")

def money(x):
    return "n/a" if pd.isna(x) else f"{x:,.2f}"

def pct(x):
    return "n/a" if pd.isna(x) else f"{x:.2f}%"


# ============================================================
# 0. ONE BASE TABLE: order + payment (date, method) + customer (region)
#    product and payment rows are linked by row_id + customer_id
# ============================================================
ret_list = ", ".join(f"'{v}'" for v in RETURN_VALUES)

base = spark.sql(f"""
SELECT p.transaction_id,
       p.customer_id,
       c.customer_name,
       p.merchant_category                 AS product,
       p.amount_aed                        AS amount,
       p.items_count,
       p.return_option,
       CASE WHEN lower(trim(p.return_option)) IN ({ret_list}) THEN 1 ELSE 0 END AS is_returned,
       y.payment_method,
       y.timestamp_utc                     AS ts,
       CASE WHEN y.timestamp_utc IS NOT NULL
            THEN concat(year(y.timestamp_utc), '-Q', quarter(y.timestamp_utc)) END AS qtr,
       c.shipping_city                     AS region
FROM {S}.gold_product p
LEFT JOIN {S}.gold_payment  y ON p.row_id = y.row_id AND p.customer_id = y.customer_id
LEFT JOIN {S}.gold_customer c ON p.customer_id = c.customer_id
""").toPandas()

base["amount"] = base["amount"].astype(float)
base["returned_amount"] = base["amount"] * base["is_returned"]


# ============================================================
# DATA CHECK
# ============================================================
title("RETAILIQ KPI REPORT  -  DATA CHECK")
p(f"Orders in gold_product           : {len(base):,}")
p(f"Orders linked to a payment/date  : {base['ts'].notna().sum():,}")
p(f"Orders linked to a customer      : {base['region'].notna().sum():,}")
p(f"\nreturn_option values (counted as returned: {RETURN_VALUES})")
show(base["return_option"].value_counts(dropna=False).rename_axis("return_option").reset_index(name="orders"))
p(f"\npayment_method values (non-digital: {NON_DIGITAL})")
show(base["payment_method"].value_counts(dropna=False).rename_axis("payment_method").reset_index(name="orders"))


# ============================================================
# KPI 1: ORDERS VS RETURNS BY CUSTOMER AND PRODUCT (TOP 5)
# ============================================================
title("KPI 1: ORDERS VS RETURNS BY CUSTOMER AND PRODUCT (TOP 5)")

def orders_returns(keys):
    g = (base.groupby(keys, dropna=False)
             .agg(orders=("transaction_id", "count"),
                  returns=("is_returned", "sum"),
                  sales_aed=("amount", "sum"))
             .reset_index())
    g["return_rate"] = g["returns"] / g["orders"] * 100
    return g

def fmt1(g):
    g = g.copy()
    g["sales_aed"] = g["sales_aed"].map(money)
    g["return_rate"] = g["return_rate"].map(pct)
    return g

cust = orders_returns(["customer_id", "customer_name"])
prod = orders_returns(["product"])

total_orders, total_returns = len(base), int(base["is_returned"].sum())
p(f"Overall: {total_orders:,} orders, {total_returns:,} returns, "
  f"return rate {pct(total_returns * 100 / total_orders if total_orders else float('nan'))}")

p(f"\nTop {TOP_N} customers by ORDERS:")
show(fmt1(cust.sort_values(["orders", "sales_aed"], ascending=False).head(TOP_N)))
p(f"\nTop {TOP_N} customers by RETURNS:")
show(fmt1(cust.sort_values(["returns", "return_rate"], ascending=False).head(TOP_N)))
p(f"\nTop {TOP_N} products by ORDERS:")
show(fmt1(prod.sort_values(["orders", "sales_aed"], ascending=False).head(TOP_N)))
p(f"\nTop {TOP_N} products by RETURNS:")
show(fmt1(prod.sort_values(["returns", "return_rate"], ascending=False).head(TOP_N)))


# ============================================================
# KPI 2: REGION-WISE CUSTOMER BASE AND CUSTOMER GROWTH QoQ
# ============================================================
title("KPI 2: REGION-WISE CUSTOMER BASE AND CUSTOMER GROWTH QoQ")
p("Region = customer's shipping city. Customers per quarter = customers who ordered in that quarter.")

reg = (base.dropna(subset=["region"])
           .groupby("region")["customer_id"].nunique()
           .sort_values(ascending=False)
           .reset_index(name="customers"))
reg["share"] = (reg["customers"] / reg["customers"].sum() * 100).map(pct)
top_regions = reg["region"].head(10).tolist()

p(f"\nCustomer base by region (top 10 of {len(reg)} regions):")
show(reg.head(10))

q = (base.dropna(subset=["region", "qtr"])
         .groupby(["region", "qtr"])["customer_id"].nunique()
         .reset_index(name="customers")
         .sort_values(["region", "qtr"]))
q["growth_qoq"] = q.groupby("region")["customers"].pct_change() * 100

if len(q):
    top_q = q[q["region"].isin(top_regions)]
    p("\nCustomers per quarter (top 10 regions):")
    show(top_q.pivot(index="region", columns="qtr", values="customers")
              .fillna(0).astype(int).loc[top_regions].reset_index())

    p("\nCustomer growth QoQ % (top 10 regions):")
    show(top_q.pivot(index="region", columns="qtr", values="growth_qoq")
              .loc[top_regions].apply(lambda col: col.map(pct)).reset_index())

    latest = q["qtr"].max()
    lat = q[(q["qtr"] == latest) & q["growth_qoq"].notna()].sort_values("growth_qoq", ascending=False)
    lat_f = lat.assign(growth_qoq=lat["growth_qoq"].map(pct))[["region", "customers", "growth_qoq"]]
    p(f"\nFastest growing regions in {latest}:")
    show(lat_f.head(5))
    p(f"\nSlowest / declining regions in {latest}:")
    show(lat_f.tail(5).iloc[::-1])
else:
    p("\n(no dates linked to orders, quarterly growth not available)")


# ============================================================
# KPI 3: PRODUCT-WISE SALES VS MARGIN
# ============================================================
title("KPI 3: PRODUCT-WISE SALES VS MARGIN")
p("Note: the dataset has no cost price, so margin = net sales after returns / sales x 100.")

pr = (base.groupby("product")
          .agg(orders=("transaction_id", "count"),
               sales=("amount", "sum"),
               returned_sales=("returned_amount", "sum"))
          .reset_index())
pr["net_sales"] = pr["sales"] - pr["returned_sales"]
pr["margin_pct"] = pr["net_sales"] / pr["sales"] * 100
pr["sales_share"] = pr["sales"] / pr["sales"].sum() * 100

med_sales, med_margin = pr["sales"].median(), pr["margin_pct"].median()
def performance(r):
    hi_s, hi_m = r["sales"] >= med_sales, r["margin_pct"] >= med_margin
    if hi_s and hi_m:   return "Star: grow"
    if hi_s:            return "High sales, low margin: fix returns"
    if hi_m:            return "Low sales, high margin: promote"
    return "Weak: review"
pr["performance"] = pr.apply(performance, axis=1)

pr = pr.sort_values("sales", ascending=False)
show(pr.assign(sales=pr["sales"].map(money),
               returned_sales=pr["returned_sales"].map(money),
               net_sales=pr["net_sales"].map(money),
               margin_pct=pr["margin_pct"].map(pct),
               sales_share=pr["sales_share"].map(pct))
       [["product", "orders", "sales", "sales_share", "returned_sales", "net_sales", "margin_pct", "performance"]])

if len(pr):
    best = pr.loc[pr["margin_pct"].idxmax()]
    worst = pr.loc[pr["margin_pct"].idxmin()]
    p(f"\nHighest margin product : {best['product']} ({pct(best['margin_pct'])})")
    p(f"Lowest margin product  : {worst['product']} ({pct(worst['margin_pct'])})")


# ============================================================
# KPI 4: % OF DIGITAL PAYMENTS
# ============================================================
title("KPI 4: % OF DIGITAL PAYMENTS")
p("Digital % = digital payment amount / total payment amount x 100")

pay = base.dropna(subset=["payment_method"]).copy()
pay["type"] = (~pay["payment_method"].str.lower().str.strip().isin(NON_DIGITAL)).map(
    {True: "Digital", False: "Non-digital"})

if len(pay):
    dig = pay["type"] == "Digital"
    p(f"\nDigital payments by AMOUNT : {pct(pay.loc[dig, 'amount'].sum() / pay['amount'].sum() * 100)}")
    p(f"Digital payments by COUNT  : {pct(dig.mean() * 100)}")

    m = (pay.groupby(["payment_method", "type"])
            .agg(payments=("transaction_id", "count"), amount=("amount", "sum"))
            .reset_index().sort_values("amount", ascending=False))
    m["amount_share"] = m["amount"] / m["amount"].sum() * 100
    p("\nBy payment method:")
    show(m.assign(amount=m["amount"].map(money), amount_share=m["amount_share"].map(pct)))

    t = pay.dropna(subset=["qtr"])
    if len(t):
        tq = (t.assign(dig_amt=t["amount"].where(t["type"] == "Digital", 0))
               .groupby("qtr").agg(total=("amount", "sum"), digital=("dig_amt", "sum"))
               .reset_index().sort_values("qtr"))
        tq["digital_pct"] = (tq["digital"] / tq["total"] * 100).map(pct)
        p("\nDigital % trend by quarter:")
        show(tq.assign(total=tq["total"].map(money), digital=tq["digital"].map(money)))
else:
    p("\n(no payment methods linked to orders)")


# ============================================================
# KPI 5: SALES GROWTH QoQ
# ============================================================
title("KPI 5: SALES GROWTH QoQ")

qs = (base.dropna(subset=["qtr"])
          .groupby("qtr")
          .agg(orders=("transaction_id", "count"), sales=("amount", "sum"))
          .reset_index().sort_values("qtr"))
qs["growth_qoq"] = qs["sales"].pct_change() * 100

if len(qs):
    show(qs.assign(sales=qs["sales"].map(money), growth_qoq=qs["growth_qoq"].map(pct)))
    g = qs.dropna(subset=["growth_qoq"])
    if len(g):
        b, w = g.loc[g["growth_qoq"].idxmax()], g.loc[g["growth_qoq"].idxmin()]
        p(f"\nBest quarter   : {b['qtr']} ({pct(b['growth_qoq'])})")
        p(f"Worst quarter  : {w['qtr']} ({pct(w['growth_qoq'])})")
        p(f"Average growth : {pct(g['growth_qoq'].mean())}")
    p("Note: the first and last quarters may be partial if the data starts or ends mid-quarter.")
else:
    p("(no dates linked to orders, sales growth not available)")


# ============================================================
# SAVE THE REPORT AS TEXT
# ============================================================
title(f"Report generated {datetime.now():%d %b %Y %H:%M}")
try:
    with open(REPORT_FILE, "w") as f:
        f.write("\n".join(out))
    print(f"Saved to {REPORT_FILE}")
except Exception as e:
    print(f"Could not save the text file ({e}); copy the output above instead.")