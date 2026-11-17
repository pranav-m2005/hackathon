# ============================================================
# LOAD 3 EXCEL FILES -> BRONZE TABLES (with column check)
# Run this from the "hackathon" folder in Databricks
# Needs openpyxl (add "openpyxl" to req.txt / Environment if missing)
# ============================================================
import os
import re
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

CATALOG_SCHEMA = "workspace.hackathon"   # where you ran the CREATE TABLE statements
BASE_PATH = os.getcwd()                # folder of this file = hackathon repo
WRITE_MODE = "overwrite"               # "overwrite" = safe to re-run, "append" = keep adding

SOURCES = {
    "bronze_customer": "Customer Data.xlsx",
    "bronze_product":  "Product Data.xlsx",
    "bronze_payment":  "Payment Data.xlsx",
}

# Columns we add ourselves (not expected in Excel)
AUDIT_COLS = ["source_file", "ingested_at"]

# If an Excel header doesn't match the table column after cleaning, map it here
# e.g. "bronze_payment": {"timestamp": "timestamp_utc"}
RENAME = {
    "bronze_customer": {},
    "bronze_product":  {},
    "bronze_payment":  {},
}


def clean_col(name):
    """'Customer ID ' -> 'customer_id', 'Amount (AED)' -> 'amount_aed'"""
    name = str(name).strip().lower()
    name = re.sub(r"[^0-9a-z]+", "_", name)
    return name.strip("_")


def check_columns(table, excel_cols, expected_cols):
    """Prints a match report. Returns list of missing columns."""
    missing = [c for c in expected_cols if c not in excel_cols]
    extra = [c for c in excel_cols if c not in expected_cols]

    # row_id can be generated if Excel doesn't have it
    missing_real = [c for c in missing if c != "row_id"]

    print(f"\n----- {table} -----")
    print(f"Excel columns : {excel_cols}")
    print(f"Table columns : {expected_cols}")
    if not missing and not extra:
        print("RESULT: all columns match")
    if "row_id" in missing:
        print("NOTE  : row_id not in Excel -> will be generated (1, 2, 3...)")
    if missing_real:
        print(f"MISSING in Excel (add to RENAME map): {missing_real}")
    if extra:
        print(f"EXTRA in Excel (will be ignored)    : {extra}")
    return missing_real


def load_excel_to_bronze(table, file_name):
    full_table = f"{CATALOG_SCHEMA}.{table}"
    path = os.path.join(BASE_PATH, file_name)

    # 1. Read Excel as text (bronze = raw, no type changes)
    pdf = pd.read_excel(path, dtype=str, engine="openpyxl")
    pdf.columns = [clean_col(c) for c in pdf.columns]
    pdf = pdf.rename(columns=RENAME.get(table, {}))

    # 2. Compare with the real table schema
    target_schema = spark.table(full_table).schema
    target_cols = [f.name for f in target_schema]
    expected_cols = [c for c in target_cols if c not in AUDIT_COLS]

    missing = check_columns(table, list(pdf.columns), expected_cols)
    if missing:
        raise ValueError(f"{table}: columns missing in {file_name}: {missing}")

    # 3. Generate row_id if needed
    if "row_id" in expected_cols and "row_id" not in pdf.columns:
        pdf["row_id"] = [str(i) for i in range(1, len(pdf) + 1)]

    # 4. Keep only table columns, NaN -> NULL
    pdf = pdf[expected_cols].astype(object).where(pdf[expected_cols].notna(), None)
    string_schema = StructType([StructField(c, StringType(), True) for c in expected_cols])
    sdf = spark.createDataFrame(pdf, schema=string_schema)

    # 5. Add audit columns
    sdf = (sdf
           .withColumn("source_file", F.lit(file_name))
           .withColumn("ingested_at", F.current_timestamp()))

    # 6. Cast to the table's types, in the table's column order
    sdf = sdf.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in target_schema])

    # 7. Write
    sdf.write.mode(WRITE_MODE).saveAsTable(full_table)
    print(f"LOADED: {sdf.count()} rows -> {full_table}")


# ============================================================
# RUN
# ============================================================
for table, file_name in SOURCES.items():
    load_excel_to_bronze(table, file_name)

# Quick verification
for table in SOURCES:
    n = spark.table(f"{CATALOG_SCHEMA}.{table}").count()
    print(f"{table}: {n} rows")

display(spark.table(f"{CATALOG_SCHEMA}.bronze_customer").limit(5))