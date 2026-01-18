import argparse
import math
import re
from typing import List

import pandas as pd

# --------------------
# Regex patterns
# --------------------
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

# --------------------
# Canonical mappings
# (extendable)
# --------------------
CANONICAL_MAPS = {
    "country": {
        "turkye": "Turkey",
        "türkiye": "Turkey",
        "turkey": "Turkey",
        "germeny": "Germany",
        "germany": "Germany",
        "frence": "France",
        "france": "France",
    },
    "department": {
        "suport": "Support",
        "support": "Support",
        "operatons": "Operations",
        "operations": "Operations",
        "marketng": "Marketing",
        "marketing": "Marketing",
        "salles": "Sales",
        "sales": "Sales",
        "finnance": "Finance",
        "finance": "Finance",
        "leegal": "Legal",
        "legal": "Legal",
    },
}

CATEGORICAL_COLUMNS = [
    "country",
    "city",
    "department",
    "category",
    "payment_method",
    "status",
    "tier",
    "region_code",
]

NUMERIC_COLUMNS = [
    "quantity",
    "unit_price",
    "discount_percent",
    "tax_rate",
    "loyalty_points",
    "rating",
    "total_amount",
]


# --------------------
# Helpers
# --------------------
def is_missing(v) -> bool:
    return pd.isna(v) or (isinstance(v, str) and v.strip() == "")


def normalize_text(v: str) -> str:
    return " ".join(v.strip().split()).casefold()


def is_ascii(v: str) -> bool:
    try:
        v.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def safe_float(v):
    if is_missing(v):
        return None
    try:
        return float(str(v).strip())
    except Exception:
        return "__PARSE_FAIL__"


# --------------------
# ETL logic
# --------------------
def apply_canonical_mapping(df: pd.DataFrame) -> None:
    for col, mapping in CANONICAL_MAPS.items():
        if col not in df.columns:
            continue
        df[col] = (
            df[col]
            .fillna("")
            .astype(str)
            .apply(lambda x: mapping.get(normalize_text(x), x.strip()))
        )


def validate_row(row) -> List[str]:
    errors = []

    # Email
    email = row.get("email")
    if not is_missing(email):
        if not is_ascii(email) or not EMAIL_RE.match(email):
            errors.append("INVALID_EMAIL")

    # Numeric checks
    for col in ["quantity", "unit_price", "total_amount", "loyalty_points"]:
        v = safe_float(row.get(col))
        if v == "__PARSE_FAIL__" or (v is not None and v < 0):
            errors.append(f"INVALID_{col.upper()}")

    for col in ["discount_percent", "tax_rate"]:
        v = safe_float(row.get(col))
        if v == "__PARSE_FAIL__" or (v is not None and not (0 <= v <= 100)):
            errors.append(f"INVALID_{col.upper()}")

    # Date
    if not is_missing(row.get("order_date")):
        try:
            pd.to_datetime(row["order_date"], format="%Y-%m-%d")
        except Exception:
            errors.append("INVALID_ORDER_DATE")

    # Semantic check: total_amount
    try:
        q = safe_float(row.get("quantity"))
        up = safe_float(row.get("unit_price"))
        d = safe_float(row.get("discount_percent"))
        t = safe_float(row.get("tax_rate"))
        tot = safe_float(row.get("total_amount"))

        if None not in (q, up, d, t, tot):
            expected = q * up * (1 - d / 100) * (1 + t / 100)
            if abs(tot - expected) > 0.05:
                errors.append("TOTAL_AMOUNT_MISMATCH")
    except Exception:
        errors.append("TOTAL_AMOUNT_MISMATCH")

    return errors


# --------------------
# Main
# --------------------
def main():
    ap = argparse.ArgumentParser(description="ETL clean + reject pipeline")
    ap.add_argument("--input", default="data/raw/large_dataset.csv")
    ap.add_argument("--out-clean", default="data/clean/clean_transactions.csv")
    ap.add_argument("--out-reject", default="data/reject/rejected_transactions.csv")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--max-rows", type=int, default=0)
    args = ap.parse_args()

    # Output dirs
    import os

    for file_path in [args.out_clean, args.out_reject]:
        dir_path = os.path.dirname(file_path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

    reader = pd.read_csv(
        args.input,
        dtype=str,
        chunksize=args.chunksize,
        encoding="utf-8",
        encoding_errors="replace",
        low_memory=False,
    )

    clean_chunks = []
    reject_chunks = []

    processed = 0

    for df in reader:
        if args.max_rows and processed >= args.max_rows:
            break

        processed += len(df)

        # Normalize categoricals
        apply_canonical_mapping(df)

        # Missing rules
        if "region_code" in df.columns:
            df["region_code"] = df["region_code"].fillna("UNKNOWN")

        # Validate rows
        error_lists = df.apply(validate_row, axis=1)
        df["reject_reason"] = error_lists.apply(lambda x: ";".join(x))

        clean_df = df[df["reject_reason"] == ""].drop(columns=["reject_reason"])
        reject_df = df[df["reject_reason"] != ""]

        clean_chunks.append(clean_df)
        reject_chunks.append(reject_df)

        print(f"processed {processed:,} rows")

    # Write outputs
    pd.concat(clean_chunks).to_csv(args.out_clean, index=False)
    pd.concat(reject_chunks).to_csv(args.out_reject, index=False)

    print("ETL completed")
    print(f"Clean rows written to: {args.out_clean}")
    print(f"Rejected rows written to: {args.out_reject}")


if __name__ == "__main__":
    main()
