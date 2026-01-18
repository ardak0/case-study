import argparse
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date

import pandas as pd


EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
TXN_RE = re.compile(r"^TXN\d{10}$")
CUST_RE = re.compile(r"^CUST\d{5}$")
PROD_CODE_RE = re.compile(r"^[A-Z0-9]{8}$")


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


def norm_text(value: str) -> str:
    return " ".join(value.strip().split()).casefold()


def is_missing(value: object) -> bool:
    # Handles pandas scalar missing values (e.g., <NA>, NaT) as well.
    if pd.isna(value):
        return True
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def is_ascii(value: str) -> bool:
    try:
        value.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False


def safe_float(value: object):
    if is_missing(value):
        return pd.NA
    try:
        return float(str(value).strip())
    except Exception:
        return "__PARSE_FAIL__"


@dataclass
class NumericChecks:
    parse_fail: int = 0
    missing: int = 0
    negative: int = 0
    out_of_range: int = 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Chunked data-quality checks for large_dataset.csv")
    ap.add_argument("--path", default="data/raw/large_dataset.csv")
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--max-rows", type=int, default=0, help="0 means no limit")
    ap.add_argument("--top-k", type=int, default=25)
    args = ap.parse_args()

    path = args.path

    total_rows = 0

    missing_counts = Counter()
    whitespace_issues = Counter()
    newline_issues = Counter()

    # pattern checks
    invalid_email = 0
    email_non_ascii = 0
    email_structurally_invalid = 0
    invalid_phone = 0
    invalid_txn_id = 0
    invalid_customer_id = 0
    invalid_product_code = 0

    example_invalid_emails = []
    example_rating_parse_fail = []

    # categorical frequency + normalization collisions
    raw_value_counts = {col: Counter() for col in CATEGORICAL_COLUMNS}
    norm_to_raws = {col: defaultdict(set) for col in CATEGORICAL_COLUMNS}

    numeric_checks = {col: NumericChecks() for col in NUMERIC_COLUMNS}

    total_amount_mismatch = 0
    total_amount_checked = 0

    date_invalid = 0
    date_min = None
    date_max = None

    dtype = {col: "string" for col in (CATEGORICAL_COLUMNS + NUMERIC_COLUMNS)}
    # Keep these as strings too so we can catch formatting problems.
    for col in ["transaction_id", "customer_id", "customer_name", "email", "phone", "postal_code", "product_name", "product_code", "order_date", "is_returning_customer", "sales_rep_id"]:
        dtype[col] = "string"

    reader = pd.read_csv(
        path,
        dtype=dtype,
        chunksize=args.chunksize,
        encoding="utf-8",
        encoding_errors="replace",
        low_memory=False,
    )

    for chunk_index, df in enumerate(reader, start=1):
        if args.max_rows and total_rows >= args.max_rows:
            break

        if args.max_rows:
            remaining = args.max_rows - total_rows
            if remaining <= 0:
                break
            if len(df) > remaining:
                df = df.iloc[:remaining].copy()

        total_rows += len(df)

        # missing counts (avoid double-counting <NA> in both isna() and stripped == "")
        for col in df.columns:
            series = df[col]
            if series.dtype.name.startswith("string"):
                miss_mask = series.isna() | (series.fillna("").str.strip() == "")
                miss = miss_mask.sum()
            else:
                miss = series.isna().sum()
            if miss:
                missing_counts[col] += int(miss)

        # whitespace/newline issues on text-like columns
        text_cols = [c for c in df.columns if df[c].dtype.name.startswith("string")]
        for col in text_cols:
            s = df[col].fillna("")
            whitespace_issues[col] += int((s != s.str.strip()).sum())
            newline_issues[col] += int(s.str.contains(r"[\r\n]", regex=True).sum())

        # pattern checks
        email_s = df.get("email")
        if email_s is not None:
            s = email_s.fillna("").astype(str).str.strip()
            non_empty = s != ""
            non_ascii_mask = non_empty & (~s.map(is_ascii))
            email_non_ascii += int(non_ascii_mask.sum())

            structural_bad_mask = non_empty & (~non_ascii_mask) & (~s.str.match(EMAIL_RE))
            email_structurally_invalid += int(structural_bad_mask.sum())

            bad_mask = non_ascii_mask | structural_bad_mask
            invalid_email += int(bad_mask.sum())
            if len(example_invalid_emails) < 10 and bad_mask.any():
                for v in s[bad_mask].head(10 - len(example_invalid_emails)).tolist():
                    example_invalid_emails.append(v)

        phone_s = df.get("phone")
        if phone_s is not None:
            s = phone_s.fillna("").astype(str).str.strip()
            digits = s.str.replace(r"\D", "", regex=True)
            invalid_phone += int(((s != "") & (digits.str.len() < 7)).sum())

        txn_s = df.get("transaction_id")
        if txn_s is not None:
            s = txn_s.fillna("").astype(str).str.strip()
            invalid_txn_id += int(((s != "") & (~s.str.match(TXN_RE))).sum())

        cust_s = df.get("customer_id")
        if cust_s is not None:
            s = cust_s.fillna("").astype(str).str.strip()
            invalid_customer_id += int(((s != "") & (~s.str.match(CUST_RE))).sum())

        prod_s = df.get("product_code")
        if prod_s is not None:
            s = prod_s.fillna("").astype(str).str.strip()
            invalid_product_code += int(((s != "") & (~s.str.match(PROD_CODE_RE))).sum())

        # categorical stats
        for col in CATEGORICAL_COLUMNS:
            if col not in df.columns:
                continue
            s = df[col].fillna("").astype(str)
            raw_value_counts[col].update(s.value_counts(dropna=False).to_dict())
            for v in s.unique():
                if not v or str(v).strip() == "":
                    continue
                norm_to_raws[col][norm_text(str(v))].add(str(v))

        # numeric checks + total_amount consistency
        numeric_values = {}
        for col in NUMERIC_COLUMNS:
            if col not in df.columns:
                continue
            s = df[col]
            parsed = s.map(safe_float)

            numeric_checks[col].missing += int(parsed.isna().sum())
            numeric_checks[col].parse_fail += int((parsed == "__PARSE_FAIL__").sum())

            ok = parsed[(parsed != "__PARSE_FAIL__") & (~parsed.isna())].astype(float)
            numeric_values[col] = ok

            if col in {"quantity", "unit_price", "total_amount", "loyalty_points"}:
                numeric_checks[col].negative += int((ok < 0).sum())

        # range checks
        if "discount_percent" in numeric_values:
            ok = numeric_values["discount_percent"]
            numeric_checks["discount_percent"].out_of_range += int(((ok < 0) | (ok > 100)).sum())
        if "tax_rate" in numeric_values:
            ok = numeric_values["tax_rate"]
            numeric_checks["tax_rate"].out_of_range += int(((ok < 0) | (ok > 100)).sum())
        if "rating" in numeric_values:
            ok = numeric_values["rating"]
            numeric_checks["rating"].out_of_range += int(((ok < 0) | (ok > 5)).sum())

        needed = {"quantity", "unit_price", "discount_percent", "tax_rate", "total_amount"}
        if needed.issubset(df.columns):
            q = df["quantity"].map(safe_float)
            up = df["unit_price"].map(safe_float)
            disc = df["discount_percent"].map(safe_float)
            tax = df["tax_rate"].map(safe_float)
            tot = df["total_amount"].map(safe_float)

            ok_mask = (
                (q != "__PARSE_FAIL__")
                & (up != "__PARSE_FAIL__")
                & (disc != "__PARSE_FAIL__")
                & (tax != "__PARSE_FAIL__")
                & (tot != "__PARSE_FAIL__")
                & (~q.isna())
                & (~up.isna())
                & (~disc.isna())
                & (~tax.isna())
                & (~tot.isna())
            )

            if ok_mask.any():
                qv = q[ok_mask].astype(float)
                upv = up[ok_mask].astype(float)
                dv = disc[ok_mask].astype(float)
                tv = tax[ok_mask].astype(float)
                tov = tot[ok_mask].astype(float)

                expected = qv * upv * (1 - (dv / 100.0)) * (1 + (tv / 100.0))
                diff = (tov - expected).abs()

                # tolerate small rounding
                bad = diff > 0.05
                total_amount_mismatch += int(bad.sum())
                total_amount_checked += int(ok_mask.sum())

        # date checks
        if "order_date" in df.columns:
            s = df["order_date"].fillna("").astype(str).str.strip()
            parsed = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")
            date_invalid += int(((s != "") & (parsed.isna())).sum())
            if parsed.notna().any():
                pmin = parsed.min()
                pmax = parsed.max()
                date_min = pmin.date() if date_min is None else min(date_min, pmin.date())
                date_max = pmax.date() if date_max is None else max(date_max, pmax.date())

        if "rating" in df.columns:
            s = df["rating"].fillna("").astype(str).str.strip()
            if len(example_rating_parse_fail) < 10:
                bad = (s != "") & (~s.str.match(r"^-?\d+(\.\d+)?$"))
                if bad.any():
                    for v in s[bad].head(10 - len(example_rating_parse_fail)).tolist():
                        example_rating_parse_fail.append(v)

        if chunk_index % 5 == 0:
            print(f"processed {total_rows:,} rows...")

    # Build report
    print("\n=== DATA QUALITY SUMMARY ===")
    print(f"Rows analyzed: {total_rows:,}")

    print("\n-- Missing values (top 15 columns) --")
    for col, cnt in missing_counts.most_common(15):
        pct = (cnt / total_rows) * 100 if total_rows else 0
        print(f"{col}: {cnt:,} ({pct:.2f}%)")

    print("\n-- Text formatting issues (top 10 columns) --")
    worst_ws = whitespace_issues.most_common(10)
    for col, cnt in worst_ws:
        pct = (cnt / total_rows) * 100 if total_rows else 0
        print(f"{col}: leading/trailing whitespace in {cnt:,} rows ({pct:.2f}%)")

    worst_nl = newline_issues.most_common(10)
    for col, cnt in worst_nl:
        pct = (cnt / total_rows) * 100 if total_rows else 0
        print(f"{col}: embedded newline in {cnt:,} rows ({pct:.2f}%)")

    print("\n-- Pattern validity counts --")
    print(f"invalid transaction_id (expected TXN##########): {invalid_txn_id:,}")
    print(f"invalid customer_id (expected CUST#####): {invalid_customer_id:,}")
    print(f"invalid product_code (expected 8 chars A-Z0-9): {invalid_product_code:,}")
    print(f"invalid email: {invalid_email:,}")
    print(f"  - non-ASCII emails: {email_non_ascii:,}")
    print(f"  - structurally invalid ASCII emails: {email_structurally_invalid:,}")
    print(f"invalid phone (<7 digits): {invalid_phone:,}")
    if example_invalid_emails:
        print("example invalid emails:")
        for v in example_invalid_emails[:10]:
            print(f"  - {v!r}")

    print("\n-- Numeric parsing / range issues --")
    for col in NUMERIC_COLUMNS:
        chk = numeric_checks[col]
        if chk.missing or chk.parse_fail or chk.negative or chk.out_of_range:
            print(
                f"{col}: missing={chk.missing:,} parse_fail={chk.parse_fail:,} "
                f"negative={chk.negative:,} out_of_range={chk.out_of_range:,}"
            )

    if total_amount_checked:
        pct = (total_amount_mismatch / total_amount_checked) * 100
        print("\n-- total_amount consistency --")
        print(
            f"Checked {total_amount_checked:,} rows with all required numeric fields; "
            f"mismatches (> $0.05): {total_amount_mismatch:,} ({pct:.2f}%)"
        )

    print("\n-- order_date parsing --")
    print(f"invalid order_date strings: {date_invalid:,}")
    if date_min and date_max:
        print(f"date range: {date_min.isoformat()} to {date_max.isoformat()}")

    if example_rating_parse_fail:
        print("example non-numeric rating values:")
        for v in example_rating_parse_fail[:10]:
            print(f"  - {v!r}")

    print("\n-- Categorical inconsistencies (normalization collisions) --")
    for col in CATEGORICAL_COLUMNS:
        collisions = [
            (norm, raws)
            for norm, raws in norm_to_raws[col].items()
            if len(raws) > 1
        ]
        collisions.sort(key=lambda x: len(x[1]), reverse=True)
        if not collisions:
            continue
        print(f"{col}: {len(collisions):,} normalized values map to multiple raw spellings")
        for norm, raws in collisions[:10]:
            raws_list = sorted(list(raws))
            print(f"  - {norm!r} -> {raws_list[:8]}{' ...' if len(raws_list) > 8 else ''}")

    print("\n-- Top categorical values (raw, top-k) --")
    for col in CATEGORICAL_COLUMNS:
        print(f"\n{col}:")
        for v, cnt in raw_value_counts[col].most_common(args.top_k):
            display = v
            if isinstance(display, str) and len(display) > 60:
                display = display[:57] + "..."
            print(f"  {display!r}: {cnt:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
