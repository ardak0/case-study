import pandas as pd
import argparse
import os


def main():
    ap = argparse.ArgumentParser(description="Post-ETL data quality metrics")
    ap.add_argument("--raw", default="data/raw/large_dataset.csv")
    ap.add_argument("--clean", default="data/clean/clean_transactions.csv")
    ap.add_argument("--reject", default="data/reject/rejected_transactions.csv")
    args = ap.parse_args()

    print("Loading datasets...")

    raw_rows = sum(1 for _ in open(args.raw)) - 1 if os.path.exists(args.raw) else None
    clean_df = pd.read_csv(args.clean)
    reject_df = pd.read_csv(args.reject)

    clean_rows = len(clean_df)
    reject_rows = len(reject_df)
    total_processed = clean_rows + reject_rows

    print("\n=== DATA QUALITY METRICS ===")
    if raw_rows is not None:
        print(f"Raw rows:        {raw_rows:,}")
    print(f"Processed rows:  {total_processed:,}")
    print(f"Clean rows:      {clean_rows:,}")
    print(f"Rejected rows:   {reject_rows:,}")

    if total_processed > 0:
        reject_rate = (reject_rows / total_processed) * 100
        print(f"Reject rate:     {reject_rate:.2f}%")

    if "reject_reason" in reject_df.columns:
        print("\nTop rejection reasons:")
        reasons = (
            reject_df["reject_reason"]
            .str.split(";")
            .explode()
            .value_counts()
            .head(10)
        )
        for reason, cnt in reasons.items():
            print(f"  {reason}: {cnt:,}")

    print("\nData quality metrics computed successfully.")


if __name__ == "__main__":
    main()
