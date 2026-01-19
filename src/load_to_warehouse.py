import duckdb
import pandas as pd
import argparse
import os


def main():
    ap = argparse.ArgumentParser(description="Load clean data into DuckDB warehouse")
    ap.add_argument("--input", default="data/clean/clean_transactions.csv")
    ap.add_argument("--db", default="warehouse.duckdb")
    ap.add_argument("--table", default="fact_transactions")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"Clean dataset not found: {args.input}")

    print("Connecting to DuckDB warehouse...")
    con = duckdb.connect(args.db)

    print("Loading clean dataset...")
    df = pd.read_csv(args.input)

    print("Creating fact table...")
    con.execute(f"DROP TABLE IF EXISTS {args.table}")
    con.execute(f"""
        CREATE TABLE {args.table} AS
        SELECT *
        FROM df
    """)

    print("Warehouse load completed")
    print(f"Database: {args.db}")
    print(f"Table: {args.table}")
    print(f"Rows loaded: {len(df):,}")

    con.close()


if __name__ == "__main__":
    main()
