import duckdb

con = duckdb.connect("warehouse.duckdb")

# 1. Row count
rows = con.execute(
    "SELECT COUNT(*) FROM fact_transactions"
).fetchone()[0]
print("Rows in fact_transactions:", rows)

# 2. Revenue by country
df = con.execute("""
    SELECT country, SUM(total_amount) AS revenue
    FROM fact_transactions
    GROUP BY country
    ORDER BY revenue DESC
""").fetchdf()

print("\nRevenue by country:")
print(df)

# 3. Daily revenue trend
df = con.execute("""
    SELECT order_date, SUM(total_amount) AS revenue
    FROM fact_transactions
    GROUP BY order_date
    ORDER BY order_date
    LIMIT 10
""").fetchdf()

print("\nDaily revenue sample:")
print(df)

con.close()
