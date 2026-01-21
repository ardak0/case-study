from fastapi import FastAPI, Header, HTTPException, Depends
import duckdb
import psycopg2
from fastapi.middleware.cors import CORSMiddleware


# --- DB CONFIG ---
DUCKDB_PATH = "warehouse.duckdb"

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "case_db",
    "user": "case_user",
    "password": "case_pass",
}

app = FastAPI(title="Analytics API with Roles")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # case study için yeterli
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],  # X-User için GEREKLİ
)



# --- Connections ---
def get_pg_conn():
    return psycopg2.connect(**PG_CONFIG)


def get_duck_conn():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


# --- Auth / Role ---
def get_current_user(x_user: str = Header(...)):
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT username, role, tenant_id FROM users WHERE username = %s",
        (x_user,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid user")

    return {
        "username": row[0],
        "role": row[1],
        "tenant_id": row[2],
    }


# --- Endpoints ---
@app.get("/metrics/revenue-by-country")
def revenue_by_country(user=Depends(get_current_user)):
    if user["role"] == "guest":
        raise HTTPException(status_code=403, detail="Guests not allowed")

    con = get_duck_conn()
    data = con.execute("""
        SELECT country, SUM(total_amount) AS revenue
        FROM fact_transactions
        GROUP BY country
        ORDER BY revenue DESC
    """).fetchall()
    con.close()

    return [{"country": r[0], "revenue": r[1]} for r in data]


@app.get("/metrics/daily-revenue")
def daily_revenue(user=Depends(get_current_user)):
    con = get_duck_conn()
    data = con.execute("""
        SELECT order_date, SUM(total_amount) AS revenue
        FROM fact_transactions
        GROUP BY order_date
        ORDER BY order_date
    """).fetchall()
    con.close()

    return [{"date": str(r[0]), "revenue": r[1]} for r in data]


@app.get("/admin/users")
def list_users(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT username, role, tenant_id FROM users")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {"username": r[0], "role": r[1], "tenant_id": r[2]}
        for r in rows
    ]
