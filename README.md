Case Study – End-to-End Analytics Pipeline

This project implements an end-to-end data analytics pipeline starting from a raw transactional dataset and ending with a role-based analytics dashboard.
The focus of the case study is data engineering, data quality, ETL design, and analytical access, rather than UI complexity.

---

1. Project Scope and Goals

The goals of this project are:

- Analyze and document data quality issues in a large raw dataset
- Design and implement a robust ETL pipeline
- Build an analytics-ready warehouse
- Expose analytical results via a backend API
- Enforce role-based access control
- Provide a minimal frontend dashboard to demonstrate usage

The project intentionally avoids over-engineering (such as heavy frontend frameworks) to keep the focus on data and analytics concerns.

---

2. Dataset Overview

Source file: large_dataset.csv
Size: approximately 1 GB
Rows: approximately 5,000,000
Format: CSV (UTF-8)

The raw dataset is not committed to the repository due to size constraints.
All processing assumes the dataset is available locally at:

data/raw/large_dataset.csv

---

3. Data Quality Analysis

A dedicated data profiling script was written to analyze the raw dataset in chunks.

Key findings (documented in DATA_QUALITY.md):

- Inconsistent categorical values caused by typos (for example: Germany vs Germeny)
- Non-ASCII characters in email addresses
- Missing values in key fields such as rating and region_code
- Numeric fields largely consistent, with validated totals
- No duplicate transaction IDs

These findings directly informed the ETL rules.

---

4. ETL Design

Extract:

- The CSV file is read in chunks to support large file sizes
- All columns are initially loaded as strings to preserve raw values and detect formatting issues

Transform:

- Canonical mapping tables are applied to normalize categorical values
- Numeric fields are validated for non-negativity and valid ranges
- Dates are parsed using ISO format (YYYY-MM-DD)
- Rating values are preserved as NULL when missing
- Missing region_code values are filled with the value UNKNOWN
- Invalid records are excluded from analytics rather than force-corrected

Load:

- Cleaned and validated data is loaded into an analytics warehouse
- The final fact table is named fact_transactions

---

5. Analytics Warehouse

Technology used: DuckDB

Purpose:

- Execute analytical queries such as aggregations, trends, and metrics

Reasoning:

- Columnar execution model
- Zero-configuration setup
- High performance for analytical workloads

DuckDB is used only for analytics and not for user management or authentication.

---

6. User Management and Roles

User and role management is handled using PostgreSQL.

Tables:

- users
- tenants

Roles:

- Admin: full access to analytics and user management endpoints
- User: access to aggregated analytics only
- Guest: access to limited public metrics

Multi-tenancy:

- Users may optionally belong to a tenant
- Tenant support is enforced at the API layer

---

7. Backend API

Framework: FastAPI

Responsibilities:

- Serve analytical queries from DuckDB
- Enforce role-based access control using PostgreSQL
- Act as the single integration point for frontend access

Example endpoints:

- /metrics/revenue-by-country
- /metrics/daily-revenue
- /admin/users (admin-only)

Authentication is simulated using an X-User HTTP header for simplicity.

---

8. Frontend Dashboard

The frontend is implemented using plain HTML, CSS, and JavaScript.

Features:

- User login simulation
- Role badge display (Admin, User, Guest)
- Revenue by country visualization (bar chart)
- Daily revenue trend visualization (line chart)
- Admin-only user list
- Clear visualization of role restrictions

Charts are rendered using Chart.js via CDN.

The frontend is intentionally minimal and exists solely to demonstrate backend integration, role enforcement, and analytical outputs.

---

9. Running the Project

Requirements:

- Docker
- Docker Compose

The project is fully containerized. No local Python environment setup is required.

---

Setup Steps:

1. Build and start all services:

   docker compose up --build

2. Verify that the services are running:
   - Backend API (FastAPI + Swagger):
     http://localhost:8000/docs

   - Frontend dashboard:
     http://localhost:8080

---

Authentication:

Requests are authenticated using the X-User HTTP header.
The following users are pre-seeded in the PostgreSQL database:

- admin_user (role: admin)
- normal_user_a (role: user)
- normal_user_b (role: user)
- guest_user (role: guest)

Example API request:

curl -H "X-User: admin_user" http://localhost:8000/metrics/revenue-by-country

---

10. Design Decisions and Trade-offs

- DuckDB was chosen for analytics due to simplicity and performance
- PostgreSQL was used for transactional concerns such as users, roles, and tenants
- The frontend was intentionally kept minimal to avoid distracting from data engineering goals
- Invalid data is excluded rather than force-corrected to preserve data integrity

---

11. What This Project Demonstrates

- Strong understanding of data quality and ETL design
- Clear separation between analytics and transactional systems
- Practical implementation of role-based access control
- Ability to design an end-to-end data platform
- Professional scope control and architectural reasoning

---

12. Out of Scope

- Production-grade authentication mechanisms (OAuth, JWT)
- Complex frontend frameworks
- Real-time or streaming ingestion
- Automated orchestration tools such as Airflow

These items were intentionally excluded to keep the project focused and concise.

---

API Documentation

The backend API exposes interactive documentation via Swagger UI.

Once the backend is running, API documentation is available at:
http://127.0.0.1:8000/docs

Architecture Diagram can be found at docs/architecture.png
