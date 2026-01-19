# Data Warehouse Model

This document describes the analytical data warehouse model designed
for the transactional dataset after the ETL process.

The model follows a star-schema–oriented approach, optimized for
analytical queries, aggregations, and reporting use cases.

---

## Design Principles

- Separation of concerns between transactional data and analytics
- Read-optimized structure for reporting and aggregation
- Clear distinction between facts and dimensions
- Compatibility with columnar analytical databases (e.g. ClickHouse)

---

## Fact Table

### fact_transactions

The central fact table stores transactional events at the
transaction-level grain (one row per transaction).

#### Grain

- One row represents one completed transaction record after ETL
  validation.

#### Columns

- transaction_id (PK)
- customer_id (FK)
- product_code (FK)
- date_key (FK)
- country
- city
- department
- category
- payment_method
- status
- tier
- region_code
- quantity
- unit_price
- discount_percent
- tax_rate
- total_amount
- loyalty_points
- rating
- is_returning_customer
- sales_rep_id

#### Measures

- quantity
- unit_price
- total_amount
- loyalty_points
- discount_percent
- tax_rate
- rating

#### Rationale

All numeric measures required for revenue, volume, and performance
analysis are stored directly in the fact table to support fast
aggregation without additional joins.

---

## Dimension Tables

### dim_customer

Stores customer-related attributes.

#### Columns

- customer_id (PK)
- customer_name
- email
- tier
- region_code

#### Rationale

Separating customer attributes allows customer-level analysis
(e.g. segmentation by tier or region) without duplicating attributes
across fact records.

---

### dim_product

Stores product-related attributes.

#### Columns

- product_code (PK)
- product_name
- category
- department

#### Rationale

Supports product and category-level analytics while keeping the
fact table compact.

---

### dim_date

Stores calendar-related attributes derived from order_date.

#### Columns

- date_key (PK, YYYYMMDD)
- date
- day
- month
- quarter
- year
- day_of_week

#### Rationale

Date dimensions enable flexible time-based analysis (daily, monthly,
quarterly, yearly) without repeated date parsing.

---

### dim_location

Stores geographic attributes.

#### Columns

- country
- city
- region_code

#### Rationale

Allows geographic aggregations and simplifies location-based filtering
in analytical queries.

---

## Reject Dataset (Non-Warehouse)

Rejected records produced during ETL are intentionally excluded from
the data warehouse.

- Stored separately for audit and monitoring
- Contain rejection reason codes
- Used for data quality tracking and upstream remediation

---

## Analytical Use Cases

This model supports the following example queries:

- Total revenue by country, city, or region
- Revenue and volume trends over time
- Customer segmentation by tier and region
- Product and category performance analysis
- Impact of discounts and tax rates on revenue
- Data quality monitoring via reject rates

---

## Extensibility

- Additional dimensions (e.g. sales_rep, payment_provider) can be added
  without modifying existing fact data.
- The model is compatible with incremental loading strategies.
- Can be deployed on ClickHouse or similar analytical databases.

---

## Summary

The proposed warehouse model provides a clear separation between
validated transactional facts and descriptive dimensions, enabling
efficient, scalable, and reliable analytical reporting.
