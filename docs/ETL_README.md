# ETL Overview

This document describes the ETL (Extract, Transform, Load) process used
to transform the raw transactional dataset into an analytics-ready
format.

The ETL logic is derived directly from the findings documented in
`DATA_QUALITY.md` and the rules defined in `ETL_RULES.md`.

---

## Input

- Source file: `data/raw/large_dataset.csv`
- Size: ~1 GB
- Rows: ~5,000,000
- Format: CSV (UTF-8)

---

## Extract

- Data is read in chunks to avoid excessive memory usage.
- All columns are initially loaded as strings to preserve raw values
  and detect formatting issues.

---

## Transform

The following transformations are applied:

### Categorical Normalization

- Standardize spelling, casing, and whitespace.
- Map known misspellings to canonical values using lookup tables.

### Validation Rules

- Email addresses must be ASCII-only and structurally valid.
- Numeric fields must be non-negative.
- Percentage fields must be in the range [0, 100].
- Dates must be parsable in ISO format (YYYY-MM-DD).

### Missing Values

- `rating`: preserved as NULL.
- `region_code`: filled with `UNKNOWN` when missing.

### Semantic Checks

- `total_amount` is validated against:
  quantity × unit_price × (1 − discount_percent / 100) × (1 + tax_rate / 100)

- A tolerance of ±0.05 is allowed for rounding.
- Rows failing this check are rejected.

---

## Load

- Valid records are written to a clean dataset for analytical use.
- Invalid records are written to a separate reject dataset along with
  a rejection reason.
- Raw data is never modified in place.

---

## Outputs

- **Clean dataset**
- Analytics-ready
- Conforms to all ETL rules
- **Rejected dataset**
- Contains invalid rows
- Includes reason codes (e.g. `INVALID_EMAIL`, `NEGATIVE_VALUE`)

---

## Auditability & Reproducibility

- Each ETL run is deterministic and repeatable.
- All rejected records remain traceable to their original raw values.
- ETL logic can be re-run on the same input to produce identical results.
