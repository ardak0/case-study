# ETL Rules

This document defines the data cleaning, validation, and transformation
rules applied during the ETL process.

All rules are derived directly from the findings documented in
`DATA_QUALITY.md`. The purpose of these rules is to convert the raw
transactional dataset into a consistent, analytics-ready format while
preserving auditability.

---

## 1. Categorical Normalization

### Columns

- country
- city
- department
- category
- payment_method
- status
- tier
- region_code

### Rules

1. Trim leading and trailing whitespace.
2. Apply case-insensitive comparison.
3. Map known misspellings and variants to a canonical value using
   lookup tables.

### Examples

- `Turkye`, `Türkiye` → `Turkey`
- `Operatons` → `Operations`
- `Wire Tranfer` → `Wire Transfer`

### Rationale

Unnormalized categorical values fragment aggregations and prevent
reliable joins to dimension tables.

---

## 2. Email Validation

### Column

- email

### Rules

1. Email addresses must be ASCII-only.
2. Email structure must match a standard validation regex.
3. Rows with invalid email values are rejected.

### Assumption

Downstream systems are assumed to require ASCII-only email addresses
for compatibility.

---

## 3. Missing Values

### rating

- Missing values are preserved as NULL.
- No default imputation is applied.

### region_code

- Missing values are replaced with `UNKNOWN`.

### Rationale

Preserving NULL values avoids introducing artificial bias into
analytical results.

---

## 4. Numeric Fields

### Columns

- quantity
- unit_price
- total_amount
- loyalty_points

### Rules

- Values must be numeric.
- Values must be greater than or equal to zero.
- Rows violating these rules are rejected.

---

## 5. Percentage Fields

### Columns

- discount_percent
- tax_rate

### Rules

- Valid range: [0, 100].
- Values outside this range cause the row to be rejected.

---

## 6. Date Handling

### Column

- order_date

### Rules

1. Dates must be parsable in ISO format (YYYY-MM-DD).
2. Rows with invalid or unparsable dates are rejected.

---

## 7. Semantic Validation

### total_amount Consistency

The following condition must hold:

total_amount ≈ quantity × unit_price × (1 − discount_percent / 100)
× (1 + tax_rate / 100)

- A tolerance of ±0.05 is allowed for rounding.
- Rows failing this validation are rejected.

### Rationale

Ensures financial consistency and prevents corrupted revenue metrics.

---

## 8. Rejection & Audit Policy

- Any row violating one or more rules is rejected.
- Rejected rows are written to a separate dataset with a rejection
  reason code.
- Raw source data is never modified in place.

---

## 9. Determinism & Reproducibility

- ETL rules are deterministic.
- Re-running the ETL on the same input produces identical outputs.
- All transformations are traceable and explainable.
