DATA QUALITY OBSERVATIONS

Profiled `large_dataset.csv` (5,000,000 rows, 26 columns).

## High-signal issues

### 1) Inconsistent categorical values (typos / alternate spellings)

Several key categorical fields contain multiple spellings for what appear to be the same value. This will fragment grouping/aggregation and break joins to reference tables.

Examples (counts are raw occurrences):

- `country`: `Turkey` (416,562) vs `Turkye` (416,849) vs `Türkiye` (416,441); `Germany` (416,351) vs `Germeny` (416,580); `France` (417,166) vs `Frence` (416,071)
- `city`: `Paris` (417,053) vs `Pariss` (417,564); `London` (417,521) vs `Londoon` (416,124); `Istanbul` (415,777) vs `Istanbull` (417,132); `Berlin` (416,668) vs `Berlinn` (415,774)
- `department`: `Support` (357,123) vs `Suport` (358,233); `Operations` (356,988) vs `Operatons` (356,827); `Marketing` (356,730) vs `Marketng` (357,693); `Sales` (357,519) vs `Salles` (356,651); `Finance` (357,845) vs `Finnance` (356,583); `Legal` (357,355) vs `Leegal` (355,919)
- `category`: `Electronics` (499,686) vs `Electronnics` (500,219); `Hardware` (499,349) vs `Hardwer` (500,095); `Software` (500,025) vs `Softwrae` (501,800); `Furniture` (500,601) vs `Furnitur` (499,822); `Clothing` (499,656) vs `Clothng` (498,747)
- `payment_method`: `Wire Transfer` (455,169) vs `Wire Tranfer` (453,842); `PayPal` (454,918) vs `PayPall` (455,059); `Credit Card` (454,618) vs `Credt Card` (454,686); `Crypto` (455,017) vs `Cryto` (454,652); `Check` (454,829) vs `Chek` (453,616)
- `status`: `Pending` (499,821) vs `Pendng` (500,325); `Processing` (499,848) vs `Procesing` (499,959); `Approved` (499,880) vs `Aproved` (499,528); `Rejected` (500,157) vs `Rejectd` (499,436); `Completed` (500,812) vs `Completted` (500,234)
- `tier`: `Enterprise` (499,808) vs `Enterprize` (501,307); `Professional` (500,768) vs `Profesional` (500,241); `Standard` (499,826) vs `Standart` (499,626); `Premium` (499,586) vs `Premum` (499,162); `Basic` (499,117) vs `Basik` (500,559)

### 2) Email field contains non-ASCII characters

- `email`: 1,426,959 values contain non-ASCII characters (e.g., `ç, ö, ş, ı`).

Whether this is an “error” depends on the intended system. Many validation rules and legacy systems assume ASCII-only emails; if that is your case, these will fail.

### 3) Missing values in key fields

- `region_code`: 1,000,903 missing (20.02%)
- `rating`: 831,398 missing (16.63%)

## Checks that look good

- `transaction_id` format: 0 invalid (expected `TXN##########`)
- `customer_id` format: 0 invalid (expected `CUST#####`)
- `product_code` format: 0 invalid (expected 8 chars `A-Z0-9`)
- `order_date` parsing: 0 invalid, range 2021-01-01 to 2021-12-14
- `total_amount` math: 0 mismatches (within $0.05) against `quantity * unit_price * (1-discount%) * (1+tax%)`
- Duplicate `transaction_id`: 0 (via DuckDB `COUNT(*) - COUNT(DISTINCT transaction_id)`)

## Recommended cleanup / validation rules

1. Create a canonical mapping table per categorical column (e.g., map `Operatons` → `Operations`) and standardize during ingestion.
2. Decide whether `email` should support internationalized addresses (SMTPUTF8). If not:
   - either reject non-ASCII emails, or
   - transliterate to ASCII (lossy) + store original raw value separately.
3. Treat missing `rating` as NULL explicitly (do not coerce to 0).
4. For missing `region_code`, define a rule (e.g., infer from `country` if that mapping is available; otherwise set to `UNKNOWN`).
