# pq Plugin Benchmark Results

Generated with polars-readstat-rs 0.9.4. All times in seconds (avg per rep).

---

## Parquet vs Stata `.dta`

10 variables (1 int, 1 str, 8 float). Single rep per cell.

| rows | Stata save | pq save | Stata use | pq use | pq use (5 vars) |
|-----:|----------:|--------:|----------:|-------:|----------------:|
| 1,000 | 0.00 | 0.06 | 0.00 | 0.01 | 0.00 |
| 10,000 | 0.00 | 0.01 | 0.00 | 0.01 | 0.01 |
| 100,000 | 0.00 | 0.04 | 0.00 | 0.06 | 0.02 |
| 1,000,000 | 0.03 | 0.22 | 0.01 | 0.23 | 0.12 |
| 10,000,000 | 0.18 | 2.45 | 0.13 | 2.29 | 1.11 |
| 1,000,000 × 100 cols | 0.17 | 1.71 | 0.11 | 2.77 | 0.16 |
| 100,000 × 1,000 cols | 0.16 | 1.87 | 0.11 | 2.66 | 0.03 |

> Parquet trades read/write speed for portability and column projection (5-var subset vs full read).

---

## CSV: `pq use_csv` / `pq save_csv` vs native Stata

1,000,000 rows, 10 variables, 3 reps.

| operation | pq (s) | native (s) | pq speedup |
|-----------|-------:|-----------:|-----------:|
| write | 0.2353 | 2.7897 (`export delimited`) | **11.9×** |
| read — full file | 1.0227 | 2.6817 (`import delimited`) | **2.6×** |
| read — 5-var subset | 0.8127 | 2.4890 (`import delimited`) | **3.1×** |

> `import delimited` has no column projection; the subset comparison reads all columns then drops.

---

## SPSS: `pq use_spss` vs native `import spss`

pq-generated `.sav` files, 10 variables (int/float/str mix), 5 reps. Subset: 4 vars (`id grp x1 s1`).

| rows | pq full (s) | native full (s) | pq speedup | pq sub (s) | native sub (s) | sub speedup |
|-----:|------------:|----------------:|-----------:|-----------:|---------------:|------------:|
| 100,000 | 0.0590 | 0.3536 | **6.0×** | 0.0372 | 0.2970 | **8.0×** |
| 500,000 | 0.1514 | 1.6950 | **11.2×** | 0.0932 | 1.5480 | **16.6×** |
| 1,000,000 | 0.2604 | 3.2416 | **12.5×** | 0.1594 | 2.8810 | **18.1×** |

---

## SAS: `pq use_sas` vs native `import sas`

88,932 rows (hhpub25.sas7bdat), 5 reps. Subset: 5 vars (`H_IDNUM GEREG GESTFIPS GEDIV HRHTYPE`).

| operation | pq (s) | native (s) | pq speedup |
|-----------|-------:|-----------:|-----------:|
| read — full file | 0.4928 | 3.2608 (`import sas`) | **6.6×** |
| read — 5-var subset | 0.0856 | 0.1354 (`import sas`) | **1.6×** |
