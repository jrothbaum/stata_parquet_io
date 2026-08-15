# pq Plugin Benchmark Results

Generated with polars-readstat-rs 0.9.4. All times in seconds (avg per rep).

---

## Parquet vs Stata `.dta`

10 variables (1 int, 1 str, 8 float). Single rep per cell.

| rows | Stata save | pq save | Stata use | pq use | pq use (5 vars) |
|-----:|----------:|--------:|----------:|-------:|----------------:|
| 1,000 | 0.00 | 0.02 | 0.00 | 0.03 | 0.01 |
| 10,000 | 0.00 | 0.01 | 0.00 | 0.01 | 0.01 |
| 100,000 | 0.01 | 0.04 | 0.01 | 0.03 | 0.02 |
| 1,000,000 | 0.03 | 0.29 | 0.02 | 0.07 | 0.05 |
| 10,000,000 | 0.26 | 2.82 | 0.15 | 0.61 | 0.40 |
| 1,000,000 × 100 cols | 0.20 | 2.27 | 0.13 | 0.37 | 0.06 |
| 100,000 × 1,000 cols | 0.19 | 2.47 | 0.12 | 0.60 | 0.03 |

> Parquet trades read/write speed for portability and column projection (5-var subset vs full read).
> `pq use` is now within ~4× of native `.dta` at 10M rows (down from ~15×), and 4-7× faster than before on wide files (100-1,000 cols).

---

## CSV: `pq use_csv` / `pq save_csv` vs native Stata

1,000,000 rows, 10 variables, 3 reps.

| operation | pq (s) | native (s) | pq speedup |
|-----------|-------:|-----------:|-----------:|
| write | 0.2960 | 3.6590 (`export delimited`) | **12.4×** |
| read — full file | 1.4567 | 3.2693 (`import delimited`) | **2.2×** |
| read — 5-var subset | 1.3070 | 3.0097 (`import delimited`) | **2.3×** |

> `import delimited` has no column projection; the subset comparison reads all columns then drops.
> CSV read speed is essentially unchanged from before.

---

## SPSS: `pq use_spss` vs native `import spss`

pq-generated `.sav` files, 10 variables (int/float/str mix), 5 reps. Subset: 4 vars (`id grp x1 s1`).

| rows | pq full (s) | native full (s) | pq speedup | pq sub (s) | native sub (s) | sub speedup |
|-----:|------------:|----------------:|-----------:|-----------:|---------------:|------------:|
| 100,000 | 0.0484 | 0.4264 | **8.8×** | 0.0406 | 0.3846 | **9.5×** |
| 500,000 | 0.0900 | 2.0524 | **22.8×** | 0.0710 | 1.8262 | **25.7×** |
| 1,000,000 | 0.1452 | 4.0696 | **28.0×** | 0.1150 | 3.6776 | **32.0×** |

> pq's own full-read time at 1M rows is down from 0.2604s to 0.1452s, about 1.8× faster than before.

---

## SAS: `pq use_sas` vs native `import sas`

88,932 rows (hhpub25.sas7bdat), 5 reps. Subset: 5 vars (`H_IDNUM GEREG GESTFIPS GEDIV HRHTYPE`).

| operation | pq (s) | native (s) | pq speedup |
|-----------|-------:|-----------:|-----------:|
| read — full file (157 vars) | 0.1468 | 3.8346 (`import sas`) | **26.1×** |
| read — 5-var subset | 0.0934 | 0.1758 (`import sas`) | **1.9×** |

> Full read (157 vars) is now 3.4× faster than before (0.4928s → 0.1468s); the 5-var subset is about the same.
