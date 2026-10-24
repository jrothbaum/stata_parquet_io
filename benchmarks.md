# pq Plugin Benchmark Results

Generated with stata_parquet_io 4.0.4 (polars-readstat-rs 0.26.0). All times in seconds (avg per rep; each benchmark run 3 times in sequence and averaged).

---

## Parquet vs Stata `.dta`

Average of 3 runs, single rep per run. Mix of strings, ints, and floats.

| rows | Stata save | pq save | Stata use | pq use | pq use (5 vars) |
|-----:|----------:|--------:|----------:|-------:|----------------:|
| 1,000 | 0.00 | 0.03 | 0.00 | 0.02 | 0.01 |
| 10,000 | 0.00 | 0.01 | 0.00 | 0.02 | 0.01 |
| 100,000 | 0.00 | 0.04 | 0.01 | 0.03 | 0.01 |
| 1,000,000 | 0.03 | 0.26 | 0.02 | 0.06 | 0.04 |
| 10,000,000 | 0.19 | 2.01 | 0.12 | 0.32 | 0.24 |
| 1,000,000 × 100 cols | 0.18 | 1.62 | 0.11 | 0.28 | 0.05 |
| 100,000 × 1,000 cols | 0.17 | 1.76 | 0.11 | 0.48 | 0.01 |
| 10,000,000 × 100 cols | 1.92 | 17.01 | 3.65 | 4.10 | 0.43 |

> Parquet trades read/write speed for portability and column projection (5-var subset vs full read).
> `pq use` is now within ~3× of native `.dta` at 10M rows (down from ~15×); about on par at 10M × 100 cols (1.1×); and 4-7× faster than before on wide files (100-1,000 cols).

---

## CSV: `pq use_csv` / `pq save_csv` vs native Stata

1,000,000 rows, 10 variables, 3 reps per run, average of 3 runs.

| operation | pq (s) | native (s) | pq speedup |
|-----------|-------:|-----------:|-----------:|
| write | 0.2451 | 2.8248 (`export delimited`) | **11.5×** |
| read — full file | 1.3448 | 2.6452 (`import delimited`) | **2.0×** |
| read — 5-var subset | 1.2089 | 2.4353 (`import delimited`) | **2.0×** |

> `import delimited` has no column projection; the subset comparison reads all columns then drops.
> CSV read speed is essentially unchanged from before.

---

## SPSS: `pq use_spss` vs native `import spss`

pq-generated `.sav` files, 10 variables (int/float/str mix), 5 reps per run, average of 3 runs. Subset: 4 vars (`id grp x1 s1`).

| rows | pq full (s) | native full (s) | pq speedup | pq sub (s) | native sub (s) | sub speedup |
|-----:|------------:|----------------:|-----------:|-----------:|---------------:|------------:|
| 100,000 | 0.0293 | 0.3553 | **12.1×** | 0.0141 | 0.3033 | **21.5×** |
| 500,000 | 0.0611 | 1.5941 | **26.1×** | 0.0326 | 1.4237 | **43.7×** |
| 1,000,000 | 0.1041 | 3.2519 | **31.2×** | 0.0553 | 2.8723 | **51.9×** |

> pq's own full-read time at 1M rows is down from 0.2604s to 0.1041s, about 2.5× faster than before.

---

## SAS: `pq use_sas` vs native `import sas`

88,932 rows (hhpub25.sas7bdat), 5 reps per run, average of 3 runs. Subset: 5 vars (`H_IDNUM GEREG GESTFIPS GEDIV HRHTYPE`).

| operation | pq (s) | native (s) | pq speedup |
|-----------|-------:|-----------:|-----------:|
| read — full file (157 vars) | 0.1447 | 3.4375 (`import sas`) | **23.8×** |
| read — 5-var subset | 0.0327 | 0.1402 (`import sas`) | **4.3×** |

> Full read (157 vars) is now 3.4× faster than before (0.4928s → 0.1447s); the 5-var subset is about 2.9× faster than before (0.0934s → 0.0327s).
