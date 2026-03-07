# Read/Write Parquet, SAS, SPSS, and CSV files in Stata

`pq` is a Stata package for high-performance file IO across Parquet, SAS, SPSS, and CSV formats. Built on [Polars](https://github.com/pola-rs/polars). Requires Stata 16+.

## Installation

**From SSC:**
```stata
ssc install pq
```

**Manual:** download the package for your platform from the [latest release](https://github.com/jrothbaum/stata_parquet_io/releases) and place the files in your `PLUS/p` directory (`sysdir` shows the path).

**Mac ARM users:** if you see a Gatekeeper error on first use, go to **System Preferences → Privacy & Security**, find the blocked `.dylib`, and click **Allow Anyway**.

## Quick Start

```stata
* Parquet
pq use mydata.parquet, clear
pq save mydata.parquet, replace

* Format shortcuts (set format automatically)
pq use_sas   source.sas7bdat, clear
pq use_spss  source.sav, clear
pq use_csv   source.csv, clear
pq save_spss out.sav, replace
pq save_csv  out.csv, replace
```

pq use/append/merge/save commands also accept `format(parquet|sas|spss|csv)` as an option.

## Key Options

**Reading** (`use`, `append`, `merge`, and format shortcuts):

| Option | Description |
|--------|-------------|
| `if(expr)` | SQL predicate pushdown — filters at read time |
| `in(range)` | Row range, e.g. `in(1/1000)` |
| varlist | Load only selected columns, e.g. `pq use id age using data.parquet` |
| `compress` | Downcast numerics to smallest lossless type |
| `sort(varlist)` | Sort on load; prefix `-` for descending |
| `drop(varlist)` | Exclude columns by name or pattern |
| `preserve_order` | Maintain source row order (SAS/SPSS) |
| `parse_dates` | Auto-detect and convert date strings (CSV) |
| `relaxed` | Union files with mismatched schemas (Parquet) |
| `asterisk_to_variable(name)` | Extract wildcard match into a variable (Parquet) |

**Saving** (`save`, `save_spss`, `save_csv`):

| Option | Description |
|--------|-------------|
| `replace` | Overwrite existing file |
| `if(expr)` | Save a filtered subset with stata if syntax |
| `partition_by(varlist)` | Hive-partitioned output directory (Parquet) |
| `compression(type)` | `zstd` (default), `snappy`, `gzip`, etc. (Parquet) |

For the full option reference, run `help pq` after installing.

## Examples

```stata
* Load selected columns with a filter
pq use id year earnings using cps.parquet, clear if(year >= 2010 & !missing(earnings))

* Load multiple files; extract year from filename
pq use /data/cps_*.parquet, clear asterisk_to_variable(year)

* Combine Parquet files with slightly different schemas
pq use /data/*.parquet, clear relaxed

* Append a second file, compressing on load
pq append extra.parquet, compress

* SAS read preserving source order
pq use_sas survey.sas7bdat, clear preserve_order

* CSV read with date parsing
pq use_csv raw.csv, clear parse_dates

* Save partitioned by state and year
pq save /output/data, replace partition_by(state year)
```

## Data Types

| Source type | Stata type | Notes |
|-------------|------------|-------|
| String | `str#` / `strL` | Auto-sized; >2045 chars → strL |
| Integer | `byte`/`int`/`long` | Sized by range |
| Float/Double | `float`/`double` | Preserves precision |
| Boolean | `byte` | 0/1 |
| Date | `long` (%td) | |
| DateTime | `double` (%tc) | |
| Time | `double` (%tchh:mm:ss) | |
| Binary | *dropped* | Not supported by Stata plugin API |

## Limitations

- **Binary columns** are silently dropped.
- **strL reads** are slower than str# due to Stata plugin constraints.
- **`if()` uses SQL semantics**: missing values are not treated as greater than any value (unlike Stata's native `if`).
- **CSV date filters**: use ISO literals (`DATE '2020-01-05'`, `TIMESTAMP '2020-01-05 00:00:00'`) rather than Stata's `td()`/`tc()` functions in `if()`.

## Performance

Benchmarks run on AMD Ryzen 7 8845HS (16 cores), 14 GB RAM, Windows 11, Stata 17 SE.

**Parquet vs `.dta`** — 1,000,000 rows:

| | Stata `.dta` | `pq` (10 cols) | `pq` (5 of 1,000 cols) |
|---|---|---|---|
| Full read | 0.02s | 0.22s | — |
| Subset columns | 0.04s | 0.13s | **0.04s** |

Parquet full reads are slower than `.dta` (Stata's native format is highly optimized). The value is **roundtripping with Python, R, and Spark** — Stata has no native Parquet writer (with a newly available reader in Stata Now). Column selection on very wide Parquet files also matches or beats `.dta`.


**CSV** — 100,000 rows × 10 variables, average of 3 runs:

| Operation | `pq` | Stata native | Speedup |
|-----------|------|--------------|---------|
| Write | 0.035s | 0.384s (`export delimited`) | **11×** |
| Read — all columns | 0.100s | 0.714s (`import delimited`) | **7×** |
| Read — 4 of 10 columns | 0.073s | 0.362s (`import delimited` + `keep`) | **5×** |

`import delimited` does not support column projection (outside of contiguous columns by index with colrange); the Stata "subset" time is a full load followed by `keep`. `pq` skips parsing of unused fields in the lazy CSV scan.

**SAS** — 88,932 rows, average of 5 runs:

| Operation | `pq` | `import sas` | Speedup |
|-----------|------|--------------|---------|
| Full read | 0.71s | 3.50s | **5×** |
| Subset columns | 0.27s | 0.14s | — |


**SPSS** — GSS 2024 survey (3,309 rows × 813 variables), average of 5 runs:

| Operation | `pq` | Stata native | Notes |
|-----------|------|--------------|-------|
| Read — all columns | 0.68s | 1.61s (`import spss`) | **2.4× faster** |
| Write — all columns | 0.43s | — | **No Stata equivalent** |

Stata can read SPSS files but has no `export spss` command. `pq save_spss` enables full roundtripping.

