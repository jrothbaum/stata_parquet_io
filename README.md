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
pq use  mydata.parquet,  clear
pq use  source.sas7bdat, clear
pq use  source.sav,      clear
pq use  source.csv,      clear

pq save mydata.parquet,  replace
pq save out.sav,         replace
pq save out.csv,         replace
```

Format is inferred from the file extension (`.sav`/`.zsav` → spss, `.csv` → csv, else → parquet).

## Key Options

**Reading:**

| Option | Description |
|--------|-------------|
| `if(expr)` | SQL predicate pushdown — filters rows at read time |
| `in(range)` | Row range, e.g. `in(1/1000)` |
| varlist | Load only selected columns: `pq use id age using data.parquet` |
| `compress` | Downcast numerics to smallest lossless type |
| `sort(varlist)` | Sort on load; prefix `-` for descending |
| `drop(varlist)` | Exclude columns by name or pattern |
| `cast(json)` | Cast columns to specified types, e.g. `cast({"col":"int32"})` |
| `lax` | With `cast()`, produce nulls instead of erroring on bad values |
| `parse_dates` | Auto-detect and convert date strings (CSV) |
| `preserve_order` | Maintain source row order (SAS/SPSS) |
| `relaxed` | Union files with mismatched schemas (Parquet) |

**Saving:**

| Option | Description |
|--------|-------------|
| `replace` | Overwrite existing file |
| `if(expr)` | Save a filtered subset using Stata if syntax |
| `partition_by(varlist)` | Hive-partitioned output directory (Parquet) |
| `compression(type)` | `zstd` (default), `snappy`, `gzip`, etc. (Parquet) |

Run `help pq` for the full reference.

## Examples

```stata
* Load selected columns with a filter
pq use id year earnings using cps.parquet, clear if(year >= 2010 & !missing(earnings))

* Load multiple files; extract year from filename
pq use /data/cps_*.parquet, clear asterisk_to_variable(year)

* Append a second file, compressing on load
pq append extra.parquet, compress

* SAS read preserving source order
pq use survey.sas7bdat, clear preserve_order

* CSV read with date parsing
pq use raw.csv, clear parse_dates

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
| Binary | `str#` / *dropped* | Pass `binary_to_string` to decode as string; otherwise dropped |

## Performance

Benchmarks run on AMD Ryzen 7 8845HS, 14 GB RAM, Windows 11, Stata 17 SE. See [benchmarks.md](benchmarks.md) for full tables.

| Format | Operation | pq speedup vs native |
|--------|-----------|---------------------|
| CSV | Write | **12× faster** than `export delimited` |
| CSV | Read | **2.6–3× faster** than `import delimited` |
| SPSS | Read (1M rows) | **12.5× faster** than `import spss` |
| SPSS | Read subset cols (1M rows) | **18× faster** than `import spss` |
| SAS | Read | **6.6× faster** than `import sas` |
| Parquet | Full read | Can be slower than `.dta` or `import parquet` |
| Parquet | Filtered read (`if(year > 2010)`) | Predicate pushdown skips rows before loading, not an option for `import parquet' |
| Parquet | Random sample (`random_share(0.01)`) | Reproducible sample without reading the full file |
| Parquet | Column subset on wide files | Faster than `.dta` when reading a few columns from many |
| Parquet | Write | Allows better integration with non-Stata pipelines.  Not available natively in Stata. |

## Limitations

- **Binary columns** are silently dropped unless `binary_to_string` is passed, which decodes them as strings.
- **strL reads** are slower than `str#` due to Stata plugin constraints.
- **`if()` uses SQL semantics**: missing values are not treated as greater than any value (unlike Stata's native `if`).
- **CSV date filters**: use ISO literals (`DATE '2020-01-05'`) rather than Stata's `td()`/`tc()` functions inside `if()`.
