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
