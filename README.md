# Read/Write parquet files in stata

`pq` is a Stata package that enables reading and writing Parquet files directly in Stata. This plugin bridges the gap between Stata's data analysis capabilities and the increasingly popular Parquet file format, which is optimized for storage and performance with large datasets.

## Features

- **Read** Parquet files into Stata datasets
- **Write** Stata datasets to Parquet files  
- **Append** Parquet files to existing data in memory
- **Merge** Parquet files with existing data using standard Stata merge syntax
- **Describe** Parquet file structure without loading the data
- Support for variable selection and filtering during read operations
- Automatic handling of data types between Stata and Parquet
- Preserves original Parquet column names via variable labels
- Performance optimizations including compression and parallel processing
- Support for partitioned datasets and wildcard file patterns

## Installation
### Option 1: Install from SSC
```stata
ssc install pq
```
[SSC listing](https://ideas.repec.org/c/boc/bocode/s459458.html)

### Option 2: Manual installation
1. Download the package files for your archictecture (Linux, Mac, or Windows - or to be safe all three from the SSC zip)  from the [latest release](https://github.com/jrothbaum/stata_parquet_io/releases)
2. Place the files in your PLUS/p directory (find location with `sysdir`)

### Important Note for Mac ARM Users

You may encounter an error related to Mac Gatekeeper restrictions on unsigned binaries. To resolve this:

1. Go to **System Preferences/Settings → Privacy & Security**
2. Look for a message about the blocked dylib near the bottom
3. Click **"Allow Anyway"** next to the blocked file
4. Authenticate with your password if prompted
5. Try using the plugin again in Stata


## Quick Start

```stata
* Load a Parquet file
pq use using mydata.parquet, clear

* Save current data as Parquet
pq save using mydata.parquet, replace

* Describe a Parquet file without loading
pq describe using mydata.parquet
```

## Usage

### Reading Parquet Files

```stata
* Basic usage - read entire file
pq use using filename.parquet, clear

* Read specific variables
pq use var1 var2 var3 using filename.parquet, clear

* Read with observation filtering (using Stata syntax that will be converted to SQL)
pq use using filename.parquet, clear if(value > 100)

* Read subset of rows
pq use using filename.parquet, clear in(1/1000)

* Use wildcards to select variables
pq use x* using filename.parquet, clear

* Performance optimizations
pq use using large_file.parquet, clear compress compress_string_to_numeric sort(id)

* Random sampling a specific number of rows
pq use using large_file.parquet, clear random_n(1000)

* Random sampling a specific number of rows, with a seed for replicability
pq use using large_file.parquet, clear random_n(1000) random_seed(12345)

* Random sampling a specific share of rows, with a seed for replicability
pq use using large_file.parquet, clear random_share(0.1) random_seed(12345)
```

### Appending Data

```stata
* Append Parquet file to existing data
pq append using additional_data.parquet

* Append with filtering
pq append using new_data.parquet, if(year == 2024)
```

### Merging Data

```stata
* Standard Stata merge syntax with Parquet files
pq merge 1:1 id using lookup_table.parquet, generate(_merge)
pq merge m:1 category_id using categories.parquet, keep(match) nogenerate
```

### Writing Parquet Files

```stata
* Save entire dataset
pq save using filename.parquet, replace

* Save specific variables
pq save var1 var2 var3 using filename.parquet, replace

* Save with filtering
pq save using filename.parquet, replace if(value > 100)

* Save with compression options
pq save using compressed.parquet, replace compression(zstd) compression_level(9)

* Create partitioned dataset
pq save using /output/partitioned_data, replace partition_by(year region)

* Preserve original Parquet variable names (default behavior)
pq save using filename.parquet, replace
* OR disable automatic renaming
pq save using filename.parquet, replace noautorename
```

### Examining Parquet Files

```stata
* Basic structure information
pq describe using filename.parquet

* Detailed information including string lengths
pq describe using filename.parquet, detailed

* Quiet mode for programmatic use
pq describe using filename.parquet, quietly
return list  // View returned values
```


## Advanced Features

### Working with Multiple Files

```stata
* Load multiple files with wildcard patterns
pq use using /data/sales_*.parquet, clear asterisk_to_variable(year)

* Combine files with different schemas
pq use using /data/*.parquet, clear relaxed
```

### Performance Optimization

```stata
* Thread management - set environment variable to limit threads
* (useful on shared systems)
* Example: set POLARS_MAX_THREADS=4 before starting Stata

* Parallel processing strategies
pq use using large_file.parquet, clear parallelize(columns)  // for wide files
pq use using large_file.parquet, clear parallelize(rows)     // for tall files

* Compression and optimization
pq use using large_file.parquet, clear compress compress_string_to_numeric
```

### Variable Name Handling

Parquet files support more flexible variable names than Stata (spaces, special characters, unlimited length). When reading Parquet files:

- Original column names are preserved in variable labels as `{parquet_name:original_name}`
- Variables are renamed if they contain reserved words or exceed 32 characters
- When saving, original Parquet names are automatically restored unless `noautorename` is specified

### Partitioned Datasets

```stata
* Save as partitioned dataset (creates directory structure)
pq save using /output/partitioned_data, replace partition_by(year region)

* Control partition overwrite behavior
pq save using /output/data, replace partition_by(year) nopartitionoverwrite
```

## Data Type Support

| Parquet Type | Stata Type | Notes |
|--------------|------------|-------|
| String | str# or strL | Automatically sized; >2045 chars become strL |
| Integer | byte/int/long | Automatically sized based on range |
| Float | float/double | Preserves precision |
| Boolean | byte | 0/1 values |
| Date | long | Formatted as %td |
| DateTime | double | Formatted as %tc |
| Time | double | Formatted as %tchh:mm:ss |
| Binary | *dropped* | Not currently supported by Stata for C plugins |

## Technical Details

- Built on the [Polars](https://github.com/pola-rs/polars) library for blazing-fast performance (as all Rust libraries require you note)
- Requires Stata 16.0 or later
- Cross-platform support (Windows, Linux, macOS)
- Efficient memory usage with optional compression
- Plugin-based architecture for optimal performance

## Limitations

- **Binary data**: Not supported (columns are dropped with warning)
- **strL performance**: Reading strL columns is slower due to Stata plugin limitations  
- **SQL vs. Stata syntax**: The `if()` condition converts Stata if conditions to SQL-style comparisons where missing values are not treated as greater than any value (unlike Stata)



## Benchmarks
This was run on my computer, with the following specs:<br>
CPU: 	AMD Ryzen 7 8845HS w/ Radeon 780M Graphics<br>
Cores: 	16<br>
RAM: 	14Gi<br>
OS: 	Windows 11<br>
Run:	June 2, 2025<br>
This is not intended to be a scientific benchmark, see the code below.

Basically, it just draws a bunch of random normally distributed float variables (and an integer index stored as a float and a string variable) of various sizes (n_rows, n_columns) and save/use them as parquet and dta files and compares the time.  For each, I report the time for the save/use and next to the parquet time, I report the parquet time/dta time.


```
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(1000)
Number of observations (_N) was 0, now 1,000.
(          1,000,              10)
    1: Stata:       save:        0.00
    2: Parquet:     save:        0.00             4.00
    3: Stata:       use:         0.01
    4: Parquet:     use:         0.01             0.91

    Loading only 5 variables of 10
    5: Stata:       use:         0.00
    6: Parquet:     use:         0.01          15.00

.                                 
. 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(10000)
Number of observations (_N) was 0, now 10,000.
(         10,000,              10)
    1: Stata:       save:        0.00
    2: Parquet:     save:        0.01             9.00
    3: Stata:       use:         0.01
    4: Parquet:     use:         0.02             2.88

    Loading only 5 variables of 10
    5: Stata:       use:         0.00
    6: Parquet:     use:         0.01          10.00

. 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(100000)
Number of observations (_N) was 0, now 100,000.
(        100,000,              10)
    1: Stata:       save:        0.01
    2: Parquet:     save:        0.03             5.50
    3: Stata:       use:         0.00
    4: Parquet:     use:         0.07            17.00

    Loading only 5 variables of 10
    5: Stata:       use:         0.01
    6: Parquet:     use:         0.04           5.43

.                                 
.                                 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(1000000)
Number of observations (_N) was 0, now 1,000,000.
(      1,000,000,              10)
    1: Stata:       save:        0.03
    2: Parquet:     save:        0.26            10.24
    3: Stata:       use:         0.02
    4: Parquet:     use:         0.24            11.80

    Loading only 5 variables of 10
    5: Stata:       use:         0.04
    6: Parquet:     use:         0.13           3.47

.                                 
.                                 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(10000000)
Number of observations (_N) was 0, now 10,000,000.
(     10,000,000,              10)
    1: Stata:       save:        0.15
    2: Parquet:     save:        1.56            10.34
    3: Stata:       use:         0.11
    4: Parquet:     use:         1.83            16.79

    Loading only 5 variables of 10
    5: Stata:       use:         0.31
    6: Parquet:     use:         0.99           3.16

. 
. benchmark_parquet_io_data,      n_cols(100)     ///
>                                 n_rows(1000000)
Number of observations (_N) was 0, now 1,000,000.
(      1,000,000,             100)
    1: Stata:       save:        0.15
    2: Parquet:     save:        1.43             9.72
    3: Stata:       use:         0.10
    4: Parquet:     use:         2.47            24.95

    Loading only 5 variables of 100
    5: Stata:       use:         0.14
    6: Parquet:     use:         0.14           0.99

. 
. benchmark_parquet_io_data,      n_cols(1000)    ///
>                                 n_rows(100000)
Number of observations (_N) was 0, now 100,000.
(        100,000,           1,000)
    1: Stata:       save:        0.14
    2: Parquet:     save:        1.58            11.35
    3: Stata:       use:         0.10
    4: Parquet:     use:         1.92            18.31

    Loading only 5 variables of 1000
    5: Stata:       use:         0.08
    6: Parquet:     use:         0.06           0.71

```




Benchmark code:
```
capture program drop benchmark_parquet_io_data
program define benchmark_parquet_io_data
	version 16
	syntax		, 	n_cols(integer)			///
					n_rows(integer)
	
	clear
	set obs `n_rows'
	local cols_created = 0

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = _n
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = char(65 + floor(runiform()*5))
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
	
	local n_to_load = 5
	local subset_to_load
	forvalues i=1/`n_to_load' {
		local subset_to_load `subset_to_load' c_`i'
	}
	
	
	
	tempfile path_save_root
	quietly {
		timer clear
		di "save stata"
		timer on 1
		save "`path_save_root'.dta", replace
		timer off 1
		
		di "save parquet"
		timer on 2
		
		di `"pq save "`path_save_root'.parquet", replace"'
		pq save "`path_save_root'.parquet", replace
		timer off 2
		
		di "use stata"
		timer on 3
		use "`path_save_root'.dta", clear
		timer off 3
		
		di "use parquet"
		timer on 4
		di `"pq use "`path_save_root'.parquet", clear"'
		pq use "`path_save_root'.parquet", clear
		timer off 4
		
		
		di "use stata"
		timer on 5
		use `subset_to_load' using "`path_save_root'.dta", clear
		timer off 5
		
		di "use parquet"
		timer on 6
		di `"pq use "`path_save_root'.parquet", clear"'
		pq use `subset_to_load' using "`path_save_root'.parquet", clear
		timer off 6
		
		timer list
		local save_stata = r(t1)
		local save_parquet = r(t2)
		local use_stata = r(t3)
		local use_parquet = r(t4)
		local use_stata_subset = r(t5)
		local use_parquet_subset = r(t6)
		local save_ratio = r(t2)/r(t1)
		local use_ratio = r(t4)/r(t3)
		local use_ratio_subset = r(t6)/r(t5)
		noisily di "(" %15.0fc `n_rows' ", " %15.0fc `n_cols' ")"
		noisily di "	1: Stata:	save:	" %9.2f `save_stata'
		noisily di "	2: Parquet:	save:	" %9.2f `save_parquet' "	" %9.2f `save_ratio'
		noisily di "	3: Stata:	use:	" %9.2f `use_stata'
		noisily di "	4: Parquet:	use:	" %9.2f `use_parquet'  "	" %9.2f `use_ratio'
		
		noisily di ""
		noisily di "	Loading only `n_to_load' variables of `n_cols'"
		noisily di "	5: Stata:	use:	" %9.2f `use_stata_subset'
		noisily di "	6: Parquet:	use:	" %9.2f `use_parquet_subset'  "      " %9.2f `use_ratio_subset'
	}
	
	capture erase `path_save_root'.parquet
	capture erase `path_save_root'.dta
	
end


clear
set seed 1565225

benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(1000)
				

benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(10000)

benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(100000)
				
				
benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(1000000)
				
				
benchmark_parquet_io_data, 	n_cols(10)	///
				n_rows(10000000)

benchmark_parquet_io_data, 	n_cols(100)	///
				n_rows(1000000)

benchmark_parquet_io_data, 	n_cols(1000)	///
				n_rows(100000)
```
