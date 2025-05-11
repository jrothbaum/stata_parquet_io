# PQ - Parquet File Reader/Writer for Stata

`pq` is a Stata package that enables reading and writing Parquet files directly in Stata. This plugin bridges the gap between Stata's data analysis capabilities and the increasingly popular Parquet file format, which is optimized for storage and performance with large datasets.

## Features

- Read Parquet files into Stata datasets
- Write Stata datasets to Parquet files
- Describe Parquet file structure without loading the data
- Support for variable selection and filtering
- Automatic handling of data types between Stata and Parquet
- Preserves original Parquet column names

## Installation (PENDING - NOT IMPLEMENTED)

```stata
* Option 1: Install from SSC (when available)
ssc install pq

* Option 2: Manual installation
* Download the package files and place them in your PLUS directory
* The plugin files should be placed in the 'p' folder within your PLUS directory
```

## Usage

### Reading Parquet Files

```stata
* Basic usage - read entire file
pq use using filename.parquet, clear

* Read specific variables
pq use var1 var2 var3 using filename.parquet, clear

* Read with observation filtering (STILL UNDER DEVELOPMENT AND TESTING)
pq use using filename.parquet, clear in 1/1000
pq use using filename.parquet, clear if value > 100

* Use wildcards to select variables
pq use x* using filename.parquet, clear
```

### Writing Parquet Files

```stata
* Save entire dataset
pq save using filename.parquet, replace

* Save specific variables (STILL UNDER DEVELOPMENT AND TESTING)
pq save var1 var2 var3 using filename.parquet, replace

* Save with observation filtering (STILL UNDER DEVELOPMENT AND TESTING)
pq save using filename.parquet, replace in 1/1000
pq save using filename.parquet, replace if value > 100

* Disable automatic variable renaming (by default restores original Parquet names)
pq save using filename.parquet, replace noautorename
```

### Examining Parquet Files

```stata
* Basic structure information
pq describe using filename.parquet

* Detailed information including data types
pq describe using filename.parquet, detailed

* Silent mode (store results without display)
pq describe using filename.parquet, quietly
```

## Advanced Features

### Variable Name Handling

When reading Parquet files, the original column names are preserved in variable labels. When saving back to Parquet, the package automatically restores the original Parquet column names unless the `noautorename` option is specified.

### Return Values

The `pq describe` command returns details about the Parquet file structure that can be accessed via Stata's `r()` values:

```stata
pq describe using filename.parquet, quietly
display r(n_rows)       // Number of rows
display r(n_columns)    // Number of columns
display r(name_1)       // Name of first column
display r(type_1)       // Data type of first column
```

## Requirements

- Stata 16.0 or higher
- The appropriate plugin file for your operating system (.dll for Windows, .dylib for Mac, .so for Linux)

## Technical Details

This package uses a plugin based on the *blazingly-fast* (as required for all Rust packages, but also true in at least this case) [Polars](https://github.com/pola-rs/polars) library to handle Parquet files efficiently.  Polars developed by [Ritchie Vink](https://www.ritchievink.com/) and many others.


## Benchmarks
This was run on my computer, with the following specs (and reading the data from an external SSD):<br>
CPU: AMD Ryzen 7 8845HS w/ Radeon 780M Graphics<br>
Cores: 16<br>
RAM: 14Gi<br>
OS: Linux Mint 22<br>

This is not intended to be a scientific benchmark, see the code below.

Basically, it just draws a bunch of random normally distributed float variables (and an integer index stored as a float and a string variable) of various sizes (n_rows, n_columns) and save/use them as parquet and dta files and compares the time.  For each, I report the time for the save/use and next to the parquet time, I report the parquet time/dta time.


```


. 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(1000)
Number of observations (_N) was 0, now 1,000.
(          1,000,              10)
    1: Stata:       save:        0.00
    2: Parquet:     save:        0.01             5.00
    3: Stata:       use:         0.01
    4: Parquet:     use:         0.01             0.64

.                                 
. 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(10000)
Number of observations (_N) was 0, now 10,000.
(         10,000,              10)
    1: Stata:       save:        0.00
    2: Parquet:     save:        0.01             4.00
    3: Stata:       use:         0.01
    4: Parquet:     use:         0.01             1.17

.                                 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(100000)
Number of observations (_N) was 0, now 100,000.
(        100,000,              10)
    1: Stata:       save:        0.00
    2: Parquet:     save:        0.05            12.50
    3: Stata:       use:         0.00
    4: Parquet:     use:         0.08            19.00

.                                 
.                                 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(1000000)
Number of observations (_N) was 0, now 1,000,000.
(      1,000,000,              10)
    1: Stata:       save:        0.03
    2: Parquet:     save:        0.29             9.86
    3: Stata:       use:         0.02
    4: Parquet:     use:         0.32            18.71

.                                 
.                                 
. benchmark_parquet_io_data,      n_cols(10)      ///
>                                 n_rows(10000000)
Number of observations (_N) was 0, now 10,000,000.
(     10,000,000,              10)
    1: Stata:       save:        0.19
    2: Parquet:     save:        2.31            12.33
    3: Stata:       use:         0.19
    4: Parquet:     use:         2.46            13.17

. 
. benchmark_parquet_io_data,      n_cols(5000)    ///
>                                 n_rows(10000)
Number of observations (_N) was 0, now 10,000.
(         10,000,           5,000)
    1: Stata:       save:        0.10
    2: Parquet:     save:        1.44            15.04
    3: Stata:       use:         0.05
    4: Parquet:     use:         3.62            67.07

. 



```




Benchmark code:
```
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
	
	
	
	
	tempfile path_save_root
	local path_save_root C:\Users\jonro\Downloads\test_benchmark
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
		
		timer list
		local save_stata = r(t1)
		local save_parquet = r(t2)
		local use_stata = r(t3)
		local use_parquet = r(t4)
		local save_ratio = r(t2)/r(t1)
		local use_ratio = r(t4)/r(t3)
		noisily di "(" %15.0fc `n_rows' ", " %15.0fc `n_cols' ")"
		noisily di "	1: Stata:	save:	" %9.2f `save_stata'
		noisily di "	2: Parquet:	save:	" %9.2f `save_parquet' "	" %9.2f `save_ratio'
		noisily di "	3: Stata:	use:	" %9.2f `use_stata'
		noisily di "	4: Parquet:	use:	" %9.2f `use_parquet'  "	" %9.2f `use_ratio'
	}
	
	capture erase `path_save_root'.parquet
	capture erase `path_save_root'.dta
	
end

```
