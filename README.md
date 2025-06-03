# Read/Write parquet files in stata

`pq` is a Stata package that enables reading and writing Parquet files directly in Stata. This plugin bridges the gap between Stata's data analysis capabilities and the increasingly popular Parquet file format, which is optimized for storage and performance with large datasets.

## Features

- Read Parquet files into Stata datasets
- Write Stata datasets to Parquet files
- Describe Parquet file structure without loading the data
- Support for variable selection and filtering
- Automatic handling of data types between Stata and Parquet
- Preserves original Parquet column names

## Installation
- Current status - github actions create the proper files for Windows, Linux, and Mac.  I have successfully tested that the Linux and Windows github build files work.  I will soon submit them for download via SSC.

```stata
* Option 1: Install from SSC
ssc install pq
(https://ideas.repec.org/c/boc/bocode/s459458.html)

* Option 2: Manual installation
* Download the package files from the latest build at releases (https://github.com/jrothbaum/stata_parquet_io/releases) and place them in your PLUS directory
* The plugin files should be placed in the 'p' folder within your PLUS directory
```

## Usage

### Reading Parquet Files

```stata
* Basic usage - read entire file
pq use using filename.parquet, clear

* Read specific variables
pq use var1 var2 var3 using filename.parquet, clear

* Read with observation filtering
pq use using filename.parquet, clear in(1/1000)
pq use using filename.parquet, clear if(value > 100)

* Use wildcards to select variables
pq use x* using filename.parquet, clear
```

### Writing Parquet Files

```stata
* Save entire dataset
pq save using filename.parquet, replace

* Save specific variables
pq save var1 var2 var3 using filename.parquet, replace

* Save a subset of observations
pq save using filename.parquet, replace if(value > 100)

* Disable automatic variable renaming.  Renaming by default restores original Parquet names from labels if a rename was needed (reserved words like int or variables longer than 32 characters, etc.)
pq save using filename.parquet, replace noautorename
```

### Examining Parquet Files

```stata
* Basic structure information (detailed and quietly are really for getting slightly more information before calling pq use, but you can use it)
pq describe using filename.parquet, [detailed quietly]
```

## Advanced Features

### Thread management

By default, stata_parquet_io will use all the threads available on the computer.  If that is not desirable, set the environment variable POLARS_MAX_THREADS to the number of threads you want to use (for example on a shared system).  This will limit the number of threads used in the parquet read/write operations (from polars in Rust) and in serializing the data to Stata.

### Variable Name Handling

Parquet files can have much more flexible variable names than Stata, including spaces, dashes, pretty much anything.  They also isn't really a limit to the length of a variable name for a parquet file.  Stata variable names are limited to 32 alphanumeric characters. 
 When reading Parquet files, the original column names are preserved in variable labels. When saving back to Parquet, the package automatically restores the original Parquet column names unless the `noautorename` option is specified.

## Technical Details

This package uses a plugin based on the *blazingly-fast* (as required for all Rust packages, but also true in at least this case) [Polars](https://github.com/pola-rs/polars) library to handle Parquet files efficiently.  Polars is being developed by [Ritchie Vink](https://www.ritchievink.com/) and many others.

## Limitations
Binary data is not supported, and I'm not sure I will implement parquet<->stata support for Binary<->strL binary.  Reads of strL string columns will be slow as there is no support for setting strL values in the C plugin and I needed to use I/O to implement a hacky workaround.

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
