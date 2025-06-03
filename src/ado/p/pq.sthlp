{smcl}
{* *! version 1.2.0 June 2025}{...}
{title:Title}

{phang}
{bf:pq} {hline 2} Read, write, and manage Parquet files in Stata

{marker syntax}{...}
{title:Syntax}

{phang}
Import a Parquet file into Stata:

{p 8 17 2}
{cmd:pq use} [{varlist}] {cmd:using} {it:filename} [, {opt clear} {opt append} {opt in(range)} {opt if(expression)} {opt relaxed} {opt asterisk_to_variable(string)} {opt parallelize(string)} {opt sort(varlist)} 
{opt compress} {opt compress_string_to_numeric}]

{phang}
Append a Parquet file to existing data:

{p 8 17 2}
{cmd:pq append} [{varlist}] {cmd:using} {it:filename} [, {opt in(range)} {opt if(expression)} {opt relaxed} {opt asterisk_to_variable(string)} {opt parallelize(string)} {opt sort(varlist)} {opt compress} 
{opt compress_string_to_numeric}]

{phang}
Merge a Parquet file with existing data:

{p 8 17 2}
{cmd:pq merge} {it:merge_type} [{varlist}] {cmd:using} {it:filename} [, {merge_options} {opt in(range)} {opt if(expression)} {opt relaxed} {opt asterisk_to_variable(string)} {opt parallelize(string)} {opt sort(varlist)} {opt compress} 
{opt compress_string_to_numeric}]

{phang}
Save Stata data as a Parquet file:

{p 8 17 2}
{cmd:pq save} [{varlist}] {cmd:using} {it:filename} [, {opt replace} {opt if(expression)} {opt noautorename} {opt partition_by(varlist)} {opt compression(string)} {opt compression_level(integer)} {opt nopartitionoverwrite} {opt compress} 
{opt compress_string_to_numeric}]

{phang}
Describe contents of a Parquet file:

{p 8 17 2}
{cmd:pq describe} {cmd:using} {it:filename} [, {opt quietly} {opt detailed} 
{opt asterisk_to_variable(string)}]

{marker description}{...}
{title:Description}

{pstd}
{cmd:pq} provides commands for working with Apache Parquet files in Stata. Parquet is a columnar storage file format 
designed to efficiently store and process large datasets. This package allows Stata users to directly read from
and write to Parquet files, making it easier to work with other data science tools and platforms that support
this format, such as Python (pandas, polars), R, Spark, duckdb, and many others.

{pstd}
The package supports five main operations: {cmd:use} (load data), {cmd:append} (add to existing data), 
{cmd:merge} (join with existing data), {cmd:save} (write data), and {cmd:describe} (examine file structure).

{pstd}
{bf:IMPORTANT NOTE FOR MAC ARM USERS}:

{pstd}
You may get an error message that is related to Mac Gatekeeper and restrictions on unsigned binaries. Unfortunately, preventing this 
requires a developer account that costs $99/year and I am not getting  the subscription.  If you want to use the plugin (pq.dylib),
you can do the following:

{p 4 8 2}1. Go to System Preferences/Settings → Privacy & Security

{p 4 8 2}2. Look for a message about the blocked dylib near the bottom

{p 4 8 2}3. Click "Allow Anyway" next to the blocked file

{p 4 8 2}4. You may need to authenticate with your password

{p 4 8 2}5. Try using the plugin again in Stata

{marker options}{...}
{title:Options}

{dlgtab:Options for pq use and pq append}

{phang}
{opt clear} specifies that it is okay to replace the data in memory, even though the current data have not been saved to disk. Only available with {cmd:pq use}.

{phang}
{opt in(range)} specifies a subset of rows to read. The format is {it:first/last} where {it:first} is the starting row (1-based indexing) 
and {it:last} is the ending row. For example, {cmd:in(10/20)} would read rows 10 through 20.

{phang}
{opt if(expression)} imports only rows that satisfy the specified condition. This filter is applied directly during reading
and can significantly improve performance compared to reading all data and then filtering in Stata. 
This will convert the standard Stata syntax to SQL for parquet filtering.  Generally, 
it will work exactly as Stata does.  However, note that {cmd:>} is interpreted
as in SQL, which is different than Stata (it will not include missing values as greater than any value).

{phang}
{opt relaxed} enables vertical relaxed concatenation when reading multiple files, allowing files with different schemas 
to be combined by converting columns to their supertype (e.g., if a column is int8 in one file and int16 in another, 
it will be converted to int16 in the final result).  This is relevant for reading data from hive
partitions or glob files (e.g. /path/*.parquet).

{phang}
{opt asterisk_to_variable(string)} when reading files with wildcard patterns (e.g., /file/*.parquet), creates a new variable 
with the specified name containing the part of the filename that matched the asterisk. For example, reading /file/2019.parquet 
and /file/2020.parquet would create a variable with values "2019" and "2020" for the respective records.

{phang}
{opt parallelize(string)} specifies the parallelization strategy. Options are {cmd:"columns"}, {cmd:"rows"}, or {cmd:""} (default).
This can improve performance when reading tall (many rows) vs. wide (many columns) files.

{phang}
{opt sort(varlist)} sorts the data by the specified variables during the read operation, which can be more efficient than 
sorting after loading all data into memory.  Again, per SQL and not Stata standards, nulls are 
not treated as greater than values.

{phang}
{opt compress} enables compression of data during the read operation to reduce memory usage.
This is the equivalent of Stata's compress, but should be much faster. 

{phang}
{opt compress_string_to_numeric} automatically converts string variables to numeric when possible during the read operation
(e.g. when all the string values are numeric).  This is equivalent to {cmd:"destring, replace"} but should be much faster.

{dlgtab:Options for pq merge}

{phang}
{it:merge_type} specifies the type of merge operation. Standard Stata merge types are supported: {cmd:1:1}, {cmd:1:m}, {cmd:m:1}, and {cmd:m:m}.

{phang}
{it:merge_options} are the standard options available with Stata's {cmd:merge} command, including:

{phang2}
{opt assert(results)} specifies the required match results.

{phang2}
{opt generate(newvar)} specifies the name of the variable that will mark the merge results.

{phang2}
{opt nogenerate} specifies that the merge-result variable not be created.

{phang2}
{opt force} allows the merge to proceed even when the key variables have different storage types.

{phang2}
{opt keep(results)} specifies which observations to keep after merging.

{phang2}
{opt keepusing(varlist)} specifies which variables from the using dataset to keep.

{phang2}
{opt nolabels} specifies that value labels not be copied from the using dataset.

{phang2}
{opt nonotes} specifies that notes not be copied from the using dataset.

{phang2}
{opt replace} specifies that matching variables in the master dataset be replaced with values from the using dataset.

{phang2}
{opt noreport} specifies that the merge table not be displayed.

{phang2}
{opt sorted} specifies that the datasets are already sorted by the key variables.

{phang2}
{opt update} specifies that missing values in the master dataset be replaced with values from the using dataset.

{phang}
All read options ({opt in()}, {opt if()}, {opt relaxed}, {opt asterisk_to_variable()}, {opt parallelize()}, {opt sort()}, {opt compress}, {opt compress_string_to_numeric}) are also available with {cmd:pq merge}.
This will load the data using {cmd:pq use} in a temporary frame, {cmd:save} it to a temporary dta file, and then run the specified {cmd:merge}.

{dlgtab:Options for pq save}

{phang}
{opt replace} permits {cmd:pq save} to overwrite an existing Parquet file.

{phang}
{opt if(expression)} saves only rows that satisfy the specified condition. Note that {cmd:>} is interpreted
as in SQL, which is different than Stata (it will not include missing values as greater than any value).

{phang}
{opt noautorename} prevents automatic renaming of variables based on Parquet metadata stored in variable labels.
By default, variables that were renamed when imported will be restored to their original Parquet column names when saved.

{phang}
{opt partition_by(varlist)} creates a partitioned Parquet dataset, splitting the data into separate files based on 
the unique values of the specified variables. This can improve query performance for large datasets.

{phang}
{opt compression(string)} specifies the compression algorithm to use in the saved parquet file.
Options are {cmd:"lz4"}, {cmd:"uncompressed"}, 
{cmd:"snappy"}, {cmd:"gzip"}, {cmd:"lzo"}, {cmd:"brotli"}, {cmd:"zstd"}, or {cmd:""} (default, which uses zstd).

{phang}
{opt compression_level(integer)} specifies the compression level for algorithms that support it. Valid ranges depend 
on the compression algorithm: zstd (1-22), brotli (0-11), gzip (0-9). Default is -1 (use algorithm default).

{phang}
{opt nopartitionoverwrite} prevents overwriting existing partitions when saving partitioned datasets. 
By default, existing partitions will be overwritten. Not overwriting a partition can be useful to add an
additional file to a partition (like a new year of data) without overwriting the existing data.

{phang}
{opt compress} enables compression of data during the write operation.

{phang}
{opt compress_string_to_numeric} automatically converts string variables to numeric when possible during the write operation.

{dlgtab:Options for pq describe}

{phang}
{opt quietly} suppresses display of column information, but stores results in return values
for programmatic use.

{phang}
{opt detailed} provides more detailed information about each column, including string lengths for string columns.

{phang}
{opt asterisk_to_variable(string)} when describing files with wildcard patterns, shows information about the variable 
that would be created from the asterisk pattern.

{marker examples}{...}
{title:Examples}

{dlgtab:Loading data}

{pstd}Load a Parquet file into Stata:{p_end}
{phang2}{cmd:. pq use using example.parquet, clear}{p_end}

{pstd}Load only specific variables:{p_end}
{phang2}{cmd:. pq use id name age using example.parquet, clear}{p_end}

{pstd}Load with a filter condition:{p_end}
{phang2}{cmd:. pq use using example.parquet, clear if(age > 30)}{p_end}

{pstd}Load a subset of rows:{p_end}
{phang2}{cmd:. pq use using example.parquet, clear in(101/200)}{p_end}

{pstd}Load with compression and optimization:{p_end}
{phang2}{cmd:. pq use using large_file.parquet, clear compress compress_string_to_numeric}{p_end}

{pstd}Load and sort data during read:{p_end}
{phang2}{cmd:. pq use using unsorted.parquet, clear sort(id date)}{p_end}

{dlgtab:Appending data}

{pstd}Append a Parquet file to existing data:{p_end}
{phang2}{cmd:. pq append using additional_data.parquet}{p_end}

{pstd}Append with filtering:{p_end}
{phang2}{cmd:. pq append using new_data.parquet, if(year == 2024)}{p_end}

{dlgtab:Merging data}

{pstd}Perform a 1:1 merge with a Parquet file:{p_end}
{phang2}{cmd:. pq merge 1:1 id using lookup_table.parquet, generate(_merge)}{p_end}

{pstd}Perform a many-to-one merge keeping only matches:{p_end}
{phang2}{cmd:. pq merge m:1 category_id using categories.parquet, keep(match) nogenerate}{p_end}

{pstd}Merge with specific variables and filtering:{p_end}
{phang2}{cmd:. pq merge 1:m customer_id using transactions.parquet, keepusing(amount date) if(amount > 100)}{p_end}

{dlgtab:Working with multiple files}

{pstd}Load multiple files with wildcard pattern:{p_end}
{phang2}{cmd:. pq use using /data/sales_*.parquet, clear asterisk_to_variable(year)}{p_end}

{pstd}Load with relaxed schema merging:{p_end}
{phang2}{cmd:. pq use using /data/*.parquet, clear relaxed}{p_end}

{dlgtab:Performance optimization}

{pstd}Load with parallel processing:{p_end}
{phang2}{cmd:. pq use using large_file.parquet, clear parallelize(columns)}{p_end}

{dlgtab:Describing files}

{pstd}Describe contents of a Parquet file:{p_end}
{phang2}{cmd:. pq describe using example.parquet}{p_end}

{pstd}Describe with detailed information:{p_end}
{phang2}{cmd:. pq describe using example.parquet, detailed}{p_end}

{dlgtab:Saving data}

{pstd}Save data as a Parquet file:{p_end}
{phang2}{cmd:. pq save using newfile.parquet, replace}{p_end}

{pstd}Save only specific variables:{p_end}
{phang2}{cmd:. pq save id name income using newfile.parquet, replace}{p_end}

{pstd}Save with a filter condition:{p_end}
{phang2}{cmd:. pq save using filtered.parquet, replace if(age >= 18)}{p_end}

{pstd}Save with compression:{p_end}
{phang2}{cmd:. pq save using compressed.parquet, replace compression(zstd) compression_level(9)}{p_end}

{pstd}Save as partitioned dataset:{p_end}
{phang2}{cmd:. pq save using /output/partitioned_data, replace partition_by(year region)}{p_end}

{pstd}Save with optimization options:{p_end}
{phang2}{cmd:. pq save using optimized.parquet, replace compress compress_string_to_numeric}{p_end}

{marker remarks}{...}
{title:Remarks}

{pstd}
This package uses Polars (a fast DataFrame library written in Rust) through a Stata plugin interface to provide
efficient reading and writing of Parquet files. The implementation supports various data types including
string, numeric, datetime, date, time, and strL variables.

{pstd}
When you import a Parquet file with {cmd:pq use}, the original column names from the Parquet file
are stored as variable labels with the format {cmd:{{}parquet_name:original_name{}}}.
When you later save the data with {cmd:pq save}, these columns will be automatically renamed back
to their original Parquet names unless you specify the {opt noautorename} option.

{pstd}
Binary columns in Parquet files are not currently supported and will be automatically dropped when importing.

{pstd}
The {opt if()} condition syntax uses SQL-style comparisons, which differ from Stata in that missing values 
are not considered greater than any value when using the {cmd:>} operator.

{pstd}
Partitioned datasets created with {opt partition_by()} organize data into separate files based on the unique 
combinations of the partitioning variables, which can significantly improve query performance for large datasets.

{pstd}
The {cmd:pq merge} command loads the Parquet file into a temporary frame, converts it to a temporary Stata dataset,
and then performs a standard Stata merge operation with all the usual merge options and functionality.

{pstd}
The compression options ({opt compress} and {opt compress_string_to_numeric}) can significantly improve performance
and reduce memory usage, especially when working with large datasets or datasets with many string variables that
could be converted to numeric.

{pstd}
String variables longer than 2045 characters are automatically converted to strL format during import.

{marker returned}{...}
{title:Returned values}

{pstd}
{cmd:pq describe} returns the following in {cmd:r()}:

{synoptset 20 tabbed}{...}
{p2col 5 20 24 2: Scalars}{p_end}
{synopt:{cmd:r(n_rows)}}Number of rows in the Parquet file{p_end}
{synopt:{cmd:r(n_columns)}}Number of columns in the Parquet file{p_end}

{synoptset 20 tabbed}{...}
{p2col 5 20 24 2: Macros}{p_end}
{synopt:{cmd:r(name_#)}}Name of column # (where # goes from 1 to the number of columns){p_end}
{synopt:{cmd:r(type_#)}}Data type of column #{p_end}
{synopt:{cmd:r(rename_#)}}Rename information for column # (if available){p_end}
{synopt:{cmd:r(string_length_#)}}String length for string columns (if detailed option specified){p_end}

{marker technical}{...}
{title:Technical notes}

{pstd}
The package requires a companion plugin that must be installed in Stata's PLUS directory.
The plugin files (pq.dll for Windows, pq.so for Linux, pq.dylib for macOS) must be properly installed
for the package to function. The plugin location can be overridden by setting the global macro
{cmd:parquet_dll_override} to the desired path.

{pstd}
The package works with Stata 16.0 and later versions.

{pstd}
String variables longer than 2045 characters are automatically converted to strL format during import.
For strL variables, the package uses a special processing method that reads the string data in batches
for optimal performance.

{pstd}
The package automatically handles data type conversion and recasting when appending or merging data
with different but compatible types (e.g., byte to int, int to long, float to double).

{marker acknowledgments}{...}
{title:Acknowledgments}

{pstd}
This package uses the Polars library for Parquet file handling, which is built using Rust and provides
excellent performance for large datasets.

{marker author}{...}
{title:Author}

{pstd}
{it:Jon Rothbaum}

{pstd}
{it:U.S. Census Bureau}

{pstd}
polars_parquet package. Version 1.2.0.

{pstd}
For bug reports, feature requests, or other issues, please see {it:https://github.com/jrothbaum/stata_parquet_io}.