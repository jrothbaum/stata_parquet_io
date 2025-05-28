{smcl}
{* *! version 1.0.0 May 2025}{...}
{title:Title}

{phang}
{bf:pq} {hline 2} Read, write, and manage Parquet files in Stata

{marker syntax}{...}
{title:Syntax}

{phang}
Import a Parquet file into Stata:

{p 8 17 2}
{cmd:pq use} [{varlist}] {cmd:using} {it:filename} [, {opt clear} {opt in(range)} {opt if(expression)} {opt relaxed} {opt asterisk_to_variable(string)} {opt parallelize(string)}]

{phang}
Save Stata data as a Parquet file:

{p 8 17 2}
{cmd:pq save} [{varlist}] {cmd:using} {it:filename} [, {opt replace} {opt in(range)} {opt if(expression)} {opt noautorename} {opt partition_by(varlist)} {opt compression(string)} {opt compression_level(integer)} {opt nopartitionoverwrite}]

{phang}
Describe contents of a Parquet file:

{p 8 17 2}
{cmd:pq describe} {cmd:using} {it:filename} [, {opt quietly} {opt detailed} {opt asterisk_to_variable(string)}]

{marker description}{...}
{title:Description}

{pstd}
{cmd:pq} provides commands for working with Apache Parquet files in Stata. Parquet is a columnar storage file format 
designed to efficiently store and process large datasets. This package allows Stata users to directly read from
and write to Parquet files, making it easier to work with other data science tools and platforms that support
this format, such as Python (pandas, polars), R, Spark, duckdb, and many others.

{marker options}{...}
{title:Options}

{dlgtab:Options for pq use}

{phang}
{opt clear} specifies that it is okay to replace the data in memory, even though the current data have not been saved to disk.

{phang}
{opt in(range)} specifies a subset of rows to read. The format is {it:first/last} where {it:first} is the starting row (1-based indexing) 
and {it:last} is the ending row. For example, {cmd:in(10/20)} would read rows 10 through 20.

{phang}
{opt if(expression)} imports only rows that satisfy the specified condition. This filter is applied directly during reading
and can significantly improve performance compared to reading all data and then filtering in Stata. Note that {cmd:>} is interpreted
as in SQL, which is different than Stata (it will not include missing values as greater than any value).

{phang}
{opt relaxed} enables vertical relaxed concatenation when reading multiple files, allowing files with different schemas 
to be combined by converting columns to their supertype (e.g., if a column is int8 in one file and int16 in another, 
it will be converted to int16 in the final result).

{phang}
{opt asterisk_to_variable(string)} when reading files with wildcard patterns (e.g., /file/*.parquet), creates a new variable 
with the specified name containing the part of the filename that matched the asterisk. For example, reading /file/2019.parquet 
and /file/2020.parquet would create a variable with values "2019" and "2020" for the respective records.

{phang}
{opt parallelize(string)} specifies the parallelization strategy. Options are {cmd:"columns"}, {cmd:"rows"}, or {cmd:""} (default).
This can improve performance when reading tall (rows) vs. wide (columns) files.  In benchmarking, it honestly doesn't seem to matter much.

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
{opt compression(string)} specifies the compression algorithm to use. Options are {cmd:"lz4"}, {cmd:"uncompressed"}, 
{cmd:"snappy"}, {cmd:"gzip"}, {cmd:"lzo"}, {cmd:"brotli"}, {cmd:"zstd"}, or {cmd:""} (default, which uses zstd).

{phang}
{opt compression_level(integer)} specifies the compression level for algorithms that support it. Valid ranges depend 
on the compression algorithm: zstd (1-22), brotli (0-11), gzip (0-9). Default is -1 (use algorithm default).

{phang}
{opt nopartitionoverwrite} prevents overwriting existing partitions when saving partitioned datasets. 
By default, existing partitions will be overwritten.  Not overwriting a partition can be useful to add an
additional file to a partition (like a new year of data) without overwriting the existing data

{dlgtab:Options for pq describe}

{phang}
{opt quietly} suppresses display of column information, but still stores results in return values.

{phang}
{opt detailed} provides more detailed information about each column, including string lengths for string columns.

{phang}
{opt asterisk_to_variable(string)} when describing files with wildcard patterns, shows information about the variable 
that would be created from the asterisk pattern.

{marker examples}{...}
{title:Examples}

{pstd}Load a Parquet file into Stata:{p_end}
{phang2}{cmd:. pq use using example.parquet, clear}{p_end}

{pstd}Load only specific variables:{p_end}
{phang2}{cmd:. pq use id name age using example.parquet, clear}{p_end}

{pstd}Load with a filter condition:{p_end}
{phang2}{cmd:. pq use using example.parquet, clear if(age > 30)}{p_end}

{pstd}Load a subset of rows:{p_end}
{phang2}{cmd:. pq use using example.parquet, clear in(101/200)}{p_end}

{pstd}Load multiple files with wildcard pattern:{p_end}
{phang2}{cmd:. pq use using /data/sales_*.parquet, clear asterisk_to_variable(year)}{p_end}

{pstd}Load with relaxed schema merging:{p_end}
{phang2}{cmd:. pq use using /data/*.parquet, clear relaxed}{p_end}

{pstd}Load with parallel processing:{p_end}
{phang2}{cmd:. pq use using large_file.parquet, clear parallelize(columns)}{p_end}

{pstd}Describe contents of a Parquet file:{p_end}
{phang2}{cmd:. pq describe using example.parquet}{p_end}

{pstd}Describe with detailed information:{p_end}
{phang2}{cmd:. pq describe using example.parquet, detailed}{p_end}

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
for the package to function. You can override the plugin location by setting the global macro
{cmd:parquet_dll_override} to the path of the plugin.

{pstd}
The package works with Stata 16.0 and later versions.

{pstd}
String variables longer than 2045 characters are automatically converted to strL format during import.

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
polars_parquet package. Version 1.1.0.

{pstd}
For bug reports, feature requests, or other issues, please see {it:https://github.com/jrothbaum/stata_parquet_io}.