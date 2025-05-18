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
{cmd:pq use} [{varlist}] {cmd:using} {it:filename} [, {opt clear} {opt in(range)} {opt if(expression)}]

{phang}
Save Stata data as a Parquet file:

{p 8 17 2}
{cmd:pq save} [{varlist}] {cmd:using} {it:filename} [, {opt replace} {opt in(range)} {opt if(expression)} {opt noautorename}]

{phang}
Describe contents of a Parquet file:

{p 8 17 2}
{cmd:pq describe} {cmd:using} {it:filename} [, {opt quietly} {opt detailed}]

{marker description}{...}
{title:Description}

{pstd}
{cmd:pq} provides commands for working with Apache Parquet files in Stata. Parquet is a columnar storage file format 
designed to efficiently store and process large datasets. This package allows Stata users to directly read from
and write to Parquet files, facilitating data interchange with other data science tools and platforms that support
this format, such as Python (pandas, polars), R, Spark, and many others.

{marker options}{...}
{title:Options}

{dlgtab:Options for pq use}

{phang}
{opt clear} specifies that it is okay to replace the data in memory, even though the current data have not been saved to disk.

{phang}
{opt in(range)} specifies a subset of rows to read. The format is {it:offset/rows} where {it:offset} is the starting row (1-based indexing) 
and {it:rows} is the ending row. For example, {cmd:in(10/20)} would read rows 10 through 20.

{phang}
{opt if(expression)} imports only rows that satisfy the specified condition. This filter is applied directly during reading
and can significantly improve performance compared to reading all data and then filtering in Stata.

{dlgtab:Options for pq save}

{phang}
{opt replace} permits {cmd:pq save} to overwrite an existing Parquet file.

{phang}
{opt if(expression)} saves only rows that satisfy the specified condition.

{phang}
{opt noautorename} prevents automatic renaming of variables based on Parquet metadata stored in variable labels.
By default, variables that were renamed when imported will be restored to their original Parquet column names when saved.

{dlgtab:Options for pq describe}

{phang}
{opt quietly} suppresses display of column information, but still stores results in return values.

{phang}
{opt detailed} provides more detailed information about each column, including string lengths for string columns.

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
polars_parquet package. Version 1.0.0.

{pstd}
For bug reports, feature requests, or other issues, please see {it:https://github.com/jrothbaum/stata_parquet_io}.