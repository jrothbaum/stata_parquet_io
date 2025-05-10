{smcl}
{* *! version 1.0.0  May 2025}{...}
{vieweralsosee "[R] import" "help import"}{...}
{vieweralsosee "[R] export" "help export"}{...}
{viewerjumpto "Syntax" "polars_parquet##syntax"}{...}
{viewerjumpto "Description" "polars_parquet##description"}{...}
{viewerjumpto "Options" "polars_parquet##options"}{...}
{viewerjumpto "Examples" "polars_parquet##examples"}{...}
{viewerjumpto "Stored results" "polars_parquet##results"}{...}
{viewerjumpto "Author" "polars_parquet##author"}{...}
{title:Title}

{phang}
{bf:polars_parquet} {hline 2} Read and write Parquet files in Stata

{marker syntax}{...}
{title:Syntax}

{pstd}
Read Parquet file into Stata

{p 8 17 2}
{cmd:pq_use} [{varlist}] {cmd:using} {it:filename}{cmd:,} [{it:options}]

{pstd}
Write Stata data to Parquet file

{p 8 17 2}
{cmd:pq_save} [{varlist}] {cmd:using} {it:filename}{cmd:,} [{it:options}]

{pstd}
Describe contents of Parquet file

{p 8 17 2}
{cmd:pq_describe} {cmd:using} {it:filename}{cmd:,} [{it:options}]

{synoptset 20 tabbed}{...}
{synopthdr:pq_use options}
{synoptline}
{syntab:Main}
{synopt:{opt clear}}remove data in memory before loading parquet file{p_end}
{synopt:{opt in(first_row/last_row)}}read subset of rows from parquet file{p_end}
{synopt:{opt if(expression)}}read only rows that satisfy the specified condition{p_end}
{synoptline}

{synoptset 20 tabbed}{...}
{synopthdr:pq_save options}
{synoptline}
{syntab:Main}
{synopt:{opt replace}}overwrite existing parquet file{p_end}
{synopt:{opt in(first_row/last_row)}}write subset of rows to parquet file{p_end}
{synopt:{opt if(expression)}}write only rows that satisfy the specified condition{p_end}
{synoptline}

{synoptset 20 tabbed}{...}
{synopthdr:pq_describe options}
{synoptline}
{syntab:Main}
{synopt:{opt quietly}}suppress output{p_end}
{synopt:{opt detailed}}show detailed information about each variable{p_end}
{synoptline}

{marker description}{...}
{title:Description}

{pstd}
{cmd:polars_parquet} is a Stata package for reading and writing Parquet files. It provides high-performance 
access to data stored in Apache Parquet format, enabling efficient data exchange with other systems like Python, R, 
and big data frameworks.

{pstd}
The package includes three main commands:

{phang2}
{cmd:pq_use} - loads data from a Parquet file into Stata's memory

{phang2}
{cmd:pq_save} - writes Stata data to a Parquet file

{phang2}
{cmd:pq_describe} - provides information about the structure and contents of a Parquet file

{pstd}
This package is built on the Polars data processing library, which provides efficient handling of Parquet files.

{marker options}{...}
{title:Options}

{dlgtab:pq_use options}

{phang}
{opt clear} clears data from Stata's memory before loading the Parquet file.

{phang}
{opt in(first_row/last_row)} specifies a range of rows to read from the Parquet file. For example, {cmd:in(1/100)} 
reads the first 100 rows, while {cmd:in(101/200)} reads rows 101 through 200.

{phang}
{opt if(expression)} specifies a condition that rows must satisfy to be included. The expression uses 
SQL-like syntax for filtering.

{dlgtab:pq_save options}

{phang}
{opt replace} specifies that the Parquet file should be overwritten if it already exists.

{phang}
{opt in(first_row/last_row)} specifies a range of rows to write to the Parquet file.

{phang}
{opt if(expression)} specifies a condition that rows must satisfy to be included in the output file.

{dlgtab:pq_describe options}

{phang}
{opt quietly} suppresses the display of information about the Parquet file.

{phang}
{opt detailed} displays additional information about each variable in the Parquet file, including 
string length for string variables.

{marker examples}{...}
{title:Examples}

{pstd}Load a complete Parquet file into Stata{p_end}
{phang2}{cmd:. pq_use using "path/to/data.parquet", clear}{p_end}

{pstd}Load specific variables from a Parquet file{p_end}
{phang2}{cmd:. pq_use id name age using "path/to/data.parquet", clear}{p_end}

{pstd}Load the first 1000 rows from a Parquet file{p_end}
{phang2}{cmd:. pq_use using "path/to/data.parquet", clear in(1/1000)}{p_end}

{pstd}Load rows where age is greater than 18{p_end}
{phang2}{cmd:. pq_use using "path/to/data.parquet", clear if(age > 18)}{p_end}

{pstd}Save all variables to a Parquet file{p_end}
{phang2}{cmd:. pq_save * using "path/to/output.parquet", replace}{p_end}

{pstd}Save specific variables to a Parquet file{p_end}
{phang2}{cmd:. pq_save id name age using "path/to/output.parquet", replace}{p_end}

{pstd}Describe the contents of a Parquet file{p_end}
{phang2}{cmd:. pq_describe using "path/to/data.parquet"}{p_end}

{pstd}Get detailed description of a Parquet file{p_end}
{phang2}{cmd:. pq_describe using "path/to/data.parquet", detailed}{p_end}

{marker results}{...}
{title:Stored results}

{pstd}
{cmd:pq_describe} stores the following in {cmd:r()}:

{synoptset 20 tabbed}{...}
{p2col 5 20 24 2: Macros}{p_end}
{synopt:{cmd:r(n_rows)}}number of rows in the Parquet file{p_end}
{synopt:{cmd:r(n_columns)}}number of columns in the Parquet file{p_end}
{synopt:{cmd:r(name_#)}}name of the #th column{p_end}
{synopt:{cmd:r(type_#)}}data type of the #th column{p_end}
{synopt:{cmd:r(string_length_#)}}string length of the #th column (if string and detailed option is used){p_end}

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