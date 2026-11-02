*!  python_roundtrip.do
*!  Python-made parquet  ->  Stata (`pq use`)  ->  parquet (`pq save`)
*!
*!  Prereqs:
*!    1. Build/obtain pq.plugin and have pq.ado on the adopath.
*!    2. Create the input file from Python:
*!         uv run examples/python_stata_metadata.py python_made.parquet
*!    3. Set PQDIR below to the folder holding pq.ado + pq.plugin.
*!
*!  After this runs, confirm nothing was lost from Python:
*!         uv run examples/python_verify.py python_made.parquet stata_resaved.parquet

clear all
set more off
version 16.0

local PQDIR   "`c(pwd)'"                 // folder with pq.ado + pq.plugin
local INFILE  "python_made.parquet"      // written by the Python example
local OUTFILE "stata_resaved.parquet"    // Stata re-saves here

adopath ++ "`PQDIR'"
capture program drop pq

*-------------------------------------------------------------------------------
* 1.  Inspect the metadata the Python file carries, without loading data
*-------------------------------------------------------------------------------
pq metadata using "`INFILE'"

*-------------------------------------------------------------------------------
* 2.  Load it and confirm the metadata attached to the Stata variables
*-------------------------------------------------------------------------------
pq use "`INFILE'", clear

describe
di as txt _n "grp variable label : " as res `"`: variable label grp'"'
di as txt    "grp value label    : " as res `"`: value label grp'"'
label list grplbl
di as txt "grp notes:"
notes grp

di as txt _n "{hline 50}"
di as res "Loaded Python-made parquet OK"
di as txt "{hline 50}"

*-------------------------------------------------------------------------------
* 3.  Re-save from Stata -- the `statametadata' option is REQUIRED to write the
*     labels/value labels/notes/formats back into the parquet footer. Without
*     it `pq save' writes the data only and the metadata is dropped.
*-------------------------------------------------------------------------------
pq save "`OUTFILE'", replace statametadata
di as res _n "Re-saved to `OUTFILE'."
di as txt "Verify from the shell with:"
di as txt "    uv run examples/python_verify.py `INFILE' `OUTFILE'"
